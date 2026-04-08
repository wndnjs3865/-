"""Integration tests for JWQuantSystem — full system boot + pipeline."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole, Direction, MarketRegime, Priority, Timeframe
from core.models import Message, TradeSignal, TradeDecision
from config import Config
from main import JWQuantSystem


@pytest.fixture
async def system():
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Config.from_env()
        config.system.audit_db_path = os.path.join(tmpdir, "test.db")
        sys = JWQuantSystem(config)
        await sys.start()
        yield sys
        await sys.stop()


def _make_signal(**overrides) -> TradeSignal:
    defaults = dict(
        id="sig_sys_test",
        symbol="BTCUSDT",
        direction=Direction.LONG,
        score=85.0,
        entry_price=50000.0,
        stop_loss=49000.0,
        take_profit_1=53000.0,
        take_profit_2=55000.0,
        timeframe=Timeframe.H4,
        regime=MarketRegime.TRENDING_BULL,
        risk_reward_ratio=3.0,
        confluence_factors=["MTF_ALIGNMENT", "BOS_H4"],
    )
    defaults.update(overrides)
    return TradeSignal(**defaults)


@pytest.mark.asyncio
async def test_system_boots_all_agents(system):
    """All 7 agents should be running after boot."""
    assert system._running is True
    assert system.mia.is_running
    assert system.qr.is_running
    assert system.cso.is_running
    assert system.crco.is_running
    assert system.es.is_running
    assert system.po.is_running
    assert system.ima.is_running


@pytest.mark.asyncio
async def test_system_snapshot(system):
    """system_snapshot should contain all agent states."""
    snap = system.system_snapshot
    assert snap["running"] is True
    assert snap["mode"] == "PAPER"
    assert "mia" in snap["agents"]
    assert "crco" in snap["agents"]
    assert "es" in snap["agents"]
    assert "bus_metrics" in snap


@pytest.mark.asyncio
async def test_ima_monitors_all_agents(system):
    """IMA should have all 7 agents registered."""
    assert len(system.ima._monitored_agents) == 7


@pytest.mark.asyncio
async def test_full_pipeline_end_to_end(system):
    """Signal → QR → CRCO → ES: full pipeline through real agents."""
    decisions = []

    async def capture(msg: Message):
        decisions.append(msg)

    system.bus.subscribe("cso.trade_decision", AgentRole.ES, capture)

    # Inject signal as if MIA sent it
    signal = _make_signal()
    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload=signal.model_dump(mode="json"),
    ))

    await asyncio.sleep(1.5)

    assert len(decisions) >= 1
    dec = TradeDecision(**decisions[0].payload)
    assert dec.approved is True
    assert dec.symbol == "BTCUSDT"
    assert dec.quant_score > 0

    # ES should have the open trade
    assert len(system.es.open_trades) >= 1

    # PO should have tracked the fill
    assert len(system.po._open_trades) >= 1

    # CRCO should have recorded the position
    assert len(system.crco._open_positions) >= 1

    # Audit should have multiple entries
    count = await system.audit.count()
    assert count > 10  # boot + signal + score + risk + decision + fill


@pytest.mark.asyncio
async def test_circuit_breaker_propagates(system):
    """Circuit breaker should propagate to all agents."""
    system.crco._daily_pnl = -600.0
    system.crco._daily_start_value = 10000.0
    system.crco._portfolio_value = 9400.0

    # Trigger via order close
    await system.bus.publish(Message(
        topic="es.order_closed",
        sender=AgentRole.ES,
        payload={
            "symbol": "BTCUSDT", "pnl": -100.0, "pnl_percent": -1.0,
            "signal_id": "test", "direction": "LONG",
            "entry_price": 50000, "exit_price": 49000,
            "stop_loss": 49000, "take_profit_1": 53000,
            "quantity": 0.01, "duration_seconds": 60,
        },
    ))
    await asyncio.sleep(0.8)

    assert system.crco._circuit_breaker_active is True
    assert system.es._circuit_breaker_active is True
    assert system.cso._circuit_breaker_active is True


@pytest.mark.asyncio
async def test_system_graceful_stop(system):
    """System should stop all agents cleanly."""
    await system.stop()
    assert system._running is False
    for agent in system._agents:
        assert not agent.is_running
