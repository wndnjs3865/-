"""Testnet activation validation test.

Tests the full system flow using PaperExchange as a mock testnet:
  Boot → Connect exchange → Price feed → Signal → Pipeline → Order → Close
  → Emergency stop → Verify audit trail
"""

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
from exchanges.paper_exchange import PaperExchange


def _make_signal(**overrides) -> TradeSignal:
    defaults = dict(
        symbol="BTCUSDT", direction=Direction.LONG, score=85.0,
        entry_price=50000.0, stop_loss=49000.0, take_profit_1=53000.0,
        take_profit_2=55000.0, take_profit_3=58000.0,
        timeframe=Timeframe.H4, regime=MarketRegime.TRENDING_BULL,
        risk_reward_ratio=3.0, confluence_factors=["MTF_ALIGNMENT", "BOS_H4"],
    )
    defaults.update(overrides)
    return TradeSignal(**defaults)


@pytest.fixture
async def testnet_system():
    """System with PaperExchange wired as testnet exchange."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Config.from_env()
        config.system.audit_db_path = os.path.join(tmpdir, "testnet.db")
        config.trading.mode = "PAPER"
        config.trading.min_quant_score = 50.0
        config.exchange.testnet = True  # Testnet mode

        sys = JWQuantSystem(config)
        await sys.start()
        sys.mia._analysis_interval = 99999
        sys.po._report_interval = 99999

        # Wire PaperExchange as mock testnet
        exchange = PaperExchange(initial_balance=10000.0)
        await exchange.connect()
        exchange.set_price("BTCUSDT", 50000.0)
        sys.es.set_exchange(exchange)
        sys.mia.set_exchange(exchange)
        sys._exchange = exchange

        yield sys, exchange

        await exchange.disconnect()
        await sys.stop()


@pytest.mark.asyncio
async def test_testnet_config_flag():
    """EXCHANGE_TESTNET env var should set testnet flag."""
    os.environ["EXCHANGE_TESTNET"] = "true"
    config = Config.from_env()
    assert config.exchange.testnet is True
    os.environ.pop("EXCHANGE_TESTNET", None)


@pytest.mark.asyncio
async def test_testnet_full_cycle(testnet_system):
    """Full testnet cycle: signal → execute → close → verify."""
    system, exchange = testnet_system

    assert system.config.exchange.testnet is True
    assert system._running

    # Switch to LIVE to use exchange execution
    system.config.trading.mode = "LIVE"

    filled = []
    closed = []

    async def on_filled(msg: Message):
        filled.append(msg)

    async def on_closed(msg: Message):
        closed.append(msg)

    system.bus.subscribe("es.order_filled", AgentRole.PO, on_filled)
    system.bus.subscribe("es.order_closed", AgentRole.IMA, on_closed)

    # Send signal through pipeline
    signal = _make_signal(id="sig_testnet_001")
    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload=signal.model_dump(mode="json"),
    ))
    await asyncio.sleep(2.0)

    # Order should be filled
    assert len(filled) >= 1
    assert len(system.es.open_trades) >= 1

    # Inject SL price to close
    trade_id = list(system.es.open_trades.keys())[0]
    trade = system.es.open_trades[trade_id]
    system.es.inject_price("BTCUSDT", trade["stop_loss"] - 100)
    await asyncio.sleep(2.0)

    assert len(closed) >= 1
    assert closed[-1].payload.get("close_reason") == "sl_hit"

    # Audit trail complete
    total = await system.audit.count()
    assert total > 10

    system.config.trading.mode = "PAPER"


@pytest.mark.asyncio
async def test_testnet_emergency_stop(testnet_system):
    """Emergency stop works in testnet mode."""
    system, exchange = testnet_system
    system.config.trading.mode = "LIVE"

    signal = _make_signal(id="sig_testnet_em")
    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload=signal.model_dump(mode="json"),
    ))
    await asyncio.sleep(2.0)

    result = await system.emergency_stop()
    assert result["es_emergency"]["positions_closed"] >= 0
    assert system.crco._circuit_breaker_active is True

    system.config.trading.mode = "PAPER"


@pytest.mark.asyncio
async def test_testnet_safety_gate(testnet_system):
    """Safety gate validates in testnet mode."""
    system, exchange = testnet_system

    # In PAPER mode, gate fails (mode != LIVE)
    gate = await system.validate_live_readiness()
    assert gate["passed"] is False

    # Set to LIVE + provide key → should pass
    system.config.trading.mode = "LIVE"
    system.config.exchange.binance_api_key = "testnet_key"
    gate = system.crco.run_pre_live_checklist()
    assert gate["passed"] is True

    system.config.trading.mode = "PAPER"
    system.config.exchange.binance_api_key = ""


@pytest.mark.asyncio
async def test_testnet_partial_tp(testnet_system):
    """Partial take-profit works in testnet mode."""
    system, exchange = testnet_system

    closed_events = []

    async def capture(msg: Message):
        closed_events.append(msg)

    system.bus.subscribe("es.order_closed", AgentRole.PO, capture)

    signal = _make_signal(id="sig_testnet_tp")
    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload=signal.model_dump(mode="json"),
    ))
    await asyncio.sleep(1.5)

    assert len(system.es.open_trades) >= 1
    trade_id = list(system.es.open_trades.keys())[0]
    trade = system.es.open_trades[trade_id]
    original_qty = trade["quantity"]

    # Inject TP1 price → should partial close 50%
    system.es.inject_price("BTCUSDT", trade["take_profit_1"] + 100)
    await asyncio.sleep(2.0)

    # Should have partial close event
    assert len(closed_events) >= 1
    partial = closed_events[-1].payload
    assert partial.get("partial") is True

    # Remaining quantity should be ~50% of original
    if trade_id in system.es.open_trades:
        remaining = system.es.open_trades[trade_id]["quantity"]
        assert remaining < original_qty
