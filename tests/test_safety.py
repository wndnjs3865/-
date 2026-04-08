"""Tests for Live Trading Safety Mechanisms.

Covers:
- 3-stage safety gate (CRCO)
- Emergency stop (ES + JWQuantSystem)
- Mode switch (PAPER ↔ LIVE)
"""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole, Direction, Priority, Timeframe, MarketRegime
from core.models import Message, TradeSignal, TradeDecision
from config import Config
from main import JWQuantSystem


@pytest.fixture
async def system():
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Config.from_env()
        config.system.audit_db_path = os.path.join(tmpdir, "safety.db")
        config.trading.mode = "PAPER"
        sys = JWQuantSystem(config)
        await sys.start()
        sys.mia._analysis_interval = 99999
        sys.po._report_interval = 99999
        yield sys
        await sys.stop()


def _make_signal(**overrides) -> TradeSignal:
    defaults = dict(
        id="sig_safety",
        symbol="BTCUSDT",
        direction=Direction.LONG,
        score=85.0,
        entry_price=50000.0,
        stop_loss=49000.0,
        take_profit_1=53000.0,
        timeframe=Timeframe.H4,
        regime=MarketRegime.TRENDING_BULL,
        risk_reward_ratio=3.0,
        confluence_factors=["MTF_ALIGNMENT"],
    )
    defaults.update(overrides)
    return TradeSignal(**defaults)


# ══════════════════════════════════════════════
# 1. CRCO 3-Stage Safety Gate
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_safety_gate_fails_in_paper_mode(system):
    """Gate 1 should fail when mode is PAPER (not LIVE)."""
    result = system.crco.run_pre_live_checklist()
    assert result["passed"] is False
    assert any("LIVE" in b for b in result["blockers"])


@pytest.mark.asyncio
async def test_safety_gate_fails_without_api_keys(system):
    """Gate 1 should fail when no exchange API keys configured."""
    system.config.trading.mode = "LIVE"
    result = system.crco.run_pre_live_checklist()
    assert result["passed"] is False
    assert any("API key" in b for b in result["blockers"])
    system.config.trading.mode = "PAPER"  # Reset


@pytest.mark.asyncio
async def test_safety_gate_fails_with_circuit_breaker(system):
    """Gate 2 should fail when circuit breaker is active."""
    system.config.trading.mode = "LIVE"
    system.config.exchange.binance_api_key = "test_key"
    system.crco._circuit_breaker_active = True

    result = system.crco.run_pre_live_checklist()
    assert result["passed"] is False
    assert any("Circuit breaker" in b for b in result["blockers"])

    system.crco._circuit_breaker_active = False
    system.config.exchange.binance_api_key = ""
    system.config.trading.mode = "PAPER"


@pytest.mark.asyncio
async def test_safety_gate_fails_with_open_positions(system):
    """Gate 2 should fail when there are open positions."""
    system.config.trading.mode = "LIVE"
    system.config.exchange.binance_api_key = "test_key"
    system.crco._open_positions["BTCUSDT"] = {"size_usd": 1000}

    result = system.crco.run_pre_live_checklist()
    assert result["passed"] is False
    assert any("position" in b.lower() for b in result["blockers"])

    system.crco._open_positions.clear()
    system.config.exchange.binance_api_key = ""
    system.config.trading.mode = "PAPER"


@pytest.mark.asyncio
async def test_safety_gate_fails_with_extreme_risk(system):
    """Gate 3 should fail with dangerous risk parameters."""
    system.config.trading.mode = "LIVE"
    system.config.exchange.binance_api_key = "test_key"
    system.config.trading.max_risk_per_trade = 0.10  # 10% — way too high

    result = system.crco.run_pre_live_checklist()
    assert result["passed"] is False
    assert any("risk" in b.lower() for b in result["blockers"])

    system.config.trading.max_risk_per_trade = 0.01
    system.config.exchange.binance_api_key = ""
    system.config.trading.mode = "PAPER"


@pytest.mark.asyncio
async def test_safety_gate_all_3_gates_present(system):
    """All 3 gates must always be present in result."""
    result = system.crco.run_pre_live_checklist()
    assert len(result["gates"]) == 3
    gate_names = [g["name"] for g in result["gates"]]
    assert "Configuration" in gate_names
    assert "System State" in gate_names
    assert "Risk Parameters" in gate_names


@pytest.mark.asyncio
async def test_safety_gate_passes_with_valid_config(system):
    """Gate should pass when all conditions are met."""
    system.config.trading.mode = "LIVE"
    system.config.exchange.binance_api_key = "real_key_here"
    system.config.trading.max_risk_per_trade = 0.01
    system.config.trading.max_leverage = 10.0
    system.config.trading.max_daily_loss = 0.03
    system.config.trading.circuit_breaker_loss = 0.05
    system.config.trading.max_open_positions = 5
    system.config.trading.min_risk_reward = 2.0

    result = system.crco.run_pre_live_checklist()
    assert result["passed"] is True
    assert len(result["blockers"]) == 0

    system.config.exchange.binance_api_key = ""
    system.config.trading.mode = "PAPER"


# ══════════════════════════════════════════════
# 2. Emergency Stop
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_emergency_stop_closes_all_positions(system):
    """Emergency stop should close all open positions."""
    # Open 2 trades
    for i, symbol in enumerate(["BTCUSDT", "ETHUSDT"]):
        signal = _make_signal(id=f"sig_em_{i}", symbol=symbol)
        await system.bus.publish(Message(
            topic="mia.trade_signal",
            sender=AgentRole.MIA,
            priority=Priority.HIGH,
            payload=signal.model_dump(mode="json"),
        ))
    await asyncio.sleep(1.5)

    assert len(system.es.open_trades) == 2

    # Hit emergency stop
    result = await system.emergency_stop()

    assert result["circuit_breaker"] is True
    assert result["es_emergency"]["positions_closed"] == 2
    assert result["es_emergency"]["errors"] == 0
    assert len(system.es.open_trades) == 0
    assert system.crco._circuit_breaker_active is True


@pytest.mark.asyncio
async def test_emergency_stop_blocks_new_orders(system):
    """After emergency stop, new orders should be blocked."""
    await system.emergency_stop()

    decisions = []

    async def capture(msg: Message):
        decisions.append(msg)

    system.bus.subscribe("cso.trade_decision", AgentRole.PO, capture)

    signal = _make_signal(id="sig_after_em")
    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        payload=signal.model_dump(mode="json"),
    ))
    await asyncio.sleep(1.0)

    assert len(decisions) == 0  # Blocked by circuit breaker


@pytest.mark.asyncio
async def test_emergency_stop_audit_logged(system):
    """Emergency stop should be recorded in audit log."""
    await system.emergency_stop()

    logs = await system.audit.query(action="emergency_stop")
    assert len(logs) >= 1


# ══════════════════════════════════════════════
# 3. Mode Switch (PAPER ↔ LIVE)
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_mode_switch_paper_to_live_blocked(system):
    """Switching PAPER→LIVE should fail without API keys."""
    result = await system.switch_mode("LIVE")
    assert result["success"] is False
    assert "blockers" in result
    assert system.config.trading.mode == "PAPER"  # Reverted


@pytest.mark.asyncio
async def test_mode_switch_live_to_paper_always_allowed(system):
    """Switching LIVE→PAPER should always succeed."""
    system.config.trading.mode = "LIVE"
    result = await system.switch_mode("PAPER")
    assert result["success"] is True
    assert system.config.trading.mode == "PAPER"


@pytest.mark.asyncio
async def test_mode_switch_same_mode_noop(system):
    """Switching to current mode should be a no-op."""
    result = await system.switch_mode("PAPER")
    assert result["success"] is True
    assert "Already" in result["message"]


@pytest.mark.asyncio
async def test_mode_switch_invalid(system):
    """Invalid mode should be rejected."""
    result = await system.switch_mode("YOLO")
    assert result["success"] is False


@pytest.mark.asyncio
async def test_mode_switch_paper_to_live_with_valid_config(system):
    """PAPER→LIVE should succeed when all gates pass."""
    system.config.exchange.binance_api_key = "real_key"
    system.config.trading.max_risk_per_trade = 0.01
    system.config.trading.max_leverage = 10.0
    system.config.trading.max_daily_loss = 0.03

    result = await system.switch_mode("LIVE")
    assert result["success"] is True
    assert system.config.trading.mode == "LIVE"
    assert result["gate_result"]["passed"] is True

    # Clean up
    system.config.trading.mode = "PAPER"
    system.config.exchange.binance_api_key = ""


@pytest.mark.asyncio
async def test_mode_switch_audit_logged(system):
    """Mode switches should be recorded in audit log."""
    system.config.trading.mode = "LIVE"
    await system.switch_mode("PAPER")

    logs = await system.audit.query(action="mode_switch")
    assert len(logs) >= 1
    assert logs[0]["detail"]["to"] == "PAPER"
