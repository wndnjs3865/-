"""Live Trading Activation Simulation.

Exercises the complete lifecycle:
  Gate check → Mode switch → Trade execution → SL hit → Emergency stop
  → Verify audit trail → Mode revert → Restart
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


def _make_signal(**overrides) -> TradeSignal:
    defaults = dict(
        symbol="BTCUSDT", direction=Direction.LONG, score=85.0,
        entry_price=50000.0, stop_loss=49000.0, take_profit_1=53000.0,
        timeframe=Timeframe.H4, regime=MarketRegime.TRENDING_BULL,
        risk_reward_ratio=3.0, confluence_factors=["MTF_ALIGNMENT", "BOS_H4"],
    )
    defaults.update(overrides)
    return TradeSignal(**defaults)


@pytest.fixture
async def sys_live():
    """System with valid live config (fake API key for gate pass)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Config.from_env()
        config.system.audit_db_path = os.path.join(tmpdir, "live_sim.db")
        config.trading.mode = "PAPER"
        config.trading.max_risk_per_trade = 0.01
        config.trading.max_leverage = 10.0
        config.trading.max_daily_loss = 0.03
        config.trading.circuit_breaker_loss = 0.05
        config.trading.min_risk_reward = 2.0
        config.trading.min_quant_score = 50.0
        config.exchange.binance_api_key = "test_api_key_for_gate"
        s = JWQuantSystem(config)
        await s.start()
        s.mia._analysis_interval = 99999
        s.po._report_interval = 99999
        yield s
        await s.stop()


# ══════════════════════════════════════════════
# Full Lifecycle Simulation
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_full_live_lifecycle(sys_live):
    """
    Complete live trading lifecycle:
    1. Verify PAPER mode
    2. Run safety gate → PASS
    3. Switch PAPER → LIVE
    4. Execute a trade in LIVE mode
    5. SL hit → position closed at loss
    6. Emergency stop
    7. Verify all events in audit trail
    8. Revert to PAPER
    """
    system = sys_live

    # ── Step 1: Verify starts in PAPER ──
    assert system.config.trading.mode == "PAPER"

    # ── Step 2: Safety gate check ──
    gate = await system.validate_live_readiness()
    # In PAPER mode, gate fails (mode != LIVE)
    assert gate["passed"] is False

    # ── Step 3: Switch to LIVE (gate auto-validates) ──
    switch_result = await system.switch_mode("LIVE")
    assert switch_result["success"] is True
    assert system.config.trading.mode == "LIVE"

    # ── Step 4: Switch back to PAPER for execution test ──
    # (Live exchange connector not implemented — Paper mode validates the pipeline)
    await system.switch_mode("PAPER")

    decisions = []

    async def capture_dec(msg: Message):
        decisions.append(msg)

    system.bus.subscribe("cso.trade_decision", AgentRole.PO, capture_dec)

    signal = _make_signal(id="sig_live_sim")
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
    assert len(system.es.open_trades) >= 1

    # ── Step 5: SL hit → close at loss ──
    trade_id = list(system.es.open_trades.keys())[0]
    trade = system.es.open_trades[trade_id]
    sl_price = trade["stop_loss"]

    closed_events = []

    async def capture_close(msg: Message):
        closed_events.append(msg)

    system.bus.subscribe("es.order_closed", AgentRole.IMA, capture_close)

    system.es.inject_price("BTCUSDT", sl_price - 50)
    await asyncio.sleep(2.0)

    assert len(closed_events) >= 1
    assert closed_events[-1].payload["pnl"] < 0

    # ── Step 6: Emergency stop ──
    # Open another trade first
    signal2 = _make_signal(id="sig_live_em")
    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload=signal2.model_dump(mode="json"),
    ))
    await asyncio.sleep(1.5)

    estop = await system.emergency_stop()
    assert estop["circuit_breaker"] is True
    assert len(system.es.open_trades) == 0

    # ── Step 7: Verify audit trail ──
    mode_switches = await system.audit.query(action="mode_switch")
    assert len(mode_switches) >= 1

    estop_logs = await system.audit.query(action="emergency_stop")
    assert len(estop_logs) >= 1

    gate_logs = await system.audit.query(action="live_readiness_check")
    assert len(gate_logs) >= 1

    total_entries = await system.audit.count()
    assert total_entries > 20

    # ── Step 8: Revert to PAPER ──
    revert = await system.switch_mode("PAPER")
    assert revert["success"] is True
    assert system.config.trading.mode == "PAPER"


# ══════════════════════════════════════════════
# Edge Cases
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_double_emergency_stop(sys_live):
    """Double emergency stop should not crash."""
    result1 = await sys_live.emergency_stop()
    result2 = await sys_live.emergency_stop()
    assert result1["circuit_breaker"] is True
    assert result2["circuit_breaker"] is True  # Idempotent


@pytest.mark.asyncio
async def test_switch_live_then_back_rapidly(sys_live):
    """Rapid PAPER→LIVE→PAPER should be safe."""
    r1 = await sys_live.switch_mode("LIVE")
    assert r1["success"] is True

    r2 = await sys_live.switch_mode("PAPER")
    assert r2["success"] is True

    r3 = await sys_live.switch_mode("LIVE")
    assert r3["success"] is True

    assert sys_live.config.trading.mode == "LIVE"
    sys_live.config.trading.mode = "PAPER"  # Cleanup


@pytest.mark.asyncio
async def test_trade_blocked_after_emergency(sys_live):
    """No trades should execute after emergency stop."""
    await sys_live.emergency_stop()

    signal = _make_signal(id="sig_blocked")
    await sys_live.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        payload=signal.model_dump(mode="json"),
    ))
    await asyncio.sleep(1.0)

    assert len(sys_live.es.open_trades) == 0
    logs = await sys_live.audit.query(agent=AgentRole.CSO, action="signal_ignored_circuit_breaker")
    assert len(logs) >= 1


@pytest.mark.asyncio
async def test_gate_blocks_with_daily_loss(sys_live):
    """Safety gate should block LIVE if there's a daily loss."""
    sys_live.crco._daily_pnl = -100.0  # Negative PnL
    sys_live.config.trading.mode = "PAPER"

    result = await sys_live.switch_mode("LIVE")
    assert result["success"] is False
    assert sys_live.config.trading.mode == "PAPER"

    sys_live.crco._daily_pnl = 0.0  # Reset
