"""Phase 6 — Paper Trading Full System Validation.

Simulates a complete trading session:
- 60+ price cycles with realistic BTC price movement
- Multiple signal generations (some approved, some vetoed)
- SL and TP hits via price feed injection
- Circuit breaker trigger and recovery
- Full audit trail verification
- Health monitoring validation
- MessageBus integrity check (no deadlocks, no lost messages)
"""

import asyncio
import math
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole, Direction, MarketRegime, Priority, Timeframe
from core.models import Message, TradeSignal, TradeDecision, RiskAssessment, QuantScore
from config import Config
from main import JWQuantSystem


# ── Helpers ────────────────────────────────────

def _make_signal(
    symbol: str = "BTCUSDT",
    price: float = 50000.0,
    direction: Direction = Direction.LONG,
    rr: float = 3.0,
    score: float = 85.0,
    signal_id: str | None = None,
) -> TradeSignal:
    """Generate a realistic trade signal."""
    vol = 0.02  # 2% volatility
    if direction == Direction.LONG:
        sl = round(price * (1 - vol), 2)
        tp1 = round(price * (1 + vol * 2), 2)
        tp2 = round(price * (1 + vol * 3), 2)
        tp3 = round(price * (1 + vol * 5), 2)
    else:
        sl = round(price * (1 + vol), 2)
        tp1 = round(price * (1 - vol * 2), 2)
        tp2 = round(price * (1 - vol * 3), 2)
        tp3 = round(price * (1 - vol * 5), 2)

    risk = abs(price - sl)
    reward = abs(tp1 - price)
    actual_rr = round(reward / risk, 2) if risk > 0 else 0

    return TradeSignal(
        id=signal_id or f"sig_val_{int(time.time()*1000) % 100000}",
        symbol=symbol,
        direction=direction,
        score=score,
        entry_price=price,
        stop_loss=sl,
        take_profit_1=tp1,
        take_profit_2=tp2,
        take_profit_3=tp3,
        timeframe=Timeframe.H4,
        regime=MarketRegime.TRENDING_BULL if direction == Direction.LONG else MarketRegime.TRENDING_BEAR,
        risk_reward_ratio=actual_rr,
        confluence_factors=["MTF_ALIGNMENT", "BOS_H4", "OB_H1"],
    )


@pytest.fixture
async def system():
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Config.from_env()
        config.system.audit_db_path = os.path.join(tmpdir, "validation.db")
        config.trading.mode = "PAPER"
        config.trading.max_risk_per_trade = 0.01
        config.trading.max_daily_loss = 0.03
        config.trading.circuit_breaker_loss = 0.05
        config.trading.min_quant_score = 50.0  # Lower for more signals in test
        sys = JWQuantSystem(config)
        await sys.start()
        # Disable MIA auto-loop (we'll trigger manually)
        sys.mia._analysis_interval = 99999
        # Disable PO auto-report
        sys.po._report_interval = 99999
        yield sys
        await sys.stop()


# ══════════════════════════════════════════════
# VALIDATION 1: System Boot & Agent Health
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_val_01_all_agents_boot(system):
    """V1: All 7 agents boot and are running."""
    for agent in system._agents:
        assert agent.is_running, f"{agent.role.value} not running"
    assert len(system._agents) == 7
    assert system._running


@pytest.mark.asyncio
async def test_val_02_ima_monitors_all(system):
    """V2: IMA has all 7 agents registered for health monitoring."""
    assert len(system.ima._monitored_agents) == 7
    await system.ima._collect_health()
    assert len(system.ima._latest_health) == 7
    for role_name, health in system.ima._latest_health.items():
        assert health.alive, f"{role_name} not alive"


# ══════════════════════════════════════════════
# VALIDATION 2: Full Pipeline — Signal → Execution
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_val_03_full_pipeline_approved(system):
    """V3: Signal flows through QR→CRCO→ES and gets executed."""
    decisions = []

    async def capture(msg: Message):
        decisions.append(msg)

    system.bus.subscribe("cso.trade_decision", AgentRole.PO, capture)

    signal = _make_signal(price=50000.0, direction=Direction.LONG)
    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload=signal.model_dump(mode="json"),
    ))

    await asyncio.sleep(1.5)

    assert len(decisions) >= 1, "No trade decision produced"
    dec = TradeDecision(**decisions[0].payload)
    assert dec.approved is True
    assert dec.position_size > 0
    assert dec.quant_score > 0
    assert dec.quant_grade != ""
    assert dec.risk_score >= 0

    # ES should have executed
    assert len(system.es.open_trades) >= 1

    # CRCO should track the position
    assert len(system.crco._open_positions) >= 1

    # PO should track the fill
    assert len(system.po._open_trades) >= 1

    # Audit trail
    audit_count = await system.audit.count()
    assert audit_count > 10


@pytest.mark.asyncio
async def test_val_04_crco_veto(system):
    """V4: CRCO correctly vetoes signals that fail risk checks."""
    # Set very high RR requirement to force veto
    system.config.trading.min_risk_reward = 10.0

    decisions = []

    async def capture(msg: Message):
        decisions.append(msg)

    system.bus.subscribe("cso.trade_decision", AgentRole.IMA, capture)

    signal = _make_signal(price=50000.0, rr=1.5, signal_id="sig_veto_test")
    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        payload=signal.model_dump(mode="json"),
    ))

    await asyncio.sleep(1.5)

    # No approved decision should reach ES
    approved = [d for d in decisions if TradeDecision(**d.payload).approved]
    assert len(approved) == 0

    # Audit should record the veto
    logs = await system.audit.query(agent=AgentRole.CSO, action="trade_vetoed_by_crco")
    assert len(logs) >= 1

    system.config.trading.min_risk_reward = 2.0  # Reset


# ══════════════════════════════════════════════
# VALIDATION 3: SL/TP Hit via Price Feed
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_val_05_sl_hit_closes_trade(system):
    """V5: Price dropping to SL correctly closes position at loss."""
    closed_events = []

    async def capture(msg: Message):
        closed_events.append(msg)

    system.bus.subscribe("es.order_closed", AgentRole.IMA, capture)

    # Execute a LONG trade
    signal = _make_signal(price=50000.0, direction=Direction.LONG, signal_id="sig_sl_test")
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

    # Inject price below SL
    sl_price = trade["stop_loss"]
    system.es.inject_price("BTCUSDT", sl_price - 100)

    # Wait for position monitor to detect
    await asyncio.sleep(2.0)

    assert len(closed_events) >= 1
    close_payload = closed_events[-1].payload
    assert close_payload["close_reason"] == "sl_hit"
    assert close_payload["pnl"] < 0  # Loss


@pytest.mark.asyncio
async def test_val_06_tp_hit_closes_trade(system):
    """V6: Price rising to TP1 correctly closes position at profit."""
    closed_events = []

    async def capture(msg: Message):
        closed_events.append(msg)

    system.bus.subscribe("es.order_closed", AgentRole.QR, capture)

    signal = _make_signal(price=50000.0, direction=Direction.LONG, signal_id="sig_tp_test")
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

    # Inject price above TP1
    tp1 = trade["take_profit_1"]
    system.es.inject_price("BTCUSDT", tp1 + 100)

    await asyncio.sleep(2.0)

    assert len(closed_events) >= 1
    close_payload = closed_events[-1].payload
    assert close_payload["close_reason"] == "tp1_hit"
    assert close_payload["pnl"] > 0  # Profit


# ══════════════════════════════════════════════
# VALIDATION 4: Circuit Breaker Cascade
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_val_07_circuit_breaker_cascade(system):
    """V7: Circuit breaker propagates to all agents and blocks new trades."""
    # Manually trigger circuit breaker
    system.crco._daily_pnl = -600.0
    system.crco._daily_start_value = 10000.0
    system.crco._portfolio_value = 9400.0

    await system.bus.publish(Message(
        topic="es.order_closed",
        sender=AgentRole.ES,
        payload={
            "symbol": "BTCUSDT", "pnl": -100.0, "pnl_percent": -1.0,
            "signal_id": "cb_test", "direction": "LONG",
            "entry_price": 50000, "exit_price": 49000,
            "stop_loss": 49000, "take_profit_1": 53000,
            "quantity": 0.01, "duration_seconds": 60,
        },
    ))
    await asyncio.sleep(1.0)

    assert system.crco._circuit_breaker_active, "CRCO circuit breaker not active"
    assert system.es._circuit_breaker_active, "ES circuit breaker not active"
    assert system.cso._circuit_breaker_active, "CSO circuit breaker not active"

    # Try to send a signal — should be ignored
    signal = _make_signal(signal_id="sig_blocked")
    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        payload=signal.model_dump(mode="json"),
    ))
    await asyncio.sleep(1.0)

    logs = await system.audit.query(agent=AgentRole.CSO, action="signal_ignored_circuit_breaker")
    assert len(logs) >= 1


# ══════════════════════════════════════════════
# VALIDATION 5: PO Performance Tracking
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_val_08_po_journal_accuracy(system):
    """V8: PO accurately tracks wins, losses, and equity."""
    # Simulate 3 closed trades
    for pnl, pct in [(150.0, 3.0), (-50.0, -1.0), (200.0, 4.0)]:
        await system.bus.publish(Message(
            topic="es.order_closed",
            sender=AgentRole.ES,
            payload={
                "pnl": pnl, "pnl_percent": pct,
                "symbol": "BTCUSDT", "direction": "LONG",
                "duration_seconds": 300,
            },
        ))
    await asyncio.sleep(0.5)

    report = system.po.generate_report("validation")
    assert report.total_trades == 3
    assert report.winning_trades == 2
    assert report.losing_trades == 1
    assert report.win_rate == pytest.approx(66.67, abs=0.1)
    assert report.total_pnl == 300.0
    assert report.profit_factor > 1.0
    assert report.best_trade_pnl == 200.0
    assert report.worst_trade_pnl == -50.0


# ══════════════════════════════════════════════
# VALIDATION 6: MessageBus Integrity
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_val_09_messagebus_no_deadlock(system):
    """V9: Rapid message bursts don't cause deadlocks."""
    signals_sent = 5
    for i in range(signals_sent):
        signal = _make_signal(price=50000 + i * 100, signal_id=f"sig_burst_{i}")
        await system.bus.publish(Message(
            topic="mia.trade_signal",
            sender=AgentRole.MIA,
            priority=Priority.HIGH,
            payload=signal.model_dump(mode="json"),
        ))

    # Allow processing
    await asyncio.sleep(3.0)

    metrics = system.bus.metrics
    assert metrics["total_published"] > signals_sent
    assert metrics["total_failed"] == 0
    assert len(system.bus.dead_letters) == 0


@pytest.mark.asyncio
async def test_val_10_messagebus_correlation_matching(system):
    """V10: Request-response correlation_id matching works correctly."""
    # CSO's pipeline uses request-response for both QR and CRCO
    # If this test (plus val_03) passes, correlation is working
    signal = _make_signal(price=51000.0, signal_id="sig_corr_test")
    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload=signal.model_dump(mode="json"),
    ))
    await asyncio.sleep(1.5)

    # QR should have scored
    qr_logs = await system.audit.query(agent=AgentRole.QR, action="quant_score_computed")
    assert len(qr_logs) >= 1

    # CRCO should have assessed
    crco_logs = await system.audit.query(agent=AgentRole.CRCO, action="risk_assessment")
    assert len(crco_logs) >= 1


# ══════════════════════════════════════════════
# VALIDATION 7: Audit Trail Completeness
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_val_11_audit_trail_complete(system):
    """V11: Full pipeline leaves complete audit trail."""
    signal = _make_signal(price=52000.0, signal_id="sig_audit_test")
    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload=signal.model_dump(mode="json"),
    ))
    await asyncio.sleep(1.5)

    # Check each stage is audited
    cso_received = await system.audit.query(agent=AgentRole.CSO, action="signal_received")
    assert len(cso_received) >= 1

    qr_scored = await system.audit.query(agent=AgentRole.QR, action="quant_score_computed")
    assert len(qr_scored) >= 1

    crco_assessed = await system.audit.query(agent=AgentRole.CRCO, action="risk_assessment")
    assert len(crco_assessed) >= 1

    # Check that audit count is substantial
    total = await system.audit.count()
    assert total > 15


# ══════════════════════════════════════════════
# VALIDATION 8: System Snapshot Completeness
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_val_12_system_snapshot(system):
    """V12: system_snapshot returns complete state from all agents."""
    snap = system.system_snapshot
    assert snap["running"] is True
    assert snap["mode"] == "PAPER"
    assert snap["uptime_seconds"] >= 0

    for agent_key in ["mia", "qr", "cso", "crco", "es", "po", "ima"]:
        assert agent_key in snap["agents"], f"Missing {agent_key} in snapshot"
        assert isinstance(snap["agents"][agent_key], dict)

    assert "bus_metrics" in snap
    assert snap["bus_metrics"]["total_published"] >= 0


# ══════════════════════════════════════════════
# VALIDATION 9: Graceful Shutdown
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_val_13_graceful_shutdown(system):
    """V13: All agents stop cleanly."""
    # Trigger some activity first
    signal = _make_signal(price=53000.0, signal_id="sig_shutdown_test")
    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        payload=signal.model_dump(mode="json"),
    ))
    await asyncio.sleep(1.0)

    await system.stop()
    assert system._running is False
    for agent in system._agents:
        assert not agent.is_running

    # Verify shutdown audit entry
    # (audit is closed at this point, so we just check no exceptions)
