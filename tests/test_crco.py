"""Tests for CRCO Agent — 11 risk checks, circuit breaker, veto logic."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole, Direction, MarketRegime, Priority, Timeframe
from core.models import Message, TradeSignal, RiskAssessment
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from config import Config
from agents.crco import CRCOAgent


@pytest.fixture
async def system():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        audit = AuditLogger(db_path=db_path)
        await audit.initialize()
        bus = MessageBus(audit_logger=audit)
        await bus.start()
        config = Config.from_env()
        yield bus, audit, config
        await bus.stop()
        await audit.close()


def _make_signal(**overrides) -> TradeSignal:
    defaults = dict(
        id="sig_test001",
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
        confluence_factors=["BOS", "OB"],
    )
    defaults.update(overrides)
    return TradeSignal(**defaults)


@pytest.mark.asyncio
async def test_crco_approves_good_signal(system):
    """CRCO should approve a clean signal with no risk violations."""
    bus, audit, config = system
    crco = CRCOAgent(bus, audit, config)
    await crco.start()

    responses = []

    async def capture(msg: Message):
        responses.append(msg)

    bus.subscribe("crco.risk_assessment", AgentRole.CSO, capture)

    signal = _make_signal()
    await bus.publish(Message(
        topic="cso.request_risk_assessment",
        sender=AgentRole.CSO,
        priority=Priority.HIGH,
        payload={
            "signal_id": signal.id,
            "trade_signal": signal.model_dump(mode="json"),
            "request_type": "full_risk_assessment",
        },
    ))

    await asyncio.sleep(0.3)
    assert len(responses) == 1

    assessment = RiskAssessment(**responses[0].payload)
    assert assessment.approved is True
    assert len(assessment.veto_reasons) == 0
    assert assessment.max_position_size > 0
    assert len(assessment.validations) == 11

    # Verify audit log
    logs = await audit.query(agent=AgentRole.CRCO, action="risk_assessment")
    assert len(logs) >= 1

    await crco.stop()


@pytest.mark.asyncio
async def test_crco_vetos_low_rr(system):
    """CRCO should veto a signal with low risk:reward ratio."""
    bus, audit, config = system
    config.trading.min_risk_reward = 2.0
    crco = CRCOAgent(bus, audit, config)
    await crco.start()

    responses = []

    async def capture(msg: Message):
        responses.append(msg)

    bus.subscribe("crco.risk_assessment", AgentRole.CSO, capture)

    signal = _make_signal(risk_reward_ratio=1.2)
    await bus.publish(Message(
        topic="cso.request_risk_assessment",
        sender=AgentRole.CSO,
        payload={
            "signal_id": signal.id,
            "trade_signal": signal.model_dump(mode="json"),
        },
    ))

    await asyncio.sleep(0.3)
    assessment = RiskAssessment(**responses[0].payload)
    assert assessment.approved is False
    assert any("RR" in r for r in assessment.veto_reasons)

    await crco.stop()


@pytest.mark.asyncio
async def test_crco_vetos_max_positions(system):
    """CRCO should veto when max positions reached."""
    bus, audit, config = system
    config.trading.max_open_positions = 2
    crco = CRCOAgent(bus, audit, config)
    await crco.start()

    # Simulate 2 open positions
    crco._open_positions = {
        "BTCUSDT": {"size_usd": 1000, "risk_pct": 0.01},
        "ETHUSDT": {"size_usd": 800, "risk_pct": 0.01},
    }

    responses = []

    async def capture(msg: Message):
        responses.append(msg)

    bus.subscribe("crco.risk_assessment", AgentRole.CSO, capture)

    signal = _make_signal(symbol="SOLUSDT")
    await bus.publish(Message(
        topic="cso.request_risk_assessment",
        sender=AgentRole.CSO,
        payload={
            "signal_id": signal.id,
            "trade_signal": signal.model_dump(mode="json"),
        },
    ))

    await asyncio.sleep(0.3)
    assessment = RiskAssessment(**responses[0].payload)
    assert assessment.approved is False
    assert any("position" in r.lower() for r in assessment.veto_reasons)

    await crco.stop()


@pytest.mark.asyncio
async def test_circuit_breaker_activation(system):
    """Circuit breaker should activate on excessive daily loss."""
    bus, audit, config = system
    config.trading.circuit_breaker_loss = 0.05
    crco = CRCOAgent(bus, audit, config)
    await crco.start()

    circuit_msgs = []

    async def capture_cb(msg: Message):
        circuit_msgs.append(msg)

    bus.subscribe("crco.circuit_breaker", AgentRole.ES, capture_cb)

    # Simulate daily loss exceeding 5%
    crco._daily_start_value = 10000.0
    crco._daily_pnl = -600.0  # -6% loss
    crco._portfolio_value = 9400.0

    # Trigger via order close event
    await bus.publish(Message(
        topic="es.order_closed",
        sender=AgentRole.ES,
        payload={
            "symbol": "BTCUSDT",
            "pnl": -200.0,
            "signal_id": "sig_loss",
            "direction": "LONG",
            "entry_price": 50000,
            "exit_price": 49000,
            "stop_loss": 49000,
            "take_profit_1": 53000,
            "quantity": 0.01,
            "pnl_percent": -2.0,
        },
    ))

    await asyncio.sleep(0.5)
    assert crco._circuit_breaker_active is True

    # Circuit breaker broadcast should have been sent
    assert len(circuit_msgs) >= 1

    # Subsequent risk assessment should be vetoed
    responses = []

    async def capture_ra(msg: Message):
        responses.append(msg)

    bus.subscribe("crco.risk_assessment", AgentRole.CSO, capture_ra)

    signal = _make_signal()
    await bus.publish(Message(
        topic="cso.request_risk_assessment",
        sender=AgentRole.CSO,
        payload={
            "signal_id": signal.id,
            "trade_signal": signal.model_dump(mode="json"),
        },
    ))

    await asyncio.sleep(0.3)
    assessment = RiskAssessment(**responses[0].payload)
    assert assessment.approved is False
    assert assessment.circuit_breaker_active is True

    await crco.stop()


@pytest.mark.asyncio
async def test_consecutive_loss_veto(system):
    """CRCO should veto after 5 consecutive losses."""
    bus, audit, config = system
    crco = CRCOAgent(bus, audit, config)
    await crco.start()

    crco._consecutive_losses = 5

    responses = []

    async def capture(msg: Message):
        responses.append(msg)

    bus.subscribe("crco.risk_assessment", AgentRole.CSO, capture)

    signal = _make_signal()
    await bus.publish(Message(
        topic="cso.request_risk_assessment",
        sender=AgentRole.CSO,
        payload={
            "signal_id": signal.id,
            "trade_signal": signal.model_dump(mode="json"),
        },
    ))

    await asyncio.sleep(0.3)
    assessment = RiskAssessment(**responses[0].payload)
    assert assessment.approved is False
    assert any("consecutive" in r.lower() for r in assessment.veto_reasons)

    await crco.stop()


@pytest.mark.asyncio
async def test_leverage_limit_veto(system):
    """CRCO should veto if leverage exceeds max."""
    bus, audit, config = system
    config.trading.max_leverage = 5.0
    crco = CRCOAgent(bus, audit, config)
    await crco.start()

    responses = []

    async def capture(msg: Message):
        responses.append(msg)

    bus.subscribe("crco.risk_assessment", AgentRole.CSO, capture)

    signal = _make_signal()
    await bus.publish(Message(
        topic="cso.request_risk_assessment",
        sender=AgentRole.CSO,
        payload={
            "signal_id": signal.id,
            "trade_signal": signal.model_dump(mode="json"),
            "leverage": 15.0,
        },
    ))

    await asyncio.sleep(0.3)
    assessment = RiskAssessment(**responses[0].payload)
    assert assessment.approved is False
    assert any("leverage" in r.lower() for r in assessment.veto_reasons)

    await crco.stop()


@pytest.mark.asyncio
async def test_risk_snapshot(system):
    """risk_snapshot property should return current state."""
    bus, audit, config = system
    crco = CRCOAgent(bus, audit, config)
    await crco.start()

    snap = crco.risk_snapshot
    assert "portfolio_value" in snap
    assert "daily_pnl" in snap
    assert "circuit_breaker_active" in snap
    assert snap["circuit_breaker_active"] is False

    await crco.stop()


@pytest.mark.asyncio
async def test_all_11_validations_present(system):
    """All 11 risk checks must be present in every assessment."""
    bus, audit, config = system
    crco = CRCOAgent(bus, audit, config)
    await crco.start()

    responses = []

    async def capture(msg: Message):
        responses.append(msg)

    bus.subscribe("crco.risk_assessment", AgentRole.CSO, capture)

    signal = _make_signal()
    await bus.publish(Message(
        topic="cso.request_risk_assessment",
        sender=AgentRole.CSO,
        payload={
            "signal_id": signal.id,
            "trade_signal": signal.model_dump(mode="json"),
        },
    ))

    await asyncio.sleep(0.3)
    assessment = RiskAssessment(**responses[0].payload)

    check_names = [v.check_name for v in assessment.validations]
    expected = [
        "circuit_breaker", "daily_loss_limit", "max_drawdown",
        "position_limit", "single_asset_exposure", "risk_reward_ratio",
        "leverage_limit", "consecutive_loss_limit", "daily_trade_count",
        "portfolio_heat", "liquidation_risk",
    ]
    for name in expected:
        assert name in check_names, f"Missing check: {name}"

    await crco.stop()
