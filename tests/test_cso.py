"""Tests for CSO (Chief Strategy Officer) Agent — full pipeline orchestration."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole, Direction, MarketRegime, Priority, Timeframe
from core.models import Message, TradeDecision, TradeSignal
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from config import Config
from agents.cso import CSOAgent
from agents.qr import QRAgent
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
        id="sig_cso_test",
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
async def test_cso_full_pipeline_approved(system):
    """Full pipeline: MIA signal → QR score → CRCO approve → ES decision."""
    bus, audit, config = system

    # Start all agents in the pipeline
    qr = QRAgent(bus, audit, config)
    crco = CRCOAgent(bus, audit, config)
    cso = CSOAgent(bus, audit, config)

    await qr.start()
    await crco.start()
    await cso.start()

    decisions = []

    async def capture_decision(msg: Message):
        decisions.append(msg)

    bus.subscribe("cso.trade_decision", AgentRole.ES, capture_decision)

    # Simulate MIA sending a signal
    signal = _make_signal()
    await bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload=signal.model_dump(mode="json"),
    ))

    await asyncio.sleep(1.0)

    # CSO should have produced a decision
    assert len(decisions) == 1
    dec = TradeDecision(**decisions[0].payload)
    assert dec.approved is True
    assert dec.symbol == "BTCUSDT"
    assert dec.direction == Direction.LONG
    assert dec.position_size > 0
    assert dec.quant_score > 0
    assert dec.quant_grade != ""

    # Check CSO stats
    assert cso.cso_snapshot["decisions_approved"] == 1
    assert cso.cso_snapshot["decisions_vetoed"] == 0

    await cso.stop()
    await crco.stop()
    await qr.stop()


@pytest.mark.asyncio
async def test_cso_vetoed_by_crco(system):
    """CRCO veto should prevent CSO from sending decision to ES."""
    bus, audit, config = system
    config.trading.min_risk_reward = 5.0  # Set high RR requirement to trigger veto

    qr = QRAgent(bus, audit, config)
    crco = CRCOAgent(bus, audit, config)
    cso = CSOAgent(bus, audit, config)

    await qr.start()
    await crco.start()
    await cso.start()

    decisions = []

    async def capture_decision(msg: Message):
        decisions.append(msg)

    bus.subscribe("cso.trade_decision", AgentRole.ES, capture_decision)

    signal = _make_signal(risk_reward_ratio=1.5)  # Below min 5.0
    await bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        payload=signal.model_dump(mode="json"),
    ))

    await asyncio.sleep(1.0)

    # No decision should be sent
    assert len(decisions) == 0
    assert cso.cso_snapshot["decisions_vetoed"] == 1

    # Audit should record the veto
    logs = await audit.query(agent=AgentRole.CSO, action="trade_vetoed_by_crco")
    assert len(logs) >= 1

    await cso.stop()
    await crco.stop()
    await qr.stop()


@pytest.mark.asyncio
async def test_cso_rejects_low_quant_score(system):
    """CSO should reject signals with quant score below minimum."""
    bus, audit, config = system
    config.trading.min_quant_score = 99.0  # Very high threshold

    qr = QRAgent(bus, audit, config)
    crco = CRCOAgent(bus, audit, config)
    cso = CSOAgent(bus, audit, config)

    await qr.start()
    await crco.start()
    await cso.start()

    decisions = []

    async def capture(msg: Message):
        decisions.append(msg)

    bus.subscribe("cso.trade_decision", AgentRole.ES, capture)

    signal = _make_signal(score=50.0)
    await bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        payload=signal.model_dump(mode="json"),
    ))

    await asyncio.sleep(1.0)
    assert len(decisions) == 0

    logs = await audit.query(agent=AgentRole.CSO, action="signal_rejected_low_score")
    assert len(logs) >= 1

    await cso.stop()
    await crco.stop()
    await qr.stop()


@pytest.mark.asyncio
async def test_cso_circuit_breaker(system):
    """CSO should ignore signals during circuit breaker."""
    bus, audit, config = system
    cso = CSOAgent(bus, audit, config)
    await cso.start()

    # Activate circuit breaker
    await bus.publish(Message(
        topic="crco.circuit_breaker",
        sender=AgentRole.CRCO,
        priority=Priority.URGENT,
        payload={"active": True, "reason": "test"},
    ))
    await asyncio.sleep(0.2)

    decisions = []

    async def capture(msg: Message):
        decisions.append(msg)

    bus.subscribe("cso.trade_decision", AgentRole.ES, capture)

    signal = _make_signal()
    await bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        payload=signal.model_dump(mode="json"),
    ))

    await asyncio.sleep(0.5)
    assert len(decisions) == 0

    logs = await audit.query(agent=AgentRole.CSO, action="signal_ignored_circuit_breaker")
    assert len(logs) >= 1

    await cso.stop()


@pytest.mark.asyncio
async def test_cso_snapshot(system):
    bus, audit, config = system
    cso = CSOAgent(bus, audit, config)
    await cso.start()

    snap = cso.cso_snapshot
    assert snap["decisions_made"] == 0
    assert snap["decisions_approved"] == 0
    assert snap["circuit_breaker_active"] is False

    await cso.stop()
