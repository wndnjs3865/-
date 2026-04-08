"""Tests for QR (Quant Researcher) Agent."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole, Direction, MarketRegime, Priority, Timeframe
from core.models import Message, QuantScore, TradeSignal
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from config import Config
from agents.qr import QRAgent


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
        id="sig_qr_test",
        symbol="BTCUSDT",
        direction=Direction.LONG,
        score=85.0,
        entry_price=50000.0,
        stop_loss=49000.0,
        take_profit_1=53000.0,
        timeframe=Timeframe.H4,
        regime=MarketRegime.TRENDING_BULL,
        risk_reward_ratio=3.0,
        confluence_factors=["MTF_ALIGNMENT", "BOS_H4", "OB_H1"],
    )
    defaults.update(overrides)
    return TradeSignal(**defaults)


@pytest.mark.asyncio
async def test_qr_scores_signal(system):
    """QR should compute 8-dimension score and publish."""
    bus, audit, config = system
    qr = QRAgent(bus, audit, config)
    await qr.start()

    responses = []

    async def capture(msg: Message):
        responses.append(msg)

    bus.subscribe("qr.quant_score", AgentRole.CSO, capture)

    signal = _make_signal()
    await bus.publish(Message(
        topic="cso.request_quant_score",
        sender=AgentRole.CSO,
        payload={
            "signal_id": signal.id,
            "trade_signal": signal.model_dump(mode="json"),
        },
    ))

    await asyncio.sleep(0.3)
    assert len(responses) == 1

    qs = QuantScore(**responses[0].payload)
    assert qs.signal_id == "sig_qr_test"
    assert qs.symbol == "BTCUSDT"
    assert 0 <= qs.total_score <= 100
    assert qs.grade in ("A+", "A", "B+", "B", "C", "D", "F")
    assert len(qs.reasons) > 0

    # All 8 dimensions should be in valid range
    for dim in ["trend_alignment", "momentum_score", "volume_profile",
                "volatility_regime", "orderflow_imbalance", "correlation_filter",
                "statistical_edge", "timing_score"]:
        val = getattr(qs, dim)
        assert 0 <= val <= 100, f"{dim}={val} out of range"

    await qr.stop()


@pytest.mark.asyncio
async def test_qr_request_response_pattern(system):
    """QR should work with CSO's request-response pattern."""
    bus, audit, config = system
    qr = QRAgent(bus, audit, config)
    await qr.start()

    signal = _make_signal()

    response = await bus.publish_and_wait(
        Message(
            topic="cso.request_quant_score",
            sender=AgentRole.CSO,
            payload={
                "signal_id": signal.id,
                "trade_signal": signal.model_dump(mode="json"),
            },
        ),
        response_topic="qr.quant_score",
        timeout=5.0,
    )

    assert response is not None
    qs = QuantScore(**response.payload)
    assert qs.total_score > 0

    await qr.stop()


@pytest.mark.asyncio
async def test_qr_high_score_for_strong_signal(system):
    """Strong signal should get a decent score."""
    bus, audit, config = system
    qr = QRAgent(bus, audit, config)
    await qr.start()

    responses = []

    async def capture(msg: Message):
        responses.append(msg)

    bus.subscribe("qr.quant_score", AgentRole.CSO, capture)

    signal = _make_signal(
        score=95.0,
        risk_reward_ratio=4.0,
        regime=MarketRegime.TRENDING_BULL,
        confluence_factors=["MTF_ALIGNMENT", "BOS_D1", "BOS_H4", "OB_H1", "FVG_M15"],
    )

    await bus.publish(Message(
        topic="cso.request_quant_score",
        sender=AgentRole.CSO,
        payload={
            "signal_id": signal.id,
            "trade_signal": signal.model_dump(mode="json"),
        },
    ))

    await asyncio.sleep(0.3)
    qs = QuantScore(**responses[0].payload)
    assert qs.total_score >= 50  # Strong signals should score above average

    await qr.stop()


@pytest.mark.asyncio
async def test_qr_audit_log(system):
    bus, audit, config = system
    qr = QRAgent(bus, audit, config)
    await qr.start()

    signal = _make_signal()
    await bus.publish(Message(
        topic="cso.request_quant_score",
        sender=AgentRole.CSO,
        payload={
            "signal_id": signal.id,
            "trade_signal": signal.model_dump(mode="json"),
        },
    ))
    await asyncio.sleep(0.3)

    logs = await audit.query(agent=AgentRole.QR, action="quant_score_computed")
    assert len(logs) >= 1
    assert logs[0]["detail"]["signal_id"] == "sig_qr_test"

    await qr.stop()


@pytest.mark.asyncio
async def test_qr_snapshot(system):
    bus, audit, config = system
    qr = QRAgent(bus, audit, config)
    await qr.start()

    snap = qr.qr_snapshot
    assert snap["scores_computed"] == 0

    await qr.stop()
