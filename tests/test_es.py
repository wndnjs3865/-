"""Tests for ES (Execution Specialist) Agent — Paper execution, SL/TP, Trailing."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import (
    AgentRole, Direction, OrderStatus, OrderType, Priority, TradeMode,
)
from core.models import Message, TradeDecision, OrderResult
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from config import Config
from agents.es import ESAgent


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


def _make_decision(**overrides) -> TradeDecision:
    defaults = dict(
        decision_id="dec_test001",
        signal_id="sig_test001",
        symbol="BTCUSDT",
        direction=Direction.LONG,
        approved=True,
        entry_price=50000.0,
        stop_loss=49000.0,
        take_profit_1=53000.0,
        take_profit_2=55000.0,
        position_size=0.1,
        leverage=3.0,
        order_type=OrderType.LIMIT,
        trade_mode=TradeMode.PAPER,
        quant_score=82.0,
        quant_grade="A",
        risk_score=20.0,
    )
    defaults.update(overrides)
    return TradeDecision(**defaults)


@pytest.mark.asyncio
async def test_paper_execution_flow(system):
    """Full paper execution: decision → submitted → filled."""
    bus, audit, config = system
    es = ESAgent(bus, audit, config)
    await es.start()

    submitted = []
    filled = []

    async def on_submitted(msg: Message):
        submitted.append(msg)

    async def on_filled(msg: Message):
        filled.append(msg)

    bus.subscribe("es.order_submitted", AgentRole.PO, on_submitted)
    bus.subscribe("es.order_filled", AgentRole.PO, on_filled)

    decision = _make_decision()
    await bus.publish(Message(
        topic="cso.trade_decision",
        sender=AgentRole.CSO,
        priority=Priority.URGENT,
        payload=decision.model_dump(mode="json"),
    ))

    await asyncio.sleep(0.5)

    # Verify submitted event
    assert len(submitted) == 1
    sub_result = OrderResult(**submitted[0].payload)
    assert sub_result.status == OrderStatus.SUBMITTED
    assert sub_result.symbol == "BTCUSDT"

    # Verify filled event
    assert len(filled) == 1
    fill_result = OrderResult(**filled[0].payload)
    assert fill_result.status == OrderStatus.FILLED
    assert fill_result.filled_price is not None
    assert fill_result.filled_quantity == 0.1
    assert fill_result.commission > 0

    # Verify open trade registered
    assert len(es.open_trades) == 1

    # Verify audit log
    logs = await audit.query(agent=AgentRole.ES, action="paper_order_filled")
    assert len(logs) >= 1

    await es.stop()


@pytest.mark.asyncio
async def test_rejects_non_approved(system):
    """ES should reject a decision that is not approved."""
    bus, audit, config = system
    es = ESAgent(bus, audit, config)
    await es.start()

    events = []

    async def on_any(msg: Message):
        events.append(msg)

    bus.subscribe("es.order_submitted", AgentRole.PO, on_any)
    bus.subscribe("es.order_filled", AgentRole.PO, on_any)

    decision = _make_decision(approved=False)
    await bus.publish(Message(
        topic="cso.trade_decision",
        sender=AgentRole.CSO,
        payload=decision.model_dump(mode="json"),
    ))

    await asyncio.sleep(0.3)
    assert len(events) == 0
    assert len(es.open_trades) == 0

    # Audit should record rejection
    logs = await audit.query(agent=AgentRole.ES, action="decision_rejected")
    assert len(logs) >= 1

    await es.stop()


@pytest.mark.asyncio
async def test_circuit_breaker_blocks_orders(system):
    """ES should reject orders when circuit breaker is active."""
    bus, audit, config = system
    es = ESAgent(bus, audit, config)
    await es.start()

    failed = []

    async def on_failed(msg: Message):
        failed.append(msg)

    bus.subscribe("es.order_failed", AgentRole.PO, on_failed)

    # Activate circuit breaker
    await bus.publish(Message(
        topic="crco.circuit_breaker",
        sender=AgentRole.CRCO,
        priority=Priority.URGENT,
        payload={"active": True, "reason": "test"},
    ))
    await asyncio.sleep(0.2)

    # Try to execute
    decision = _make_decision()
    await bus.publish(Message(
        topic="cso.trade_decision",
        sender=AgentRole.CSO,
        payload=decision.model_dump(mode="json"),
    ))

    await asyncio.sleep(0.3)
    assert len(failed) == 1
    fail_result = OrderResult(**failed[0].payload)
    assert fail_result.status == OrderStatus.FAILED
    assert "circuit breaker" in fail_result.error_message.lower()

    await es.stop()


@pytest.mark.asyncio
async def test_close_trade(system):
    """Test manual trade close with PnL calculation."""
    bus, audit, config = system
    es = ESAgent(bus, audit, config)
    await es.start()

    closed = []

    async def on_closed(msg: Message):
        closed.append(msg)

    bus.subscribe("es.order_closed", AgentRole.PO, on_closed)

    # Execute a trade first
    decision = _make_decision()
    await bus.publish(Message(
        topic="cso.trade_decision",
        sender=AgentRole.CSO,
        payload=decision.model_dump(mode="json"),
    ))
    await asyncio.sleep(0.3)

    # Get trade_id
    trade_ids = list(es.open_trades.keys())
    assert len(trade_ids) == 1
    trade_id = trade_ids[0]

    # Close at profit
    await es.close_trade(trade_id, exit_price=52000.0, reason="tp1_hit")
    await asyncio.sleep(0.3)

    assert len(closed) == 1
    close_payload = closed[0].payload
    assert close_payload["pnl"] > 0  # Profitable
    assert close_payload["close_reason"] == "tp1_hit"
    assert len(es.open_trades) == 0

    # Trade history updated
    assert es.trade_count == 1
    history = es.get_trade_history()
    assert history[0].pnl > 0

    await es.stop()


@pytest.mark.asyncio
async def test_close_trade_loss(system):
    """Test trade close at a loss."""
    bus, audit, config = system
    es = ESAgent(bus, audit, config)
    await es.start()

    closed = []

    async def on_closed(msg: Message):
        closed.append(msg)

    bus.subscribe("es.order_closed", AgentRole.PO, on_closed)

    decision = _make_decision()
    await bus.publish(Message(
        topic="cso.trade_decision",
        sender=AgentRole.CSO,
        payload=decision.model_dump(mode="json"),
    ))
    await asyncio.sleep(0.3)

    trade_id = list(es.open_trades.keys())[0]
    await es.close_trade(trade_id, exit_price=49000.0, reason="sl_hit")
    await asyncio.sleep(0.3)

    assert closed[0].payload["pnl"] < 0

    await es.stop()


@pytest.mark.asyncio
async def test_trailing_stop_activation(system):
    """Trailing stop should activate after TP1 reached."""
    bus, audit, config = system
    es = ESAgent(bus, audit, config)
    await es.start()

    decision = _make_decision(take_profit_1=53000.0)
    await bus.publish(Message(
        topic="cso.trade_decision",
        sender=AgentRole.CSO,
        payload=decision.model_dump(mode="json"),
    ))
    await asyncio.sleep(0.3)

    trade_id = list(es.open_trades.keys())[0]

    # Price hasn't reached TP1 yet
    result = es.update_trailing_stop(trade_id, 51000.0)
    assert result is None

    # Price reaches TP1 → trailing activates
    result = es.update_trailing_stop(trade_id, 53500.0)
    assert result is not None
    assert result < 53500.0

    # Price moves higher → trailing moves up
    result2 = es.update_trailing_stop(trade_id, 55000.0)
    assert result2 is not None
    assert result2 > result

    await es.stop()


@pytest.mark.asyncio
async def test_execution_snapshot(system):
    """execution_snapshot should reflect current state."""
    bus, audit, config = system
    es = ESAgent(bus, audit, config)
    await es.start()

    snap = es.execution_snapshot
    assert snap["open_trades"] == 0
    assert snap["total_executed"] == 0
    assert snap["circuit_breaker_active"] is False

    await es.stop()


@pytest.mark.asyncio
async def test_multiple_concurrent_orders(system):
    """ES should handle multiple orders concurrently."""
    bus, audit, config = system
    es = ESAgent(bus, audit, config)
    await es.start()

    for i, symbol in enumerate(["BTCUSDT", "ETHUSDT", "SOLUSDT"]):
        decision = _make_decision(
            decision_id=f"dec_{i}",
            signal_id=f"sig_{i}",
            symbol=symbol,
        )
        await bus.publish(Message(
            topic="cso.trade_decision",
            sender=AgentRole.CSO,
            payload=decision.model_dump(mode="json"),
        ))

    await asyncio.sleep(0.8)
    assert len(es.open_trades) == 3

    # Close all
    for trade_id in list(es.open_trades.keys()):
        await es.close_trade(trade_id, exit_price=51000.0, reason="manual")

    await asyncio.sleep(0.3)
    assert len(es.open_trades) == 0
    assert es.trade_count == 3

    await es.stop()
