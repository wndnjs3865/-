"""Tests for QuantPulse v3 MessageBus."""

import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole, Priority
from core.models import Message
from core.message_bus import MessageBus


@pytest.fixture
async def bus():
    b = MessageBus()
    await b.start()
    yield b
    await b.stop()


@pytest.mark.asyncio
async def test_publish_subscribe(bus):
    received = []

    async def handler(msg: Message):
        received.append(msg)

    bus.subscribe("test.topic", AgentRole.CSO, handler)

    await bus.publish(Message(
        topic="test.topic",
        sender=AgentRole.MIA,
        payload={"data": "hello"},
    ))

    await asyncio.sleep(0.1)
    assert len(received) == 1
    assert received[0].payload["data"] == "hello"


@pytest.mark.asyncio
async def test_priority_ordering(bus):
    received = []

    async def handler(msg: Message):
        received.append(msg.priority)

    bus.subscribe("priority.test", AgentRole.CSO, handler)

    # Pause dispatcher temporarily by stopping and manually queuing
    await bus.stop()
    bus._running = False

    # Queue messages in reverse priority order
    await bus._queue.put((-1, 0, Message(topic="priority.test", sender=AgentRole.MIA, priority=Priority.LOW)))
    await bus._queue.put((-4, 0, Message(topic="priority.test", sender=AgentRole.MIA, priority=Priority.URGENT)))
    await bus._queue.put((-2, 0, Message(topic="priority.test", sender=AgentRole.MIA, priority=Priority.NORMAL)))
    await bus._queue.put((-3, 0, Message(topic="priority.test", sender=AgentRole.MIA, priority=Priority.HIGH)))

    # Manually deliver in order
    while not bus._queue.empty():
        _, _, msg = await bus._queue.get()
        await bus._deliver(msg)

    assert received[0] == Priority.URGENT
    assert received[1] == Priority.HIGH
    assert received[2] == Priority.NORMAL
    assert received[3] == Priority.LOW


@pytest.mark.asyncio
async def test_wildcard_subscription(bus):
    received = []

    async def handler(msg: Message):
        received.append(msg.topic)

    bus.subscribe("es.*", AgentRole.PO, handler)

    await bus.publish(Message(topic="es.order_filled", sender=AgentRole.ES))
    await bus.publish(Message(topic="es.order_failed", sender=AgentRole.ES))
    await bus.publish(Message(topic="mia.trade_signal", sender=AgentRole.MIA))

    await asyncio.sleep(0.1)
    assert len(received) == 2
    assert "es.order_filled" in received
    assert "es.order_failed" in received


@pytest.mark.asyncio
async def test_request_response(bus):
    async def responder(msg: Message):
        await bus.publish(Message(
            topic="crco.risk_assessment",
            sender=AgentRole.CRCO,
            payload={"approved": True},
            correlation_id=msg.correlation_id,
        ))

    bus.subscribe("cso.request_risk_assessment", AgentRole.CRCO, responder)

    response = await bus.publish_and_wait(
        Message(
            topic="cso.request_risk_assessment",
            sender=AgentRole.CSO,
            payload={"signal_id": "test123"},
        ),
        response_topic="crco.risk_assessment",
        timeout=5.0,
    )

    assert response is not None
    assert response.payload["approved"] is True


@pytest.mark.asyncio
async def test_metrics(bus):
    async def noop(msg: Message):
        pass

    bus.subscribe("metrics.test", AgentRole.CSO, noop)
    await bus.publish(Message(topic="metrics.test", sender=AgentRole.MIA))
    await asyncio.sleep(0.1)

    m = bus.metrics
    assert m["total_published"] >= 1
    assert m["total_delivered"] >= 1
    assert m["subscriptions"] >= 1


@pytest.mark.asyncio
async def test_history(bus):
    for i in range(5):
        await bus.publish(Message(
            topic="history.test",
            sender=AgentRole.MIA,
            payload={"i": i},
        ))

    await asyncio.sleep(0.1)
    history = bus.get_history(topic="history.test")
    assert len(history) == 5


@pytest.mark.asyncio
async def test_dead_letter_on_handler_error(bus):
    async def failing_handler(msg: Message):
        raise ValueError("intentional error")

    bus.subscribe("fail.test", AgentRole.CSO, failing_handler)
    await bus.publish(Message(topic="fail.test", sender=AgentRole.MIA))
    await asyncio.sleep(0.1)

    assert len(bus.dead_letters) >= 1
    assert bus.metrics["total_failed"] >= 1


@pytest.mark.asyncio
async def test_unsubscribe(bus):
    received = []

    async def handler(msg: Message):
        received.append(msg)

    bus.subscribe("unsub.test", AgentRole.CSO, handler)
    await bus.publish(Message(topic="unsub.test", sender=AgentRole.MIA))
    await asyncio.sleep(0.1)
    assert len(received) == 1

    bus.unsubscribe("unsub.test", AgentRole.CSO)
    await bus.publish(Message(topic="unsub.test", sender=AgentRole.MIA))
    await asyncio.sleep(0.1)
    assert len(received) == 1  # No new messages
