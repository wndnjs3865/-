"""Tests for QuantPulse v3 BaseAgent."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole, Priority
from core.models import Message
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from core.base_agent import BaseAgent


class MockAgent(BaseAgent):
    """Concrete test agent."""

    def __init__(self, role, bus, audit):
        super().__init__(role, bus, audit)
        self.received_messages = []

    async def setup_subscriptions(self):
        self.subscribe_safe("test.input", self.handle_input)

    async def on_start(self):
        pass

    async def handle_input(self, message: Message):
        self.received_messages.append(message)
        await self.audit_log("processed_input", {
            "topic": message.topic,
            "payload": message.payload,
        })


class FailingAgent(BaseAgent):
    """Agent that fails on message handling to test retry."""

    def __init__(self, role, bus, audit):
        super().__init__(role, bus, audit)
        self.attempt_count = 0

    async def setup_subscriptions(self):
        self.subscribe_safe("fail.input", self.handle_fail)

    async def on_start(self):
        pass

    async def handle_fail(self, message: Message):
        raise RuntimeError("Handler intentionally failed")


@pytest.fixture
async def system():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        audit = AuditLogger(db_path=db_path)
        await audit.initialize()
        bus = MessageBus(audit_logger=audit)
        await bus.start()
        yield bus, audit
        await bus.stop()
        await audit.close()


@pytest.mark.asyncio
async def test_agent_lifecycle(system):
    bus, audit = system
    agent = MockAgent(AgentRole.MIA, bus, audit)

    await agent.start()
    assert agent.is_running is True

    await agent.stop()
    assert agent.is_running is False

    # Check audit logs
    logs = await audit.query(agent=AgentRole.MIA)
    actions = [l["action"] for l in logs]
    assert "agent_started" in actions
    assert "agent_stopped" in actions


@pytest.mark.asyncio
async def test_agent_receives_messages(system):
    bus, audit = system
    agent = MockAgent(AgentRole.CSO, bus, audit)
    await agent.start()

    await bus.publish(Message(
        topic="test.input",
        sender=AgentRole.MIA,
        payload={"signal": "buy_btc"},
    ))

    await asyncio.sleep(0.2)
    assert len(agent.received_messages) == 1
    assert agent.received_messages[0].payload["signal"] == "buy_btc"

    # Check audit was written
    logs = await audit.query(agent=AgentRole.CSO, action="processed_input")
    assert len(logs) == 1

    await agent.stop()


@pytest.mark.asyncio
async def test_agent_publish(system):
    bus, audit = system
    sender = MockAgent(AgentRole.MIA, bus, audit)
    receiver = MockAgent(AgentRole.CSO, bus, audit)

    await sender.start()
    await receiver.start()

    # MIA publishes; CSO subscribes to test.input already
    await sender.publish("test.input", {"from": "MIA"}, priority=Priority.HIGH)

    await asyncio.sleep(0.2)
    assert len(receiver.received_messages) == 1

    await sender.stop()
    await receiver.stop()


@pytest.mark.asyncio
async def test_error_handling(system):
    bus, audit = system
    agent = FailingAgent(AgentRole.ES, bus, audit)
    await agent.start()

    await bus.publish(Message(
        topic="fail.input",
        sender=AgentRole.CSO,
        payload={"test": True},
    ))

    await asyncio.sleep(0.2)

    # Error should be logged in audit
    logs = await audit.query(agent=AgentRole.ES, action="message_handler_error")
    assert len(logs) >= 1

    await agent.stop()


@pytest.mark.asyncio
async def test_retry_async(system):
    bus, audit = system
    agent = MockAgent(AgentRole.QR, bus, audit)
    await agent.start()

    call_count = 0

    async def flaky_operation():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("Temporary failure")
        return "success"

    result = await agent.retry_async(flaky_operation, "test_operation")
    assert result == "success"
    assert call_count == 3

    # Check retry audit logs
    retry_logs = await audit.query(agent=AgentRole.QR, action="retry_attempt")
    assert len(retry_logs) == 2  # 2 failures before success

    await agent.stop()


@pytest.mark.asyncio
async def test_retry_exhausted(system):
    bus, audit = system
    agent = MockAgent(AgentRole.QR, bus, audit)
    await agent.start()

    async def always_fails():
        raise ConnectionError("Permanent failure")

    with pytest.raises(ConnectionError):
        await agent.retry_async(always_fails, "doomed_operation", max_retries=2)

    # Check exhausted audit log
    logs = await audit.query(agent=AgentRole.QR, action="retry_exhausted")
    assert len(logs) == 1

    await agent.stop()
