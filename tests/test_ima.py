"""Tests for IMA (Infrastructure & Monitoring Agent)."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole, AlertSeverity, Priority
from core.models import AlertEvent, Message
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from core.base_agent import BaseAgent
from config import Config
from agents.ima import IMAAgent


class DummyAgent(BaseAgent):
    """Minimal agent for health monitoring tests."""
    async def setup_subscriptions(self):
        pass
    async def on_start(self):
        pass


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


@pytest.mark.asyncio
async def test_ima_receives_alerts(system):
    bus, audit, config = system
    ima = IMAAgent(bus, audit, config)
    await ima.start()

    await bus.publish(Message(
        topic="ima.alert",
        sender=AgentRole.CRCO,
        payload=AlertEvent(
            severity=AlertSeverity.CRITICAL,
            source=AgentRole.CRCO,
            title="Circuit breaker activated",
            detail="Daily loss exceeded 5%",
        ).model_dump(mode="json"),
    ))

    await asyncio.sleep(0.3)
    assert len(ima._alerts) == 1
    assert ima._alerts[0].severity == AlertSeverity.CRITICAL
    assert len(ima._unacknowledged_alerts) == 1

    # Telegram should be queued for CRITICAL
    assert len(ima._telegram_queue) >= 1

    await ima.stop()


@pytest.mark.asyncio
async def test_ima_acknowledge_alert(system):
    bus, audit, config = system
    ima = IMAAgent(bus, audit, config)
    await ima.start()

    await bus.publish(Message(
        topic="ima.alert",
        sender=AgentRole.ES,
        payload=AlertEvent(
            severity=AlertSeverity.WARNING,
            source=AgentRole.ES,
            title="Order retry failed",
        ).model_dump(mode="json"),
    ))
    await asyncio.sleep(0.3)

    alert_id = ima._alerts[0].alert_id
    assert ima.acknowledge_alert(alert_id) is True
    assert len(ima._unacknowledged_alerts) == 0

    await ima.stop()


@pytest.mark.asyncio
async def test_ima_acknowledge_all(system):
    bus, audit, config = system
    ima = IMAAgent(bus, audit, config)
    await ima.start()

    for i in range(3):
        await bus.publish(Message(
            topic="ima.alert",
            sender=AgentRole.MIA,
            payload=AlertEvent(
                severity=AlertSeverity.INFO,
                source=AgentRole.MIA,
                title=f"Info alert {i}",
            ).model_dump(mode="json"),
        ))
    await asyncio.sleep(0.5)

    assert len(ima._unacknowledged_alerts) == 3
    count = ima.acknowledge_all()
    assert count == 3
    assert len(ima._unacknowledged_alerts) == 0

    await ima.stop()


@pytest.mark.asyncio
async def test_ima_health_collection(system):
    bus, audit, config = system

    # Create a dummy agent to monitor
    dummy = DummyAgent(AgentRole.MIA, bus, audit)
    await dummy.start()

    ima = IMAAgent(bus, audit, config, agents=[dummy])
    await ima.start()

    # Manually trigger health collection
    await ima._collect_health()

    assert AgentRole.MIA.value in ima._latest_health
    health = ima._latest_health[AgentRole.MIA.value]
    assert health.alive is True
    assert health.agent == AgentRole.MIA

    await ima.stop()
    await dummy.stop()


@pytest.mark.asyncio
async def test_ima_register_agent(system):
    bus, audit, config = system
    ima = IMAAgent(bus, audit, config)
    await ima.start()

    dummy = DummyAgent(AgentRole.QR, bus, audit)
    await dummy.start()

    ima.register_agent(dummy)
    assert len(ima._monitored_agents) == 1

    await ima._collect_health()
    assert AgentRole.QR.value in ima._latest_health

    await ima.stop()
    await dummy.stop()


@pytest.mark.asyncio
async def test_ima_snapshot(system):
    bus, audit, config = system
    ima = IMAAgent(bus, audit, config)
    await ima.start()

    snap = ima.ima_snapshot
    assert snap["total_alerts"] == 0
    assert snap["unacknowledged_alerts"] == 0
    assert snap["monitored_agents"] == 0

    await ima.stop()


@pytest.mark.asyncio
async def test_ima_get_alerts_by_severity(system):
    bus, audit, config = system
    ima = IMAAgent(bus, audit, config)
    await ima.start()

    for sev in [AlertSeverity.INFO, AlertSeverity.WARNING, AlertSeverity.CRITICAL]:
        await bus.publish(Message(
            topic="ima.alert",
            sender=AgentRole.MIA,
            payload=AlertEvent(
                severity=sev,
                source=AgentRole.MIA,
                title=f"{sev.value} alert",
            ).model_dump(mode="json"),
        ))
    await asyncio.sleep(0.5)

    critical = ima.get_alerts(severity=AlertSeverity.CRITICAL)
    assert len(critical) == 1

    all_alerts = ima.get_alerts()
    assert len(all_alerts) == 3

    await ima.stop()
