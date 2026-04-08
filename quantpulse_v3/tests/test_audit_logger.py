"""Tests for QuantPulse v3 AuditLogger."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole
from core.models import AuditEntry
from core.audit_logger import AuditLogger


@pytest.fixture
async def audit():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_audit.db")
        a = AuditLogger(db_path=db_path)
        await a.initialize()
        yield a
        await a.close()


@pytest.mark.asyncio
async def test_log_and_query(audit):
    await audit.log(AuditEntry(
        agent=AgentRole.CRCO,
        action="risk_assessment",
        detail={"signal_id": "sig001", "approved": False, "veto": "daily_loss"},
    ))
    await audit.log(AuditEntry(
        agent=AgentRole.CSO,
        action="trade_decision",
        detail={"signal_id": "sig001", "approved": False},
    ))

    # Query all
    results = await audit.query(limit=10)
    assert len(results) == 2

    # Query by agent
    crco_logs = await audit.query(agent=AgentRole.CRCO)
    assert len(crco_logs) == 1
    assert crco_logs[0]["action"] == "risk_assessment"

    # Query by action
    decisions = await audit.query(action="trade_decision")
    assert len(decisions) == 1


@pytest.mark.asyncio
async def test_count(audit):
    for i in range(5):
        await audit.log(AuditEntry(
            agent=AgentRole.MIA,
            action="signal_generated",
            detail={"i": i},
        ))

    total = await audit.count()
    assert total == 5

    mia_count = await audit.count(agent=AgentRole.MIA)
    assert mia_count == 5

    cso_count = await audit.count(agent=AgentRole.CSO)
    assert cso_count == 0


@pytest.mark.asyncio
async def test_entry_count_property(audit):
    assert audit.entry_count == 0
    await audit.log(AuditEntry(agent=AgentRole.ES, action="order_filled", detail={}))
    assert audit.entry_count == 1


@pytest.mark.asyncio
async def test_correlation_id(audit):
    await audit.log(AuditEntry(
        agent=AgentRole.CSO,
        action="request_risk",
        detail={"signal_id": "sig002"},
        correlation_id="corr_abc123",
    ))

    results = await audit.query(agent=AgentRole.CSO)
    assert results[0]["correlation_id"] == "corr_abc123"


@pytest.mark.asyncio
async def test_rotation():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "rotation_test.db")
        audit = AuditLogger(db_path=db_path, max_entries=10)
        await audit.initialize()

        # Insert 15 entries (exceeds max of 10)
        # Rotation triggers at entry 11 (>10), keeps 5, then entries 12-15 add 4 more = 9
        for i in range(15):
            await audit.log(AuditEntry(
                agent=AgentRole.MIA,
                action=f"action_{i}",
                detail={"i": i},
            ))

        # After rotation at 11, keeps 5, plus 4 more inserts = 9
        assert audit.entry_count == 9
        await audit.close()
