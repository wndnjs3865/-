"""Tests for PO (Performance Optimizer) Agent."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole, Direction, OrderStatus, Priority
from core.models import Message, PerformanceReport
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from config import Config
from agents.po import POAgent


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
async def test_po_tracks_filled_orders(system):
    bus, audit, config = system
    po = POAgent(bus, audit, config)
    po._report_interval = 9999
    await po.start()

    await bus.publish(Message(
        topic="es.order_filled",
        sender=AgentRole.ES,
        payload={
            "order_id": "ord_001",
            "signal_id": "sig_001",
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "filled_price": 50000.0,
            "filled_quantity": 0.1,
            "commission": 2.0,
        },
    ))
    await asyncio.sleep(0.3)

    assert len(po._open_trades) == 1
    assert "ord_001" in po._open_trades

    await po.stop()


@pytest.mark.asyncio
async def test_po_journals_closed_trades(system):
    bus, audit, config = system
    po = POAgent(bus, audit, config)
    po._report_interval = 9999
    await po.start()

    # Simulate close with profit
    await bus.publish(Message(
        topic="es.order_closed",
        sender=AgentRole.ES,
        payload={
            "order_id": "ord_001",
            "signal_id": "sig_001",
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "entry_price": 50000.0,
            "exit_price": 52000.0,
            "quantity": 0.1,
            "pnl": 195.0,
            "pnl_percent": 3.9,
            "commission": 2.0,
            "close_reason": "tp1_hit",
            "duration_seconds": 3600,
        },
    ))
    await asyncio.sleep(0.3)

    assert len(po._journal) == 1
    assert po._total_pnl == 195.0
    assert len(po._winning_pnls) == 1

    await po.stop()


@pytest.mark.asyncio
async def test_po_win_loss_tracking(system):
    bus, audit, config = system
    po = POAgent(bus, audit, config)
    po._report_interval = 9999
    await po.start()

    # 2 wins, 1 loss
    for pnl in [100.0, 50.0, -80.0]:
        await bus.publish(Message(
            topic="es.order_closed",
            sender=AgentRole.ES,
            payload={"pnl": pnl, "pnl_percent": pnl / 50, "symbol": "BTCUSDT",
                     "direction": "LONG", "duration_seconds": 300},
        ))
    await asyncio.sleep(0.5)

    assert len(po._winning_pnls) == 2
    assert len(po._losing_pnls) == 1
    assert po._total_pnl == 70.0
    assert po._max_consecutive_wins == 2
    assert po._max_consecutive_losses == 1

    await po.stop()


@pytest.mark.asyncio
async def test_po_generate_report(system):
    bus, audit, config = system
    po = POAgent(bus, audit, config)
    po._report_interval = 9999
    await po.start()

    # Add some trades
    for pnl, pct in [(100.0, 2.0), (50.0, 1.0), (-30.0, -0.6), (80.0, 1.6)]:
        await bus.publish(Message(
            topic="es.order_closed",
            sender=AgentRole.ES,
            payload={"pnl": pnl, "pnl_percent": pct, "symbol": "BTCUSDT",
                     "direction": "LONG", "duration_seconds": 600},
        ))
    await asyncio.sleep(0.5)

    report = po.generate_report("daily")
    assert report.total_trades == 4
    assert report.winning_trades == 3
    assert report.losing_trades == 1
    assert report.win_rate == 75.0
    assert report.total_pnl == 200.0
    assert report.profit_factor > 1.0
    assert report.max_drawdown >= 0.0

    await po.stop()


@pytest.mark.asyncio
async def test_po_equity_curve(system):
    bus, audit, config = system
    po = POAgent(bus, audit, config)
    po._report_interval = 9999
    await po.start()

    for pnl in [100.0, -50.0, 200.0]:
        await bus.publish(Message(
            topic="es.order_closed",
            sender=AgentRole.ES,
            payload={"pnl": pnl, "pnl_percent": 1.0, "symbol": "BTCUSDT",
                     "direction": "LONG", "duration_seconds": 60},
        ))
    await asyncio.sleep(0.5)

    assert len(po._equity_curve) == 4  # initial + 3 trades
    assert po._current_equity == 10250.0  # 10000 + 100 - 50 + 200
    assert po._peak_equity == 10250.0

    await po.stop()


@pytest.mark.asyncio
async def test_po_snapshot(system):
    bus, audit, config = system
    po = POAgent(bus, audit, config)
    po._report_interval = 9999
    await po.start()

    snap = po.po_snapshot
    assert snap["total_trades"] == 0
    assert snap["total_pnl"] == 0.0
    assert snap["current_equity"] == 10000.0

    await po.stop()
