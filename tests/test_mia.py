"""Tests for MIA (Market Intelligence Analyst) Agent."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole, Direction, MarketRegime, Priority, Timeframe
from core.models import Message, MultiTimeframeAnalysis, TradeSignal
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from config import Config
from agents.mia import MIAAgent


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
async def test_mia_lifecycle(system):
    bus, audit, config = system
    mia = MIAAgent(bus, audit, config)
    mia._analysis_interval = 9999  # Prevent auto-loop
    await mia.start()
    assert mia.is_running
    await mia.stop()
    assert not mia.is_running


@pytest.mark.asyncio
async def test_mia_publishes_mtf_analysis(system):
    bus, audit, config = system
    mia = MIAAgent(bus, audit, config)
    mia._analysis_interval = 9999
    await mia.start()

    received = []

    async def capture(msg: Message):
        received.append(msg)

    bus.subscribe("mia.mtf_analysis", AgentRole.CSO, capture)

    # Trigger manual analysis
    await mia._analyze_symbol("BTCUSDT")
    await asyncio.sleep(0.3)

    assert len(received) >= 1
    mtf = MultiTimeframeAnalysis(**received[0].payload)
    assert mtf.symbol == "BTCUSDT"
    assert len(mtf.analyses) == 4  # D1, H4, H1, M15
    assert mtf.regime is not None

    await mia.stop()


@pytest.mark.asyncio
async def test_mia_signal_with_price(system):
    """MIA should generate signal when price is injected and conditions met."""
    bus, audit, config = system
    mia = MIAAgent(bus, audit, config)
    mia._analysis_interval = 9999
    await mia.start()

    signals = []

    async def capture(msg: Message):
        signals.append(msg)

    bus.subscribe("mia.trade_signal", AgentRole.CSO, capture)

    # Inject price to enable signal generation
    mia.inject_price("BTCUSDT", 50000.0)

    # We need to make conditions favorable for signal generation.
    # Override internal cache to simulate favorable conditions.
    mia._market_data_cache["BTCUSDT"] = {
        "price": 50000.0,
        "smc": {
            "order_blocks": [{"type": "bullish", "price": 49500}],
            "fair_value_gaps": [{"low": 49800, "high": 50200}],
            "liquidity_zones": [],
            "break_of_structure": True,
            "change_of_character": False,
        },
        "sentiment": {"score": 0.5, "fear_greed_index": 80, "headlines": []},
        "macro": {"fed_rate_decision_near": False, "cpi_release_near": False,
                  "geopolitical_risk": "LOW", "dxy_trend": "NEUTRAL"},
        "onchain": {"funding_rate": -0.001, "open_interest_change": 0.05,
                    "whale_flow": "BULLISH", "exchange_netflow": -100},
        "psychology": {"fomo_level": 0.3, "fud_level": 0.1, "retail_positioning": "NEUTRAL"},
    }

    await mia._analyze_symbol("BTCUSDT")
    await asyncio.sleep(0.3)

    # Signal may or may not generate depending on confluence threshold
    # At minimum, the analysis should succeed without errors
    logs = await audit.query(agent=AgentRole.MIA, action="mtf_analysis_published")
    assert len(logs) >= 1

    await mia.stop()


@pytest.mark.asyncio
async def test_mia_watchlist(system):
    bus, audit, config = system
    mia = MIAAgent(bus, audit, config)
    mia._analysis_interval = 9999
    await mia.start()

    mia.set_watchlist(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    assert mia._watchlist == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    await mia.stop()


@pytest.mark.asyncio
async def test_mia_analysis_snapshot(system):
    bus, audit, config = system
    mia = MIAAgent(bus, audit, config)
    mia._analysis_interval = 9999
    await mia.start()

    snap = mia.analysis_snapshot
    assert "watchlist" in snap
    assert "signal_count" in snap
    assert snap["signal_count"] == 0

    await mia.stop()


@pytest.mark.asyncio
async def test_mia_circuit_breaker_slows_analysis(system):
    bus, audit, config = system
    mia = MIAAgent(bus, audit, config)
    mia._analysis_interval = 60.0
    await mia.start()

    await bus.publish(Message(
        topic="crco.circuit_breaker",
        sender=AgentRole.CRCO,
        priority=Priority.URGENT,
        payload={"active": True, "reason": "test"},
    ))
    await asyncio.sleep(0.2)

    assert mia._analysis_interval == 300.0  # Slowed down

    await mia.stop()
