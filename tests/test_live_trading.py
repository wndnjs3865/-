"""Tests for Live Trading components: exchange connector, price feed, ES live execution."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from core.enums import AgentRole, Direction, OrderType, Priority, TradeMode, Timeframe, MarketRegime
from core.models import Message, TradeDecision, TradeSignal
from config import Config
from exchanges.paper_exchange import PaperExchange
from core.price_feed import PriceFeed
from main import JWQuantSystem


# ══════════════════════════════════════════════
# Price Feed Tests
# ══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_price_feed_distributes_to_consumers():
    """Price feed should push prices to MIA and ES."""
    exchange = PaperExchange()
    await exchange.connect()
    exchange.set_price("BTC/USDT:USDT", 50000.0)

    # Create mock consumers with inject_price methods
    class MockConsumer:
        def __init__(self):
            self.prices = {}
        def inject_price(self, symbol, price):
            self.prices[symbol] = price

    mia = MockConsumer()
    es = MockConsumer()

    feed = PriceFeed(exchange, symbols=["BTC/USDT:USDT"], interval=0.1)
    feed.set_consumers(mia=mia, es=es)
    await feed.start()
    await asyncio.sleep(0.3)
    await feed.stop()

    # Both consumers should have received the price
    assert "BTCUSDT" in mia.prices
    assert mia.prices["BTCUSDT"] == 50000.0
    assert "BTCUSDT" in es.prices
    assert es.prices["BTCUSDT"] == 50000.0

    await exchange.disconnect()


@pytest.mark.asyncio
async def test_price_feed_snapshot():
    exchange = PaperExchange()
    await exchange.connect()
    exchange.set_price("BTC/USDT:USDT", 48000.0)

    feed = PriceFeed(exchange, symbols=["BTC/USDT:USDT"], interval=0.1)
    await feed.start()
    await asyncio.sleep(0.3)

    snap = feed.feed_snapshot
    assert snap["running"] is True
    assert snap["update_count"] >= 1
    assert "BTC/USDT:USDT" in snap["latest_prices"]

    await feed.stop()
    await exchange.disconnect()


# ══════════════════════════════════════════════
# ES Live Execution with PaperExchange (as mock)
# ══════════════════════════════════════════════

@pytest.fixture
async def live_system():
    """System with PaperExchange wired as the 'live' exchange."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Config.from_env()
        config.system.audit_db_path = os.path.join(tmpdir, "live_test.db")
        config.trading.mode = "PAPER"
        config.trading.min_quant_score = 50.0
        sys = JWQuantSystem(config)
        await sys.start()
        sys.mia._analysis_interval = 99999
        sys.po._report_interval = 99999

        # Wire PaperExchange as the "live" exchange
        exchange = PaperExchange(initial_balance=10000.0)
        await exchange.connect()
        exchange.set_price("BTCUSDT", 50000.0)
        sys.es.set_exchange(exchange)
        sys._exchange = exchange

        yield sys, exchange

        await exchange.disconnect()
        await sys.stop()


@pytest.mark.asyncio
async def test_es_live_execution_with_exchange(live_system):
    """ES should execute live orders when exchange is connected."""
    system, exchange = live_system

    # Switch to LIVE mode (set config directly for test)
    system.config.trading.mode = "LIVE"

    filled_events = []

    async def capture(msg: Message):
        filled_events.append(msg)

    system.bus.subscribe("es.order_filled", AgentRole.PO, capture)

    # Send a signal through the pipeline
    signal = TradeSignal(
        id="sig_live_exec",
        symbol="BTCUSDT", direction=Direction.LONG, score=85.0,
        entry_price=50000.0, stop_loss=49000.0, take_profit_1=53000.0,
        timeframe=Timeframe.H4, regime=MarketRegime.TRENDING_BULL,
        risk_reward_ratio=3.0, confluence_factors=["MTF_ALIGNMENT"],
    )

    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload=signal.model_dump(mode="json"),
    ))

    await asyncio.sleep(2.0)

    # ES should have executed via the exchange
    assert len(filled_events) >= 1
    assert len(system.es.open_trades) >= 1

    # Exchange should have a position
    positions = await exchange.fetch_positions()
    assert len(positions) >= 1

    # Audit should record live execution
    logs = await system.audit.query(agent=AgentRole.ES, action="live_order_filled")
    assert len(logs) >= 1

    system.config.trading.mode = "PAPER"


@pytest.mark.asyncio
async def test_es_live_fails_without_exchange(live_system):
    """ES should fail gracefully if exchange is disconnected."""
    system, exchange = live_system

    system.config.trading.mode = "LIVE"
    system.es.set_exchange(None)  # Remove exchange

    failed_events = []

    async def capture(msg: Message):
        failed_events.append(msg)

    system.bus.subscribe("es.order_failed", AgentRole.PO, capture)

    signal = TradeSignal(
        id="sig_no_exchange",
        symbol="BTCUSDT", direction=Direction.LONG, score=85.0,
        entry_price=50000.0, stop_loss=49000.0, take_profit_1=53000.0,
        timeframe=Timeframe.H4, regime=MarketRegime.TRENDING_BULL,
        risk_reward_ratio=3.0, confluence_factors=["MTF_ALIGNMENT"],
    )

    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload=signal.model_dump(mode="json"),
    ))

    await asyncio.sleep(2.0)

    assert len(failed_events) >= 1
    assert len(system.es.open_trades) == 0

    system.config.trading.mode = "PAPER"


@pytest.mark.asyncio
async def test_price_feed_triggers_sl_tp(live_system):
    """Price feed should trigger SL/TP closures via ES position monitor."""
    system, exchange = live_system

    # Execute a trade in paper mode first
    signal = TradeSignal(
        id="sig_feed_sl",
        symbol="BTCUSDT", direction=Direction.LONG, score=85.0,
        entry_price=50000.0, stop_loss=49000.0, take_profit_1=53000.0,
        timeframe=Timeframe.H4, regime=MarketRegime.TRENDING_BULL,
        risk_reward_ratio=3.0, confluence_factors=["MTF_ALIGNMENT"],
    )

    await system.bus.publish(Message(
        topic="mia.trade_signal",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload=signal.model_dump(mode="json"),
    ))
    await asyncio.sleep(1.5)

    assert len(system.es.open_trades) >= 1

    closed_events = []

    async def capture(msg: Message):
        closed_events.append(msg)

    system.bus.subscribe("es.order_closed", AgentRole.IMA, capture)

    # Inject price below SL → should trigger close
    system.es.inject_price("BTCUSDT", 48900.0)
    await asyncio.sleep(2.0)

    assert len(closed_events) >= 1
    assert closed_events[-1].payload["close_reason"] == "sl_hit"
