"""Tests for PaperExchange."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from exchanges.paper_exchange import PaperExchange


@pytest.fixture
async def exchange():
    ex = PaperExchange(initial_balance=10000.0)
    await ex.connect()
    yield ex
    await ex.disconnect()


@pytest.mark.asyncio
async def test_connect_disconnect(exchange):
    assert exchange.is_connected
    await exchange.disconnect()
    assert not exchange.is_connected


@pytest.mark.asyncio
async def test_set_and_fetch_price(exchange):
    exchange.set_price("BTCUSDT", 50000.0)
    ticker = await exchange.fetch_ticker("BTCUSDT")
    assert ticker.last == 50000.0
    assert ticker.bid < ticker.ask


@pytest.mark.asyncio
async def test_buy_order(exchange):
    exchange.set_price("BTCUSDT", 50000.0)
    order = await exchange.create_order("BTCUSDT", "buy", "market", 0.1)

    assert order.status == "closed"
    assert order.filled == 0.1
    assert order.price > 0

    positions = await exchange.fetch_positions()
    assert len(positions) == 1
    assert positions[0]["side"] == "long"


@pytest.mark.asyncio
async def test_sell_closes_position(exchange):
    exchange.set_price("BTCUSDT", 50000.0)
    await exchange.create_order("BTCUSDT", "buy", "market", 0.1)

    exchange.set_price("BTCUSDT", 52000.0)
    await exchange.create_order("BTCUSDT", "sell", "market", 0.1)

    positions = await exchange.fetch_positions()
    assert len(positions) == 0

    balance = await exchange.fetch_balance()
    assert balance["pnl"] > 0  # Profit after price increase


@pytest.mark.asyncio
async def test_balance_tracking(exchange):
    balance = await exchange.fetch_balance()
    assert balance["total"] == 10000.0
    assert balance["initial"] == 10000.0

    exchange.set_price("BTCUSDT", 50000.0)
    await exchange.create_order("BTCUSDT", "buy", "market", 0.1)
    balance = await exchange.fetch_balance()
    assert balance["free"] < 10000.0  # Commission deducted


@pytest.mark.asyncio
async def test_trade_count(exchange):
    assert exchange.trade_count == 0
    exchange.set_price("BTCUSDT", 50000.0)
    await exchange.create_order("BTCUSDT", "buy", "market", 0.1)
    assert exchange.trade_count == 1
