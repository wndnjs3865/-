"""QuantPulse v3 - Real-time Price Feed Service.

Fetches prices from exchange and distributes to MIA + ES.
Supports both polling (CCXT REST) and placeholder for WebSocket upgrade.

쉬운 설명:
    "거래소에서 실시간 BTC 가격을 가져와서
     MIA(분석가)와 ES(주문 실행가)에게 전달하는 배달부"
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

logger = logging.getLogger("quantpulse.price_feed")


class PriceFeed:
    """
    Real-time price feed that polls exchange tickers
    and pushes prices to MIA and ES.

    Usage:
        feed = PriceFeed(exchange, symbols=["BTC/USDT:USDT"])
        feed.set_consumers(mia=mia_agent, es=es_agent)
        await feed.start()
    """

    def __init__(
        self,
        exchange: Any,
        symbols: list[str] | None = None,
        interval: float = 5.0,  # Poll every 5 seconds
    ) -> None:
        self._exchange = exchange
        self._symbols = symbols or ["BTC/USDT:USDT"]
        self._interval = interval
        self._running = False
        self._task: asyncio.Task[None] | None = None

        # Consumers that receive price updates
        self._mia: Any | None = None
        self._es: Any | None = None

        # Latest prices cache
        self._prices: dict[str, float] = {}
        self._update_count: int = 0

    def set_consumers(self, mia: Any = None, es: Any = None) -> None:
        """Set the agents that will receive price updates."""
        self._mia = mia
        self._es = es

    async def start(self) -> None:
        """Start the price feed polling loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop(), name="price_feed")
        logger.info(f"[FEED] Price feed started for {self._symbols} (interval={self._interval}s)")

    async def stop(self) -> None:
        """Stop the price feed."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[FEED] Price feed stopped")

    async def _poll_loop(self) -> None:
        """Main polling loop: fetch prices and distribute."""
        while self._running:
            for symbol in self._symbols:
                try:
                    await self._fetch_and_distribute(symbol)
                except Exception as e:
                    logger.error(f"[FEED] Error fetching {symbol}: {e}")
            await asyncio.sleep(self._interval)

    async def _fetch_and_distribute(self, symbol: str) -> None:
        """Fetch one ticker and push to consumers (with timeout)."""
        ticker = await asyncio.wait_for(
            self._exchange.fetch_ticker(symbol), timeout=10.0,
        )
        price = ticker.last

        if price <= 0:
            return

        self._prices[symbol] = price
        self._update_count += 1

        # Convert CCXT symbol format (BTC/USDT:USDT) to internal format (BTCUSDT)
        internal_symbol = symbol.replace("/", "").replace(":USDT", "")

        # Push to MIA for analysis
        if self._mia:
            self._mia.inject_price(internal_symbol, price)

        # Push to ES for SL/TP monitoring
        if self._es:
            self._es.inject_price(internal_symbol, price)

    @property
    def prices(self) -> dict[str, float]:
        return dict(self._prices)

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def feed_snapshot(self) -> dict[str, Any]:
        return {
            "running": self._running,
            "symbols": self._symbols,
            "interval": self._interval,
            "update_count": self._update_count,
            "latest_prices": self._prices,
        }
