"""
Market Data Provider
====================
시장 데이터 수집 및 관리. KIS API 및 외부 데이터 소스 통합.
OHLCV, 호가, 체결 데이터 관리.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

import numpy as np
import pandas as pd
from loguru import logger


class Timeframe(str, Enum):
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"
    W1 = "1w"


@dataclass
class OHLCV:
    """Single OHLCV bar."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    symbol: str = ""
    timeframe: str = "1d"

    @property
    def body_pct(self) -> float:
        if self.open == 0:
            return 0.0
        return (self.close - self.open) / self.open * 100

    @property
    def range_pct(self) -> float:
        if self.low == 0:
            return 0.0
        return (self.high - self.low) / self.low * 100

    @property
    def is_bullish(self) -> bool:
        return self.close > self.open


@dataclass
class MarketSnapshot:
    """Real-time market snapshot for a symbol."""
    symbol: str
    price: float
    change_pct: float
    volume: float
    bid: float = 0.0
    ask: float = 0.0
    bid_size: float = 0.0
    ask_size: float = 0.0
    high_52w: float = 0.0
    low_52w: float = 0.0
    market_cap: float = 0.0
    pe_ratio: float = 0.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def spread(self) -> float:
        if self.bid == 0:
            return 0.0
        return (self.ask - self.bid) / self.bid * 100

    @property
    def from_52w_high_pct(self) -> float:
        if self.high_52w == 0:
            return 0.0
        return (self.price - self.high_52w) / self.high_52w * 100


@dataclass
class OrderBook:
    """Level 2 order book data."""
    symbol: str
    bids: list[tuple[float, float]] = field(default_factory=list)  # (price, size)
    asks: list[tuple[float, float]] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def bid_ask_imbalance(self) -> float:
        total_bid = sum(s for _, s in self.bids[:5])
        total_ask = sum(s for _, s in self.asks[:5])
        total = total_bid + total_ask
        if total == 0:
            return 0.0
        return (total_bid - total_ask) / total


class MarketDataProvider:
    """
    Unified market data provider.

    Aggregates data from multiple sources:
    - KIS API (한국투자증권)
    - Yahoo Finance (fallback)
    - WebSocket real-time feeds
    """

    def __init__(self):
        self._cache: dict[str, pd.DataFrame] = {}
        self._snapshots: dict[str, MarketSnapshot] = {}
        self._log = logger.bind(agent_name="MARKET_DATA")

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: Timeframe = Timeframe.D1,
        limit: int = 200,
    ) -> pd.DataFrame:
        """
        Get OHLCV data for a symbol.
        Returns DataFrame with columns: open, high, low, close, volume
        """
        cache_key = f"{symbol}_{timeframe.value}_{limit}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            df = await self._fetch_ohlcv_kis(symbol, timeframe, limit)
            if df is not None and not df.empty:
                self._cache[cache_key] = df
                return df
        except Exception as e:
            self._log.warning("KIS OHLCV fetch failed for {}: {}", symbol, e)

        # Return empty DataFrame as fallback
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    async def get_snapshot(self, symbol: str) -> Optional[MarketSnapshot]:
        """Get real-time market snapshot."""
        if symbol in self._snapshots:
            snap = self._snapshots[symbol]
            age = (datetime.now(timezone.utc) - snap.timestamp).total_seconds()
            if age < 60:  # Cache for 60 seconds
                return snap

        try:
            snapshot = await self._fetch_snapshot_kis(symbol)
            if snapshot:
                self._snapshots[symbol] = snapshot
                return snapshot
        except Exception as e:
            self._log.warning("Snapshot fetch failed for {}: {}", symbol, e)

        return self._snapshots.get(symbol)

    async def get_multiple_snapshots(
        self, symbols: list[str]
    ) -> dict[str, MarketSnapshot]:
        """Get snapshots for multiple symbols."""
        import asyncio
        tasks = [self.get_snapshot(s) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return {
            s: r for s, r in zip(symbols, results)
            if isinstance(r, MarketSnapshot)
        }

    async def _fetch_ohlcv_kis(
        self,
        symbol: str,
        timeframe: Timeframe,
        limit: int,
    ) -> Optional[pd.DataFrame]:
        """Fetch OHLCV from KIS API."""
        from ..exchange.kis_api import KISClient

        client = KISClient()
        data = await client.get_daily_price(symbol, limit)
        if not data:
            return None

        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["stck_bsop_date"], format="%Y%m%d")
        df = df.rename(columns={
            "stck_oprc": "open",
            "stck_hgpr": "high",
            "stck_lwpr": "low",
            "stck_clpr": "close",
            "acml_vol": "volume",
        })

        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.set_index("timestamp").sort_index()
        return df[["open", "high", "low", "close", "volume"]].tail(limit)

    async def _fetch_snapshot_kis(self, symbol: str) -> Optional[MarketSnapshot]:
        """Fetch real-time snapshot from KIS API."""
        from ..exchange.kis_api import KISClient

        client = KISClient()
        data = await client.get_current_price(symbol)
        if not data:
            return None

        return MarketSnapshot(
            symbol=symbol,
            price=float(data.get("stck_prpr", 0)),
            change_pct=float(data.get("prdy_ctrt", 0)),
            volume=float(data.get("acml_vol", 0)),
            high_52w=float(data.get("stck_dryy_hgpr", 0)),
            low_52w=float(data.get("stck_dryy_lwpr", 0)),
        )

    def generate_sample_data(
        self,
        symbol: str = "SAMPLE",
        days: int = 252,
        start_price: float = 100.0,
        volatility: float = 0.02,
    ) -> pd.DataFrame:
        """Generate sample OHLCV data for backtesting."""
        np.random.seed(42)
        dates = pd.date_range(end=datetime.now(), periods=days, freq="B")
        returns = np.random.normal(0.0005, volatility, days)

        prices = [start_price]
        for r in returns[1:]:
            prices.append(prices[-1] * (1 + r))

        df = pd.DataFrame(index=dates)
        df["close"] = prices
        df["open"] = df["close"].shift(1).fillna(start_price)
        df["high"] = df[["open", "close"]].max(axis=1) * (1 + np.abs(np.random.normal(0, 0.005, days)))
        df["low"] = df[["open", "close"]].min(axis=1) * (1 - np.abs(np.random.normal(0, 0.005, days)))
        df["volume"] = np.random.randint(100_000, 10_000_000, days).astype(float)

        return df

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._cache.clear()
        self._snapshots.clear()
        self._log.info("Market data cache cleared")
