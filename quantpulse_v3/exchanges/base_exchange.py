"""QuantPulse v3 - Base Exchange Interface.

Abstract interface for all exchange connectors.
Compatible with CCXT Pro patterns for easy live integration.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class Ticker:
    """Real-time ticker data."""
    symbol: str
    last: float
    bid: float
    ask: float
    high_24h: float
    low_24h: float
    volume_24h: float
    timestamp: float


@dataclass
class OHLCV:
    """Single candlestick."""
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class OrderBook:
    """Level 2 order book snapshot."""
    symbol: str
    bids: list[list[float]]  # [[price, qty], ...]
    asks: list[list[float]]
    timestamp: float


@dataclass
class ExchangeOrder:
    """Order placed on exchange."""
    order_id: str
    symbol: str
    side: str        # "buy" or "sell"
    order_type: str  # "market", "limit"
    price: float
    amount: float
    filled: float
    status: str      # "open", "closed", "canceled"
    timestamp: float


class BaseExchange(ABC):
    """Abstract base for all exchange connectors."""

    def __init__(self, name: str, api_key: str = "", api_secret: str = "") -> None:
        self.name = name
        self._api_key = api_key
        self._api_secret = api_secret
        self._connected = False

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection."""
        ...

    @abstractmethod
    async def fetch_ticker(self, symbol: str) -> Ticker:
        """Get current ticker for symbol."""
        ...

    @abstractmethod
    async def fetch_ohlcv(
        self, symbol: str, timeframe: str = "1h", limit: int = 100
    ) -> list[OHLCV]:
        """Get historical candlesticks."""
        ...

    @abstractmethod
    async def create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        amount: float,
        price: float | None = None,
    ) -> ExchangeOrder:
        """Place an order."""
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel an order."""
        ...

    @abstractmethod
    async def fetch_balance(self) -> dict[str, float]:
        """Get account balances."""
        ...

    @abstractmethod
    async def fetch_positions(self) -> list[dict[str, Any]]:
        """Get open positions (futures)."""
        ...

    @property
    def is_connected(self) -> bool:
        return self._connected
