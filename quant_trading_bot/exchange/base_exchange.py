"""
Base Exchange Interface
=======================
거래소 API 추상 인터페이스.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


@dataclass
class Order:
    """Trade order representation."""

    symbol: str
    side: OrderSide
    quantity: float
    order_type: OrderType = OrderType.MARKET
    price: Optional[float] = None
    stop_price: Optional[float] = None
    order_id: Optional[str] = None
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: float = 0.0
    filled_price: float = 0.0
    commission: float = 0.0
    strategy: str = "manual"
    notes: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = None

    @property
    def is_complete(self) -> bool:
        return self.status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED)

    @property
    def total_value(self) -> float:
        return self.filled_quantity * self.filled_price


@dataclass
class Position:
    """Open position representation."""

    symbol: str
    quantity: float
    avg_price: float
    current_price: float = 0.0
    side: OrderSide = OrderSide.BUY
    strategy: str = "manual"
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def market_value(self) -> float:
        return self.quantity * self.current_price

    @property
    def cost_basis(self) -> float:
        return self.quantity * self.avg_price

    @property
    def unrealized_pnl(self) -> float:
        return self.market_value - self.cost_basis

    @property
    def unrealized_pnl_pct(self) -> float:
        if self.cost_basis == 0:
            return 0.0
        return self.unrealized_pnl / self.cost_basis * 100


class BaseExchange(ABC):
    """Abstract base class for exchange integrations."""

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the exchange."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the exchange."""
        ...

    @abstractmethod
    async def get_balance(self) -> dict[str, float]:
        """Get account balance."""
        ...

    @abstractmethod
    async def get_positions(self) -> list[Position]:
        """Get all open positions."""
        ...

    @abstractmethod
    async def submit_order(self, order: Order) -> Order:
        """Submit an order to the exchange."""
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order."""
        ...

    @abstractmethod
    async def get_order_status(self, order_id: str) -> Order:
        """Get order status."""
        ...

    @abstractmethod
    async def get_current_price(self, symbol: str) -> dict:
        """Get current price data."""
        ...

    @abstractmethod
    async def get_daily_price(self, symbol: str, limit: int) -> list[dict]:
        """Get daily price history."""
        ...
