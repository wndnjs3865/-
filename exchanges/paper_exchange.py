"""QuantPulse v3 - Paper Exchange (Simulated).

In-memory exchange simulator for paper trading.
Implements the same BaseExchange interface as live connectors.
Supports price injection for testing and backtesting.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from .base_exchange import BaseExchange, ExchangeOrder, OHLCV, OrderBook, Ticker


class PaperExchange(BaseExchange):
    """
    In-memory paper trading exchange.

    - Simulates fills with configurable slippage
    - Tracks balances, positions, and order history
    - Prices injected externally (by MIA or test harness)
    """

    def __init__(
        self,
        initial_balance: float = 10000.0,
        slippage_bps: float = 2.0,
        commission_rate: float = 0.0004,
    ) -> None:
        super().__init__(name="paper")
        self._balance: float = initial_balance
        self._initial_balance = initial_balance
        self._slippage_bps = slippage_bps
        self._commission_rate = commission_rate

        # Injected prices: symbol → price
        self._prices: dict[str, float] = {}

        # State
        self._positions: dict[str, dict[str, Any]] = {}  # symbol → position
        self._orders: list[ExchangeOrder] = []
        self._trade_count: int = 0

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    # ── Price Injection ────────────────────────

    def set_price(self, symbol: str, price: float) -> None:
        """Inject a price (called by MIA or test harness)."""
        self._prices[symbol] = price

    def get_price(self, symbol: str) -> float:
        return self._prices.get(symbol, 0.0)

    # ── Exchange Interface ─────────────────────

    async def fetch_ticker(self, symbol: str) -> Ticker:
        price = self._prices.get(symbol, 0.0)
        spread = price * 0.0001  # 1 bps spread
        return Ticker(
            symbol=symbol,
            last=price,
            bid=price - spread,
            ask=price + spread,
            high_24h=price * 1.02,
            low_24h=price * 0.98,
            volume_24h=1000.0,
            timestamp=time.time(),
        )

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str = "1h", limit: int = 100
    ) -> list[OHLCV]:
        # Paper exchange returns empty — real data comes from MIA/external feed
        return []

    async def create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        amount: float,
        price: float | None = None,
    ) -> ExchangeOrder:
        """Simulate order execution with slippage."""
        # Stop orders are stored but not executed immediately in paper mode
        if order_type in ("stop_market", "stop_limit"):
            return ExchangeOrder(
                order_id=uuid.uuid4().hex[:12], symbol=symbol, side=side,
                order_type=order_type, price=price or 0, amount=amount,
                filled=0, status="open", timestamp=time.time(),
            )

        current = self._prices.get(symbol, 0.0)
        if current <= 0 and price:
            current = price
        elif current <= 0:
            raise ValueError(f"No price available for {symbol}")

        # Apply slippage
        slip = current * (self._slippage_bps / 10000)
        fill_price = current + slip if side == "buy" else current - slip

        # Commission
        notional = fill_price * amount
        commission = notional * self._commission_rate

        # Update balance
        if side == "buy":
            self._balance -= commission
            self._positions[symbol] = {
                "symbol": symbol,
                "side": "long",
                "amount": amount,
                "entry_price": fill_price,
                "notional": notional,
            }
        else:
            self._balance -= commission
            # Close position or open short
            if symbol in self._positions:
                pos = self._positions.pop(symbol)
                pnl = (fill_price - pos["entry_price"]) * pos["amount"]
                if pos["side"] == "short":
                    pnl = (pos["entry_price"] - fill_price) * pos["amount"]
                self._balance += pnl
            else:
                self._positions[symbol] = {
                    "symbol": symbol,
                    "side": "short",
                    "amount": amount,
                    "entry_price": fill_price,
                    "notional": notional,
                }

        self._trade_count += 1
        order = ExchangeOrder(
            order_id=uuid.uuid4().hex[:12],
            symbol=symbol,
            side=side,
            order_type=order_type,
            price=fill_price,
            amount=amount,
            filled=amount,
            status="closed",
            timestamp=time.time(),
        )
        self._orders.append(order)
        return order

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        return False  # Paper orders fill instantly

    async def fetch_balance(self) -> dict[str, float]:
        unrealized = 0.0
        for pos in self._positions.values():
            current = self._prices.get(pos["symbol"], pos["entry_price"])
            if pos["side"] == "long":
                unrealized += (current - pos["entry_price"]) * pos["amount"]
            else:
                unrealized += (pos["entry_price"] - current) * pos["amount"]
        return {
            "total": round(self._balance + unrealized, 4),
            "free": round(self._balance, 4),
            "used": round(unrealized, 4),
            "initial": self._initial_balance,
            "pnl": round(self._balance + unrealized - self._initial_balance, 4),
        }

    async def fetch_positions(self) -> list[dict[str, Any]]:
        return list(self._positions.values())

    @property
    def trade_count(self) -> int:
        return self._trade_count
