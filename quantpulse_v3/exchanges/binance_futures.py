"""QuantPulse v3 - Binance Futures Connector (CCXT).

Connects to Binance USDT-M Futures via CCXT.
Handles: ticker, OHLCV, order creation/cancellation, balances, positions.
Falls back gracefully if ccxt is not installed.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .base_exchange import BaseExchange, ExchangeOrder, OHLCV, Ticker

logger = logging.getLogger("quantpulse.exchange.binance")


class BinanceFuturesExchange(BaseExchange):
    """
    Binance USDT-M Futures connector via CCXT.

    쉬운 설명:
    - 바이낸스 거래소에 진짜 주문을 넣을 수 있는 "전화기"
    - API 키가 있어야 작동함
    - ccxt 패키지가 설치되어 있어야 함 (pip install ccxt)
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        testnet: bool = False,
    ) -> None:
        super().__init__(name="binance_futures", api_key=api_key, api_secret=api_secret)
        self._testnet = testnet
        self._exchange: Any = None  # ccxt.binanceusdm instance

    async def connect(self) -> None:
        """거래소에 연결 (CCXT 초기화)."""
        try:
            import ccxt.async_support as ccxt_async
        except ImportError:
            raise RuntimeError(
                "ccxt 패키지가 설치되지 않았습니다. "
                "설치하려면: pip install ccxt"
            )

        options: dict[str, Any] = {
            "defaultType": "future",
            "adjustForTimeDifference": True,
        }

        self._exchange = ccxt_async.binanceusdm({
            "apiKey": self._api_key,
            "secret": self._api_secret,
            "options": options,
            "enableRateLimit": True,
        })

        if self._testnet:
            self._exchange.set_sandbox_mode(True)

        # Verify connection by loading markets
        await self._exchange.load_markets()
        self._connected = True
        logger.info(f"[BINANCE] Connected ({'testnet' if self._testnet else 'mainnet'})")

    async def disconnect(self) -> None:
        """연결 종료."""
        if self._exchange:
            await self._exchange.close()
            self._exchange = None
        self._connected = False
        logger.info("[BINANCE] Disconnected")

    async def fetch_ticker(self, symbol: str) -> Ticker:
        """실시간 가격 조회."""
        self._ensure_connected()
        data = await self._exchange.fetch_ticker(symbol)
        return Ticker(
            symbol=symbol,
            last=float(data.get("last", 0)),
            bid=float(data.get("bid", 0)),
            ask=float(data.get("ask", 0)),
            high_24h=float(data.get("high", 0)),
            low_24h=float(data.get("low", 0)),
            volume_24h=float(data.get("quoteVolume", 0)),
            timestamp=time.time(),
        )

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str = "1h", limit: int = 100
    ) -> list[OHLCV]:
        """과거 캔들 데이터 조회."""
        self._ensure_connected()
        raw = await self._exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        return [
            OHLCV(
                timestamp=candle[0] / 1000,  # ms → seconds
                open=float(candle[1]),
                high=float(candle[2]),
                low=float(candle[3]),
                close=float(candle[4]),
                volume=float(candle[5]),
            )
            for candle in raw
        ]

    async def create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        amount: float,
        price: float | None = None,
    ) -> ExchangeOrder:
        """
        실제 주문 생성.

        side: "buy" 또는 "sell"
        order_type: "market" 또는 "limit"
        amount: 수량 (예: 0.001 BTC)
        price: limit 주문 시 가격 (market 주문이면 None)
        """
        self._ensure_connected()

        params: dict[str, Any] = {}

        if order_type == "market":
            result = await self._exchange.create_order(
                symbol, "market", side, amount, params=params
            )
        elif order_type == "stop_market":
            if price is None:
                raise ValueError("Stop market order requires a stop price")
            params["stopPrice"] = price
            result = await self._exchange.create_order(
                symbol, "STOP_MARKET", side, amount, None, params=params
            )
        elif order_type == "stop_limit":
            if price is None:
                raise ValueError("Stop limit order requires a price")
            params["stopPrice"] = price
            result = await self._exchange.create_order(
                symbol, "STOP", side, amount, price, params=params
            )
        else:
            if price is None:
                raise ValueError("Limit order requires a price")
            result = await self._exchange.create_order(
                symbol, "limit", side, amount, price, params=params
            )

        return ExchangeOrder(
            order_id=str(result.get("id", "")),
            symbol=symbol,
            side=side,
            order_type=order_type,
            price=float(result.get("average", result.get("price", 0)) or 0),
            amount=float(result.get("amount", 0)),
            filled=float(result.get("filled", 0)),
            status=result.get("status", "unknown"),
            timestamp=time.time(),
        )

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """주문 취소."""
        self._ensure_connected()
        try:
            await self._exchange.cancel_order(order_id, symbol)
            return True
        except Exception as e:
            logger.error(f"[BINANCE] Cancel order failed: {e}")
            return False

    async def fetch_balance(self) -> dict[str, float]:
        """계좌 잔고 조회."""
        self._ensure_connected()
        balance = await self._exchange.fetch_balance()
        usdt = balance.get("USDT", {})
        return {
            "total": float(usdt.get("total", 0)),
            "free": float(usdt.get("free", 0)),
            "used": float(usdt.get("used", 0)),
        }

    async def fetch_positions(self) -> list[dict[str, Any]]:
        """열린 포지션 조회."""
        self._ensure_connected()
        positions = await self._exchange.fetch_positions()
        return [
            {
                "symbol": p["symbol"],
                "side": "long" if float(p.get("contracts", 0)) > 0 else "short",
                "amount": abs(float(p.get("contracts", 0))),
                "entry_price": float(p.get("entryPrice", 0)),
                "unrealized_pnl": float(p.get("unrealizedPnl", 0)),
                "leverage": int(p.get("leverage", 1)),
            }
            for p in positions
            if abs(float(p.get("contracts", 0))) > 0
        ]

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        """레버리지 설정."""
        self._ensure_connected()
        try:
            await self._exchange.set_leverage(leverage, symbol)
            logger.info(f"[BINANCE] Set leverage {leverage}x for {symbol}")
        except Exception as e:
            logger.warning(f"[BINANCE] Set leverage failed (may already be set): {e}")

    def _ensure_connected(self) -> None:
        if not self._connected or not self._exchange:
            raise RuntimeError("Exchange not connected. Call connect() first.")
