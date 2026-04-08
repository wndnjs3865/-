"""
KIS (Korea Investment & Securities) API Client
===============================================
한국투자증권 Open API 클라이언트.
국내/해외 주식 시세 조회, 주문 실행, 잔고 조회.

API Docs: https://apiportal.koreainvestment.com/
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
from loguru import logger

from ..config.settings import get_settings
from .base_exchange import (
    BaseExchange,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
)


class KISClient(BaseExchange):
    """
    한국투자증권 API Client.

    Supports:
    - OAuth token management (auto-refresh)
    - Domestic stock trading (국내 주식)
    - Overseas stock trading (해외 주식 - US, JP, CN, etc.)
    - Real-time price streaming via WebSocket
    - Account balance & position queries
    """

    def __init__(self):
        self._settings = get_settings()
        self._base_url = self._settings.kis_base_url
        self._access_token: Optional[str] = None
        self._token_expires: float = 0
        self._session: Optional[aiohttp.ClientSession] = None
        self._log = logger.bind(agent_name="KIS_API")

    async def connect(self) -> None:
        """Initialize connection and get access token."""
        self._session = aiohttp.ClientSession()
        await self._get_access_token()
        self._log.info(
            "KIS API connected | env={} | account={}",
            self._settings.kis_env.value,
            self._settings.kis_account_no,
        )

    async def disconnect(self) -> None:
        """Close connection."""
        if self._session:
            await self._session.close()
            self._session = None
        self._log.info("KIS API disconnected")

    # ========================
    # Authentication
    # ========================

    async def _get_access_token(self) -> str:
        """Get or refresh OAuth access token."""
        if self._access_token and time.time() < self._token_expires:
            return self._access_token

        url = f"{self._base_url}/oauth2/tokenP"
        payload = {
            "grant_type": "client_credentials",
            "appkey": self._settings.kis_app_key,
            "appsecret": self._settings.kis_app_secret,
        }

        async with self._get_session().post(url, json=payload) as resp:
            data = await resp.json()
            if "access_token" in data:
                self._access_token = data["access_token"]
                self._token_expires = time.time() + data.get("expires_in", 86400) - 60
                self._log.info("Access token refreshed")
                return self._access_token
            else:
                raise ConnectionError(f"Token auth failed: {data}")

    def _get_session(self) -> aiohttp.ClientSession:
        if not self._session:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _get_headers(self, tr_id: str, hash_body: str = "") -> dict:
        """Build API request headers."""
        token = await self._get_access_token()
        account_parts = self._settings.kis_account_no.split("-")
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self._settings.kis_app_key,
            "appsecret": self._settings.kis_app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }
        if hash_body:
            headers["hashkey"] = await self._get_hashkey(hash_body)
        return headers

    async def _get_hashkey(self, body: str) -> str:
        """Get hashkey for POST requests."""
        url = f"{self._base_url}/uapi/hashkey"
        headers = {
            "content-type": "application/json; charset=utf-8",
            "appkey": self._settings.kis_app_key,
            "appsecret": self._settings.kis_app_secret,
        }
        async with self._get_session().post(
            url, headers=headers, data=body
        ) as resp:
            data = await resp.json()
            return data.get("HASH", "")

    # ========================
    # Market Data - Domestic
    # ========================

    async def get_current_price(self, symbol: str) -> dict:
        """
        국내 주식 현재가 조회.
        tr_id: FHKST01010100
        """
        url = f"{self._base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        headers = await self._get_headers("FHKST01010100")

        async with self._get_session().get(url, headers=headers, params=params) as resp:
            data = await resp.json()
            return data.get("output", {})

    async def get_daily_price(self, symbol: str, limit: int = 100) -> list[dict]:
        """
        국내 주식 일별 시세 조회.
        tr_id: FHKST01010400
        """
        url = f"{self._base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        }
        headers = await self._get_headers("FHKST01010400")

        async with self._get_session().get(url, headers=headers, params=params) as resp:
            data = await resp.json()
            return data.get("output", [])[:limit]

    # ========================
    # Market Data - Overseas (US)
    # ========================

    async def get_us_current_price(self, symbol: str, exchange: str = "NAS") -> dict:
        """
        해외 주식 현재가 조회.
        tr_id: HHDFS00000300
        exchange: NAS(나스닥), NYS(뉴욕), AMS(아멕스)
        """
        url = f"{self._base_url}/uapi/overseas-price/v1/quotations/price"
        params = {
            "AUTH": "",
            "EXCD": exchange,
            "SYMB": symbol,
        }
        headers = await self._get_headers("HHDFS00000300")

        async with self._get_session().get(url, headers=headers, params=params) as resp:
            data = await resp.json()
            return data.get("output", {})

    async def get_us_daily_price(
        self,
        symbol: str,
        exchange: str = "NAS",
        period: str = "D",
        limit: int = 100,
    ) -> list[dict]:
        """
        해외 주식 일별 시세 조회.
        tr_id: HHDFS76240000
        """
        url = f"{self._base_url}/uapi/overseas-price/v1/quotations/dailyprice"
        params = {
            "AUTH": "",
            "EXCD": exchange,
            "SYMB": symbol,
            "GUBN": "0",
            "BYMD": "",
            "MODP": "1",
        }
        headers = await self._get_headers("HHDFS76240000")

        async with self._get_session().get(url, headers=headers, params=params) as resp:
            data = await resp.json()
            return data.get("output2", [])[:limit]

    # ========================
    # Order Execution - Domestic
    # ========================

    async def submit_order(self, order: Order) -> Order:
        """
        국내 주식 주문.
        매수: TTTC0802U, 매도: TTTC0801U (실전)
        매수: VTTC0802U, 매도: VTTC0801U (모의)
        """
        is_virtual = self._settings.kis_env.value == "VIRTUAL"

        if order.side == OrderSide.BUY:
            tr_id = "VTTC0802U" if is_virtual else "TTTC0802U"
        else:
            tr_id = "VTTC0801U" if is_virtual else "TTTC0801U"

        account_parts = self._settings.kis_account_no.split("-")
        body = {
            "CANO": account_parts[0],
            "ACNT_PRDT_CD": account_parts[1] if len(account_parts) > 1 else "01",
            "PDNO": order.symbol,
            "ORD_DVSN": "01" if order.order_type == OrderType.MARKET else "00",
            "ORD_QTY": str(int(order.quantity)),
            "ORD_UNPR": str(int(order.price)) if order.price else "0",
        }

        body_str = json.dumps(body)
        headers = await self._get_headers(tr_id, body_str)
        url = f"{self._base_url}/uapi/domestic-stock/v1/trading/order-cash"

        self._log.info(
            "Submitting order: {} {} {} @ {}",
            order.side.value,
            order.quantity,
            order.symbol,
            order.price or "MARKET",
        )

        async with self._get_session().post(
            url, headers=headers, data=body_str
        ) as resp:
            data = await resp.json()

            if data.get("rt_cd") == "0":
                output = data.get("output", {})
                order.order_id = output.get("ODNO", "")
                order.status = OrderStatus.SUBMITTED
                self._log.info("Order submitted: {}", order.order_id)
            else:
                order.status = OrderStatus.REJECTED
                self._log.error("Order rejected: {}", data.get("msg1", "Unknown"))

        return order

    # ========================
    # Order Execution - Overseas (US)
    # ========================

    async def submit_us_order(self, order: Order, exchange: str = "NASD") -> Order:
        """
        해외 주식 주문.
        매수: JTTT1002U (실전), VTTT1002U (모의)
        매도: JTTT1006U (실전), VTTT1006U (모의)
        """
        is_virtual = self._settings.kis_env.value == "VIRTUAL"

        if order.side == OrderSide.BUY:
            tr_id = "VTTT1002U" if is_virtual else "JTTT1002U"
        else:
            tr_id = "VTTT1006U" if is_virtual else "JTTT1006U"

        account_parts = self._settings.kis_account_no.split("-")
        body = {
            "CANO": account_parts[0],
            "ACNT_PRDT_CD": account_parts[1] if len(account_parts) > 1 else "01",
            "OVRS_EXCG_CD": exchange,
            "PDNO": order.symbol,
            "ORD_QTY": str(int(order.quantity)),
            "OVRS_ORD_UNPR": str(order.price) if order.price else "0",
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": "00",
        }

        body_str = json.dumps(body)
        headers = await self._get_headers(tr_id, body_str)
        url = f"{self._base_url}/uapi/overseas-stock/v1/trading/order"

        self._log.info(
            "Submitting US order: {} {} {} @ {}",
            order.side.value,
            order.quantity,
            order.symbol,
            order.price or "MARKET",
        )

        async with self._get_session().post(
            url, headers=headers, data=body_str
        ) as resp:
            data = await resp.json()

            if data.get("rt_cd") == "0":
                output = data.get("output", {})
                order.order_id = output.get("ODNO", "")
                order.status = OrderStatus.SUBMITTED
                self._log.info("US order submitted: {}", order.order_id)
            else:
                order.status = OrderStatus.REJECTED
                self._log.error("US order rejected: {}", data.get("msg1", "Unknown"))

        return order

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        # Implementation depends on domestic vs overseas
        self._log.warning("Cancel order not yet implemented for: {}", order_id)
        return False

    async def get_order_status(self, order_id: str) -> Order:
        """Get order status by ID."""
        raise NotImplementedError("get_order_status requires implementation")

    # ========================
    # Account & Portfolio
    # ========================

    async def get_balance(self) -> dict[str, float]:
        """
        계좌 잔고 조회.
        tr_id: TTTC8434R (실전), VTTC8434R (모의)
        """
        is_virtual = self._settings.kis_env.value == "VIRTUAL"
        tr_id = "VTTC8434R" if is_virtual else "TTTC8434R"

        account_parts = self._settings.kis_account_no.split("-")
        url = f"{self._base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
        params = {
            "CANO": account_parts[0],
            "ACNT_PRDT_CD": account_parts[1] if len(account_parts) > 1 else "01",
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
        headers = await self._get_headers(tr_id)

        async with self._get_session().get(url, headers=headers, params=params) as resp:
            data = await resp.json()
            output2 = data.get("output2", [{}])
            balance_info = output2[0] if output2 else {}

            return {
                "total_value": float(balance_info.get("tot_evlu_amt", 0)),
                "cash": float(balance_info.get("dnca_tot_amt", 0)),
                "invested": float(balance_info.get("scts_evlu_amt", 0)),
                "profit_loss": float(balance_info.get("evlu_pfls_smtl_amt", 0)),
                "profit_loss_pct": float(balance_info.get("evlu_pfls_rt", 0)),
            }

    async def get_positions(self) -> list[Position]:
        """Get all open positions."""
        is_virtual = self._settings.kis_env.value == "VIRTUAL"
        tr_id = "VTTC8434R" if is_virtual else "TTTC8434R"

        account_parts = self._settings.kis_account_no.split("-")
        url = f"{self._base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
        params = {
            "CANO": account_parts[0],
            "ACNT_PRDT_CD": account_parts[1] if len(account_parts) > 1 else "01",
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
        headers = await self._get_headers(tr_id)

        async with self._get_session().get(url, headers=headers, params=params) as resp:
            data = await resp.json()
            positions = []
            for item in data.get("output1", []):
                qty = float(item.get("hldg_qty", 0))
                if qty > 0:
                    positions.append(Position(
                        symbol=item.get("pdno", ""),
                        quantity=qty,
                        avg_price=float(item.get("pchs_avg_pric", 0)),
                        current_price=float(item.get("prpr", 0)),
                    ))
            return positions

    async def get_us_balance(self) -> dict[str, float]:
        """
        해외 주식 잔고 조회.
        tr_id: JTTT3012R (실전), VTTS3012R (모의)
        """
        is_virtual = self._settings.kis_env.value == "VIRTUAL"
        tr_id = "VTTS3012R" if is_virtual else "JTTT3012R"

        account_parts = self._settings.kis_account_no.split("-")
        url = f"{self._base_url}/uapi/overseas-stock/v1/trading/inquire-balance"
        params = {
            "CANO": account_parts[0],
            "ACNT_PRDT_CD": account_parts[1] if len(account_parts) > 1 else "01",
            "OVRS_EXCG_CD": "NASD",
            "TR_CRCY_CD": "USD",
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": "",
        }
        headers = await self._get_headers(tr_id)

        async with self._get_session().get(url, headers=headers, params=params) as resp:
            data = await resp.json()
            output2 = data.get("output2", {})

            return {
                "total_value_usd": float(output2.get("tot_evlu_pfls_amt", 0)),
                "total_profit_loss": float(output2.get("ovrs_tot_pfls", 0)),
                "total_profit_loss_pct": float(output2.get("tot_pftrt", 0)),
            }
