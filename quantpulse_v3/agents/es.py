"""QuantPulse v3 - ES (Execution Specialist) Agent.

Handles order execution in Paper and Live modes.
Manages SL/TP/Trailing stop lifecycle.
Publishes order events: submitted, filled, closed, failed.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from core.base_agent import BaseAgent
from core.enums import (
    AgentRole, AlertSeverity, Direction, OrderStatus, OrderType,
    Priority, TradeMode,
)
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from core.models import (
    Message, OrderResult, TradeDecision, TradeRecord,
)
from config import Config


class ESAgent(BaseAgent):
    """
    Execution Specialist — 주문 실행 + 포지션 관리 에이전트.

    - CSO의 TradeDecision(approved=True)만 수신하여 실행
    - Paper 모드: 내부 시뮬레이션 엔진으로 즉시 체결
    - Live 모드: 거래소 API 연동 (Phase 4에서 구현)
    - SL/TP1~3/Trailing Stop 관리
    - 모든 주문 이벤트를 MessageBus로 발행
    - 모든 주문 결과를 AuditLog에 기록
    """

    def __init__(self, bus: MessageBus, audit: AuditLogger, config: Config) -> None:
        super().__init__(AgentRole.ES, bus, audit)
        self.config = config

        # ── Active orders & positions ──
        self._open_orders: dict[str, dict[str, Any]] = {}   # order_id → order_info
        self._open_trades: dict[str, dict[str, Any]] = {}   # trade_id → trade_info
        self._trade_history: list[TradeRecord] = []

        # ── Trailing stop state ──
        self._trailing_stops: dict[str, dict[str, Any]] = {}  # trade_id → trailing_info

        # ── Circuit breaker flag ──
        self._circuit_breaker_active: bool = False

        # ── Price feed (injected by JWQuantSystem or tests) ──
        self._price_feed: dict[str, float] = {}  # symbol → latest price

        # ── Live exchange connector (injected by JWQuantSystem) ──
        self._exchange: Any | None = None  # BaseExchange instance

    # ── Lifecycle ──────────────────────────────

    async def setup_subscriptions(self) -> None:
        self.subscribe_safe("cso.trade_decision", self._handle_trade_decision)
        self.subscribe_safe("crco.circuit_breaker", self._handle_circuit_breaker)

    async def on_start(self) -> None:
        self.create_task(self._position_monitor_loop(), name="es_position_monitor")
        # P0#5: Recover positions from exchange on restart
        await self._recover_positions()

    # ── Main Order Handler ─────────────────────

    async def _handle_trade_decision(self, message: Message) -> None:
        """CSO가 승인한 TradeDecision을 수신하여 주문 실행."""
        decision = TradeDecision(**message.payload)

        # Safety: CRCO approved인 경우만 실행
        if not decision.approved:
            self.logger.warning(f"[ES] Received non-approved decision {decision.decision_id}, ignoring")
            await self.audit_log("decision_rejected", {
                "decision_id": decision.decision_id,
                "reason": "not_approved",
            })
            return

        # Circuit breaker 체크
        if self._circuit_breaker_active:
            self.logger.warning(f"[ES] Circuit breaker active, rejecting {decision.decision_id}")
            await self._publish_order_failed(
                decision, "Circuit breaker active, order rejected"
            )
            return

        await self.audit_log("order_execution_start", {
            "decision_id": decision.decision_id,
            "signal_id": decision.signal_id,
            "symbol": decision.symbol,
            "direction": decision.direction.value,
            "trade_mode": decision.trade_mode.value,
        })

        # Route to appropriate execution mode
        if decision.trade_mode == TradeMode.PAPER:
            await self._execute_paper(decision)
        else:
            await self._execute_live(decision)

    # ── Paper Execution ────────────────────────

    async def _execute_paper(self, decision: TradeDecision) -> None:
        """Paper mode: 즉시 체결 시뮬레이션."""
        order_id = uuid.uuid4().hex[:12]
        trade_id = uuid.uuid4().hex[:12]

        # Simulate market fill with configurable slippage
        slippage_bps = self.config.trading.slippage_bps
        slippage_mult = 1.0 + (slippage_bps / 10000) * (
            1 if decision.direction == Direction.LONG else -1
        )
        filled_price = round(decision.entry_price * slippage_mult, 8)
        slippage = round(abs(filled_price - decision.entry_price), 8)

        # Simulate commission (configurable taker fee on notional value)
        commission_rate = self.config.trading.commission_rate
        notional_value = filled_price * decision.position_size
        commission = round(notional_value * commission_rate, 4)

        # ── Step 1: Publish order_submitted ──
        submitted_result = OrderResult(
            order_id=order_id,
            decision_id=decision.decision_id,
            signal_id=decision.signal_id,
            symbol=decision.symbol,
            direction=decision.direction,
            order_type=decision.order_type,
            status=OrderStatus.SUBMITTED,
            requested_price=decision.entry_price,
            quantity=decision.position_size,
            trade_mode=TradeMode.PAPER,
            exchange="paper",
        )
        await self._publish_order_event("es.order_submitted", submitted_result)

        # Small delay to simulate network latency
        await asyncio.sleep(0.05)

        # ── Step 2: Publish order_filled ──
        filled_result = OrderResult(
            order_id=order_id,
            decision_id=decision.decision_id,
            signal_id=decision.signal_id,
            symbol=decision.symbol,
            direction=decision.direction,
            order_type=decision.order_type,
            status=OrderStatus.FILLED,
            requested_price=decision.entry_price,
            filled_price=filled_price,
            quantity=decision.position_size,
            filled_quantity=decision.position_size,
            commission=commission,
            slippage=slippage,
            trade_mode=TradeMode.PAPER,
            exchange="paper",
        )
        await self._publish_order_event("es.order_filled", filled_result)

        # ── Step 3: Register active trade ──
        self._open_trades[trade_id] = {
            "trade_id": trade_id,
            "order_id": order_id,
            "decision_id": decision.decision_id,
            "signal_id": decision.signal_id,
            "symbol": decision.symbol,
            "direction": decision.direction.value,
            "entry_price": filled_price,
            "quantity": decision.position_size,
            "stop_loss": decision.stop_loss,
            "take_profit_1": decision.take_profit_1,
            "take_profit_2": decision.take_profit_2,
            "take_profit_3": decision.take_profit_3,
            "leverage": decision.leverage,
            "commission_entry": commission,
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "highest_price": filled_price,  # For trailing stop
            "lowest_price": filled_price,
        }

        # Set up trailing stop (activate after TP1 hit)
        self._trailing_stops[trade_id] = {
            "active": False,
            "trail_pct": self.config.trading.trailing_stop_pct,
            "activation_price": decision.take_profit_1,
            "trail_price": None,
        }

        await self.audit_log("paper_order_filled", {
            "trade_id": trade_id,
            "order_id": order_id,
            "symbol": decision.symbol,
            "direction": decision.direction.value,
            "filled_price": filled_price,
            "quantity": decision.position_size,
            "sl": decision.stop_loss,
            "tp1": decision.take_profit_1,
            "commission": commission,
            "slippage": slippage,
        })

        self.logger.info(
            f"[ES] PAPER FILLED: {decision.symbol} {decision.direction.value} "
            f"@ {filled_price} qty={decision.position_size} "
            f"SL={decision.stop_loss} TP1={decision.take_profit_1}"
        )

    # ── Live Execution ──────────────────────────

    def set_exchange(self, exchange: Any) -> None:
        """Set the live exchange connector (called by JWQuantSystem)."""
        self._exchange = exchange

    async def _execute_live(self, decision: TradeDecision) -> None:
        """Live mode: 거래소 API 주문 + retry + stop order 생성."""
        if not self._exchange:
            await self._publish_order_failed(decision, "No exchange connector configured")
            return
        if not self._exchange.is_connected:
            await self._publish_order_failed(decision, "Exchange not connected")
            return

        trade_id = uuid.uuid4().hex[:12]

        # P0#6: Retry logic — wrap exchange call in retry_async
        async def _do_order():
            side = "buy" if decision.direction == Direction.LONG else "sell"
            ot = "market" if decision.order_type == OrderType.MARKET else "limit"
            price = None if ot == "market" else decision.entry_price
            return await asyncio.wait_for(
                self._exchange.create_order(
                    symbol=decision.symbol, side=side,
                    order_type=ot, amount=decision.position_size, price=price,
                ),
                timeout=30.0,
            )

        try:
            # Set leverage (best-effort)
            try:
                await asyncio.wait_for(
                    self._exchange.set_leverage(decision.symbol, int(decision.leverage)),
                    timeout=10.0,
                )
            except Exception:
                pass

            await self._publish_order_event("es.order_submitted", OrderResult(
                decision_id=decision.decision_id, signal_id=decision.signal_id,
                symbol=decision.symbol, direction=decision.direction,
                order_type=decision.order_type, status=OrderStatus.SUBMITTED,
                requested_price=decision.entry_price, quantity=decision.position_size,
                trade_mode=TradeMode.LIVE, exchange=self._exchange.name,
            ))

            exchange_order = await self.retry_async(_do_order, "live_order", max_retries=3)

            order_id = exchange_order.order_id
            filled_price = exchange_order.price
            filled_qty = exchange_order.filled
            commission = filled_price * filled_qty * self.config.trading.commission_rate

            await self._publish_order_event("es.order_filled", OrderResult(
                order_id=order_id, decision_id=decision.decision_id,
                signal_id=decision.signal_id, symbol=decision.symbol,
                direction=decision.direction, order_type=decision.order_type,
                status=OrderStatus.FILLED, requested_price=decision.entry_price,
                filled_price=filled_price, quantity=decision.position_size,
                filled_quantity=filled_qty, commission=commission,
                slippage=round(abs(filled_price - decision.entry_price), 8),
                trade_mode=TradeMode.LIVE, exchange=self._exchange.name,
            ))

            self._open_trades[trade_id] = {
                "trade_id": trade_id, "order_id": order_id,
                "decision_id": decision.decision_id, "signal_id": decision.signal_id,
                "symbol": decision.symbol, "direction": decision.direction.value,
                "entry_price": filled_price, "quantity": filled_qty,
                "stop_loss": decision.stop_loss, "take_profit_1": decision.take_profit_1,
                "take_profit_2": decision.take_profit_2, "take_profit_3": decision.take_profit_3,
                "leverage": decision.leverage, "commission_entry": commission,
                "opened_at": datetime.now(timezone.utc).isoformat(),
                "highest_price": filled_price, "lowest_price": filled_price,
                "tp1_closed": False, "tp2_closed": False,  # P1#11: partial TP tracking
                "original_quantity": filled_qty,
            }
            self._trailing_stops[trade_id] = {
                "active": False, "trail_pct": self.config.trading.trailing_stop_pct,
                "activation_price": decision.take_profit_1, "trail_price": None,
            }

            # P0#4: Place SL stop order on exchange for safety
            await self._place_stop_order(decision.symbol, decision.direction, filled_qty, decision.stop_loss)

            await self.audit_log("live_order_filled", {
                "trade_id": trade_id, "order_id": order_id,
                "symbol": decision.symbol, "filled_price": filled_price,
                "quantity": filled_qty, "exchange": self._exchange.name,
            })
            self.logger.info(
                f"[ES] LIVE FILLED: {decision.symbol} {decision.direction.value} "
                f"@ {filled_price} qty={filled_qty} via {self._exchange.name}"
            )
        except Exception as e:
            self.logger.error(f"[ES] Live execution failed: {e}")
            await self._publish_order_failed(decision, f"Exchange error: {e}")

    # ── P0#4: Stop Order on Exchange ───────────

    async def _place_stop_order(
        self, symbol: str, direction: Direction, qty: float, stop_price: float
    ) -> None:
        """Place a stop-market order on the exchange as SL protection."""
        if not self._exchange or not self._exchange.is_connected:
            return
        try:
            close_side = "sell" if direction == Direction.LONG else "buy"
            await asyncio.wait_for(
                self._exchange.create_order(
                    symbol=symbol, side=close_side, order_type="stop_market",
                    amount=qty, price=stop_price,
                ),
                timeout=10.0,
            )
            self.logger.info(f"[ES] SL stop order placed: {symbol} {close_side} @ {stop_price}")
            await self.audit_log("stop_order_placed", {
                "symbol": symbol, "side": close_side, "stop_price": stop_price, "qty": qty,
            })
        except Exception as e:
            self.logger.warning(f"[ES] Stop order failed (ES monitor will cover SL): {e}")

    # ── P0#5: Position Recovery ────────────────

    async def _recover_positions(self) -> None:
        """On restart, sync open positions from exchange into internal state."""
        if not self._exchange or not self._exchange.is_connected:
            return
        try:
            positions = await asyncio.wait_for(
                self._exchange.fetch_positions(), timeout=15.0,
            )
            for pos in positions:
                symbol = pos.get("symbol", "")
                amount = pos.get("amount", 0)
                if amount <= 0:
                    continue
                trade_id = uuid.uuid4().hex[:12]
                direction = "LONG" if pos.get("side") == "long" else "SHORT"
                entry = pos.get("entry_price", 0)
                self._open_trades[trade_id] = {
                    "trade_id": trade_id, "order_id": "recovered",
                    "decision_id": "recovered", "signal_id": "recovered",
                    "symbol": symbol, "direction": direction,
                    "entry_price": entry, "quantity": amount,
                    "stop_loss": entry * (0.98 if direction == "LONG" else 1.02),
                    "take_profit_1": entry * (1.04 if direction == "LONG" else 0.96),
                    "take_profit_2": None, "take_profit_3": None,
                    "leverage": pos.get("leverage", 1),
                    "commission_entry": 0, "opened_at": datetime.now(timezone.utc).isoformat(),
                    "highest_price": entry, "lowest_price": entry,
                    "tp1_closed": False, "tp2_closed": False,
                    "original_quantity": amount,
                }
                self.logger.info(f"[ES] Recovered position: {symbol} {direction} qty={amount} @ {entry}")
                await self.audit_log("position_recovered", {
                    "trade_id": trade_id, "symbol": symbol,
                    "direction": direction, "entry": entry, "quantity": amount,
                })
        except Exception as e:
            self.logger.warning(f"[ES] Position recovery failed: {e}")

    # ── Position Monitor Loop ──────────────────

    async def _position_monitor_loop(self) -> None:
        """
        Paper mode 포지션 모니터링 루프.
        실제 가격 데이터 연동 전까지는 시뮬레이션 가격으로 동작.
        SL/TP 히트 체크 + Trailing stop 업데이트.
        """
        while self._running:
            try:
                for trade_id in list(self._open_trades.keys()):
                    await self._check_sl_tp(trade_id)
            except Exception as e:
                self.logger.error(f"[ES] Position monitor error: {e}")
            await asyncio.sleep(1.0)

    def inject_price(self, symbol: str, price: float) -> None:
        """Inject price for SL/TP monitoring (called by JWQuantSystem or tests)."""
        self._price_feed[symbol] = price

    async def _check_sl_tp(self, trade_id: str) -> None:
        """SL/TP/Trailing check with partial take-profit support."""
        trade = self._open_trades.get(trade_id)
        if not trade:
            return

        symbol = trade["symbol"]
        current_price = self._price_feed.get(symbol)
        if current_price is None or current_price <= 0:
            return

        direction = trade["direction"]
        is_long = direction == Direction.LONG.value

        self.update_trailing_stop(trade_id, current_price)
        sl = trade["stop_loss"]

        # SL hit → close everything
        if (is_long and current_price <= sl) or (not is_long and current_price >= sl):
            await self.close_trade(trade_id, sl, reason="sl_hit")
            return

        tp1 = trade["take_profit_1"]
        tp2 = trade.get("take_profit_2")
        tp3 = trade.get("take_profit_3")

        # P1#11: Partial TP — close configured percentage at each level
        tp1_hit = (is_long and current_price >= tp1) or (not is_long and current_price <= tp1)
        tp2_hit = tp2 and ((is_long and current_price >= tp2) or (not is_long and current_price <= tp2))
        tp3_hit = tp3 and ((is_long and current_price >= tp3) or (not is_long and current_price <= tp3))

        if tp3_hit and not trade.get("tp2_closed"):
            # TP3 before TP2 closed — close remaining
            await self.close_trade(trade_id, tp3, reason="tp3_hit")
        elif tp2_hit and not trade.get("tp2_closed"):
            close_pct = self.config.trading.tp2_close_pct
            await self._partial_close(trade_id, tp2, close_pct, "tp2_hit")
            trade["tp2_closed"] = True
        elif tp1_hit and not trade.get("tp1_closed"):
            close_pct = self.config.trading.tp1_close_pct
            await self._partial_close(trade_id, tp1, close_pct, "tp1_hit")
            trade["tp1_closed"] = True

    async def _partial_close(
        self, trade_id: str, exit_price: float, close_pct: float, reason: str,
    ) -> None:
        """Close a fraction of a position (partial take-profit)."""
        trade = self._open_trades.get(trade_id)
        if not trade:
            return
        full_qty = trade["quantity"]
        close_qty = round(full_qty * close_pct, 8)
        if close_qty <= 0:
            return

        remaining = round(full_qty - close_qty, 8)
        direction = trade["direction"]
        entry = trade["entry_price"]

        if direction == Direction.LONG.value:
            raw_pnl = (exit_price - entry) * close_qty
        else:
            raw_pnl = (entry - exit_price) * close_qty

        commission = round(exit_price * close_qty * self.config.trading.commission_rate, 4)
        net_pnl = round(raw_pnl - commission, 4)

        trade["quantity"] = remaining

        payload = {
            "order_id": trade.get("order_id", ""), "decision_id": trade["decision_id"],
            "signal_id": trade["signal_id"], "symbol": trade["symbol"],
            "direction": direction, "pnl": net_pnl,
            "pnl_percent": round((net_pnl / (entry * close_qty)) * 100, 4) if entry * close_qty > 0 else 0,
            "entry_price": entry, "exit_price": exit_price,
            "quantity": close_qty, "remaining_quantity": remaining,
            "close_reason": reason, "partial": True,
        }
        await self.publish(topic="es.order_closed", payload=payload, priority=Priority.HIGH)
        await self.audit_log("partial_close", payload)
        self.logger.info(
            f"[ES] PARTIAL CLOSE: {trade['symbol']} {reason} "
            f"closed={close_qty} remaining={remaining} PnL={net_pnl:+.2f}"
        )

        if remaining <= 0:
            self._open_trades.pop(trade_id, None)
            self._trailing_stops.pop(trade_id, None)

    async def close_trade(
        self,
        trade_id: str,
        exit_price: float,
        reason: str = "manual",
    ) -> None:
        """포지션 종료 처리 (SL/TP/Trailing/Manual 공통)."""
        trade = self._open_trades.get(trade_id)
        if not trade:
            self.logger.warning(f"[ES] Trade {trade_id} not found for close")
            return

        direction = trade["direction"]
        entry = trade["entry_price"]
        qty = trade["quantity"]
        commission_entry = trade.get("commission_entry", 0.0)

        # Calculate PnL
        if direction == Direction.LONG.value:
            raw_pnl = (exit_price - entry) * qty
        else:
            raw_pnl = (entry - exit_price) * qty

        commission_exit = round(exit_price * qty * self.config.trading.commission_rate, 4)
        total_commission = commission_entry + commission_exit
        net_pnl = round(raw_pnl - total_commission, 4)
        pnl_pct = round((net_pnl / (entry * qty)) * 100, 4) if entry * qty > 0 else 0.0

        # Create trade record
        record = TradeRecord(
            trade_id=trade_id,
            signal_id=trade["signal_id"],
            symbol=trade["symbol"],
            direction=Direction(direction),
            entry_price=entry,
            exit_price=exit_price,
            stop_loss=trade["stop_loss"],
            take_profit_1=trade["take_profit_1"],
            quantity=qty,
            pnl=net_pnl,
            pnl_percent=pnl_pct,
            commission_total=total_commission,
            trade_mode=TradeMode.PAPER,
            opened_at=datetime.fromisoformat(trade["opened_at"]),
            closed_at=datetime.now(timezone.utc),
        )
        record.duration_seconds = int(
            (record.closed_at - record.opened_at).total_seconds()  # type: ignore[operator]
        )
        self._trade_history.append(record)

        # ── Publish order_closed ──
        close_result = OrderResult(
            order_id=trade.get("order_id", ""),
            decision_id=trade["decision_id"],
            signal_id=trade["signal_id"],
            symbol=trade["symbol"],
            direction=Direction(direction),
            order_type=OrderType.MARKET,
            status=OrderStatus.CLOSED,
            requested_price=exit_price,
            filled_price=exit_price,
            quantity=qty,
            filled_quantity=qty,
            commission=commission_exit,
            trade_mode=TradeMode.PAPER,
            exchange="paper",
        )
        # Attach PnL info to payload
        payload = close_result.model_dump(mode="json")
        payload.update({
            "pnl": net_pnl,
            "pnl_percent": pnl_pct,
            "entry_price": entry,
            "exit_price": exit_price,
            "stop_loss": trade["stop_loss"],
            "take_profit_1": trade["take_profit_1"],
            "close_reason": reason,
            "duration_seconds": record.duration_seconds,
        })

        await self.publish(
            topic="es.order_closed",
            payload=payload,
            priority=Priority.HIGH,
        )

        await self.audit_log("trade_closed", {
            "trade_id": trade_id,
            "symbol": trade["symbol"],
            "direction": direction,
            "entry": entry,
            "exit": exit_price,
            "pnl": net_pnl,
            "pnl_pct": pnl_pct,
            "reason": reason,
            "duration_seconds": record.duration_seconds,
            "commission": total_commission,
        })

        # Cleanup
        self._open_trades.pop(trade_id, None)
        self._trailing_stops.pop(trade_id, None)

        pnl_emoji = "+" if net_pnl >= 0 else ""
        self.logger.info(
            f"[ES] CLOSED: {trade['symbol']} {direction} "
            f"@ {exit_price} PnL={pnl_emoji}{net_pnl:.2f} ({pnl_pct:+.2f}%) "
            f"reason={reason}"
        )

    # ── Trailing Stop Logic ────────────────────

    def update_trailing_stop(self, trade_id: str, current_price: float) -> float | None:
        """
        트레일링 스탑 업데이트. TP1 도달 후 활성화.
        Returns new trailing stop price if updated, None otherwise.
        """
        trade = self._open_trades.get(trade_id)
        trail = self._trailing_stops.get(trade_id)
        if not trade or not trail:
            return None

        direction = trade["direction"]

        # Check if TP1 reached → activate trailing
        if not trail["active"]:
            activation = trail["activation_price"]
            if direction == Direction.LONG.value and current_price >= activation:
                trail["active"] = True
            elif direction == Direction.SHORT.value and current_price <= activation:
                trail["active"] = True

        # Update trailing price
        if trail["active"]:
            pct = trail["trail_pct"]
            if direction == Direction.LONG.value:
                new_trail = current_price * (1 - pct)
                if trail["trail_price"] is None or new_trail > trail["trail_price"]:
                    trail["trail_price"] = round(new_trail, 8)
                    # Update SL to trailing stop
                    trade["stop_loss"] = trail["trail_price"]
                    return trail["trail_price"]
            else:
                new_trail = current_price * (1 + pct)
                if trail["trail_price"] is None or new_trail < trail["trail_price"]:
                    trail["trail_price"] = round(new_trail, 8)
                    trade["stop_loss"] = trail["trail_price"]
                    return trail["trail_price"]

        return None

    # ── Event Publishers ───────────────────────

    async def _publish_order_event(self, topic: str, result: OrderResult) -> None:
        """Publish an order event and log to audit."""
        await self.publish(
            topic=topic,
            payload=result.model_dump(mode="json"),
            priority=Priority.HIGH,
        )
        await self.audit_log(
            topic.replace("es.", ""),
            {
                "order_id": result.order_id,
                "symbol": result.symbol,
                "status": result.status.value,
                "direction": result.direction.value,
                "filled_price": result.filled_price,
                "quantity": result.quantity,
            },
        )

    async def _publish_order_failed(self, decision: TradeDecision, reason: str) -> None:
        """Publish order failure event."""
        fail_result = OrderResult(
            decision_id=decision.decision_id,
            signal_id=decision.signal_id,
            symbol=decision.symbol,
            direction=decision.direction,
            order_type=decision.order_type,
            status=OrderStatus.FAILED,
            requested_price=decision.entry_price,
            quantity=decision.position_size,
            trade_mode=decision.trade_mode,
            error_message=reason,
        )
        await self._publish_order_event("es.order_failed", fail_result)
        self.logger.warning(f"[ES] ORDER FAILED: {decision.symbol} - {reason}")

    # ── Circuit Breaker Handler ────────────────

    async def _handle_circuit_breaker(self, message: Message) -> None:
        """Circuit breaker broadcast 수신 → 신규 주문 차단."""
        active = message.payload.get("active", True)
        self._circuit_breaker_active = active
        reason = message.payload.get("reason", "unknown")

        if active:
            self.logger.critical(f"[ES] Circuit breaker received: {reason}")
            await self.audit_log("circuit_breaker_received", {"reason": reason})

            # Close all open positions at current prices if circuit breaker fires
            # (In paper mode, we close at entry price as fallback since no live feed)
            for trade_id, trade in list(self._open_trades.items()):
                await self.close_trade(
                    trade_id,
                    exit_price=trade["entry_price"],
                    reason="circuit_breaker_emergency_close",
                )
        else:
            self.logger.info("[ES] Circuit breaker deactivated, trading resumed")
            await self.audit_log("circuit_breaker_cleared", {})

    # ── Query Methods ──────────────────────────

    # ── Emergency Stop ──────────────────────────

    async def emergency_stop(self) -> dict[str, Any]:
        """
        EMERGENCY STOP: Close all positions immediately + block new orders.

        Used for:
        - Manual panic button
        - Pre-live safety procedure
        - Catastrophic error recovery

        Returns summary of all closed positions.
        """
        self._circuit_breaker_active = True
        closed_trades: list[dict[str, Any]] = []
        errors: list[str] = []

        self.logger.critical("[ES] EMERGENCY STOP ACTIVATED — closing all positions")

        for trade_id, trade in list(self._open_trades.items()):
            try:
                # Use entry price as fallback (no live feed guaranteed)
                exit_price = self._price_feed.get(trade["symbol"], trade["entry_price"])
                await self.close_trade(trade_id, exit_price, reason="emergency_stop")
                closed_trades.append({
                    "trade_id": trade_id,
                    "symbol": trade["symbol"],
                    "exit_price": exit_price,
                })
            except Exception as e:
                errors.append(f"{trade_id}: {e}")
                self.logger.error(f"[ES] Emergency close failed for {trade_id}: {e}")

        result = {
            "positions_closed": len(closed_trades),
            "errors": len(errors),
            "closed": closed_trades,
            "error_details": errors,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        await self.audit_log("emergency_stop", result)
        await self._emit_alert(AlertSeverity.FATAL, "EMERGENCY STOP", f"Closed {len(closed_trades)} positions")

        return result

    # ── Query Methods ──────────────────────────

    @property
    def open_trades(self) -> dict[str, dict[str, Any]]:
        return dict(self._open_trades)

    @property
    def trade_count(self) -> int:
        return len(self._trade_history)

    def get_trade_history(self, limit: int = 50) -> list[TradeRecord]:
        return self._trade_history[-limit:]

    @property
    def execution_snapshot(self) -> dict[str, Any]:
        """Current execution state for monitoring/API."""
        total_pnl = sum(t.pnl for t in self._trade_history)
        wins = sum(1 for t in self._trade_history if t.pnl > 0)
        losses = sum(1 for t in self._trade_history if t.pnl <= 0)
        return {
            "open_trades": len(self._open_trades),
            "total_executed": len(self._trade_history),
            "total_pnl": round(total_pnl, 4),
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / max(wins + losses, 1) * 100, 2),
            "circuit_breaker_active": self._circuit_breaker_active,
            "open_positions": [
                {
                    "symbol": t["symbol"],
                    "direction": t["direction"],
                    "entry": t["entry_price"],
                    "sl": t["stop_loss"],
                    "tp1": t["take_profit_1"],
                }
                for t in self._open_trades.values()
            ],
        }
