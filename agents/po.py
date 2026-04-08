"""QuantPulse v3 - PO (Performance Optimizer) Agent.

Trade journal + performance analytics + report generation.
Subscribes to all ES order events to track every trade lifecycle.
"""

from __future__ import annotations

import asyncio
import math
from datetime import datetime, timezone
from typing import Any

from core.base_agent import BaseAgent
from core.enums import AgentRole, Direction, Priority
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from core.models import Message, PerformanceReport, TradeRecord
from config import Config


class POAgent(BaseAgent):
    """
    Performance Optimizer — 성과 분석 + 저널 관리.

    - ES의 모든 주문 이벤트 수신 → Trade Journal 업데이트
    - 정기적으로 PerformanceReport 생성
    - Sharpe Ratio, Profit Factor, Max Drawdown, Win Rate 등 계산
    - Topic: po.performance_report
    """

    def __init__(self, bus: MessageBus, audit: AuditLogger, config: Config) -> None:
        super().__init__(AgentRole.PO, bus, audit)
        self.config = config

        # Trade journal
        self._journal: list[dict[str, Any]] = []
        self._open_trades: dict[str, dict[str, Any]] = {}  # order_id → info

        # Equity tracking
        self._equity_curve: list[float] = [10000.0]
        self._peak_equity: float = 10000.0
        self._current_equity: float = 10000.0

        # Stats
        self._total_pnl: float = 0.0
        self._winning_pnls: list[float] = []
        self._losing_pnls: list[float] = []
        self._consecutive_wins: int = 0
        self._consecutive_losses: int = 0
        self._max_consecutive_wins: int = 0
        self._max_consecutive_losses: int = 0

        # Report interval
        self._report_interval: float = 300.0  # 5 minutes

    async def setup_subscriptions(self) -> None:
        self.subscribe_safe("es.order_filled", self._handle_order_filled)
        self.subscribe_safe("es.order_closed", self._handle_order_closed)
        self.subscribe_safe("es.order_failed", self._handle_order_failed)

    async def on_start(self) -> None:
        self.create_task(self._report_loop(), name="po_report_loop")

    # ── Event Handlers ─────────────────────────

    async def _handle_order_filled(self, message: Message) -> None:
        """Track new trade open."""
        p = message.payload
        order_id = p.get("order_id", "")
        self._open_trades[order_id] = {
            "order_id": order_id,
            "signal_id": p.get("signal_id", ""),
            "symbol": p.get("symbol", ""),
            "direction": p.get("direction", ""),
            "filled_price": p.get("filled_price", 0.0),
            "quantity": p.get("filled_quantity", 0.0),
            "commission": p.get("commission", 0.0),
            "opened_at": p.get("timestamp", datetime.now(timezone.utc).isoformat()),
        }

    async def _handle_order_closed(self, message: Message) -> None:
        """Record completed trade to journal + update stats."""
        p = message.payload
        pnl = p.get("pnl", 0.0)
        pnl_pct = p.get("pnl_percent", 0.0)

        journal_entry = {
            "signal_id": p.get("signal_id", ""),
            "symbol": p.get("symbol", ""),
            "direction": p.get("direction", ""),
            "entry_price": p.get("entry_price", 0.0),
            "exit_price": p.get("exit_price", 0.0),
            "quantity": p.get("quantity", 0.0),
            "pnl": pnl,
            "pnl_percent": pnl_pct,
            "commission": p.get("commission", 0.0),
            "close_reason": p.get("close_reason", ""),
            "duration_seconds": p.get("duration_seconds", 0),
            "closed_at": datetime.now(timezone.utc).isoformat(),
        }
        self._journal.append(journal_entry)

        # Update equity curve
        self._total_pnl += pnl
        self._current_equity += pnl
        self._equity_curve.append(self._current_equity)
        self._peak_equity = max(self._peak_equity, self._current_equity)

        # Win/loss tracking
        if pnl > 0:
            self._winning_pnls.append(pnl)
            self._consecutive_wins += 1
            self._consecutive_losses = 0
            self._max_consecutive_wins = max(self._max_consecutive_wins, self._consecutive_wins)
        else:
            self._losing_pnls.append(pnl)
            self._consecutive_losses += 1
            self._consecutive_wins = 0
            self._max_consecutive_losses = max(self._max_consecutive_losses, self._consecutive_losses)

        # Remove from open trades
        order_id = p.get("order_id", "")
        self._open_trades.pop(order_id, None)

        await self.audit_log("trade_journaled", {
            "symbol": journal_entry["symbol"],
            "pnl": pnl,
            "total_pnl": self._total_pnl,
            "equity": self._current_equity,
            "total_trades": len(self._journal),
        })

    async def _handle_order_failed(self, message: Message) -> None:
        """Log failed orders."""
        await self.audit_log("order_failure_noted", {
            "symbol": message.payload.get("symbol", ""),
            "error": message.payload.get("error_message", ""),
        })

    # ── Report Generation ──────────────────────

    async def _report_loop(self) -> None:
        """Periodically generate and publish performance reports."""
        while self._running:
            await asyncio.sleep(self._report_interval)
            if self._journal:
                report = self.generate_report("periodic")
                await self.publish(
                    topic="po.performance_report",
                    payload=report.model_dump(mode="json"),
                    priority=Priority.NORMAL,
                )
                await self.audit_log("performance_report_published", {
                    "period": report.period,
                    "total_trades": report.total_trades,
                    "total_pnl": report.total_pnl,
                    "win_rate": report.win_rate,
                    "sharpe": report.sharpe_ratio,
                })

    def generate_report(self, period: str = "daily") -> PerformanceReport:
        """Generate a PerformanceReport from current journal data."""
        total = len(self._journal)
        wins = len(self._winning_pnls)
        losses = len(self._losing_pnls)
        win_rate = round(wins / max(total, 1) * 100, 2)

        # Max drawdown from equity curve
        max_dd = self._compute_max_drawdown()

        # Sharpe ratio (annualized, assuming daily returns)
        sharpe = self._compute_sharpe()

        # Profit factor
        total_wins = sum(self._winning_pnls) if self._winning_pnls else 0.0
        total_losses = abs(sum(self._losing_pnls)) if self._losing_pnls else 0.0
        profit_factor = round(total_wins / max(total_losses, 0.01), 2)

        # Average RR
        avg_win = total_wins / max(wins, 1)
        avg_loss = total_losses / max(losses, 1)
        avg_rr = round(avg_win / max(avg_loss, 0.01), 2)

        # Duration
        durations = [e.get("duration_seconds", 0) for e in self._journal]
        avg_duration = int(sum(durations) / max(len(durations), 1))

        best = max(self._winning_pnls) if self._winning_pnls else 0.0
        worst = min(self._losing_pnls) if self._losing_pnls else 0.0

        return PerformanceReport(
            period=period,
            total_trades=total,
            winning_trades=wins,
            losing_trades=losses,
            win_rate=win_rate,
            total_pnl=round(self._total_pnl, 4),
            max_drawdown=round(max_dd, 4),
            sharpe_ratio=sharpe,
            profit_factor=profit_factor,
            avg_risk_reward=avg_rr,
            best_trade_pnl=round(best, 4),
            worst_trade_pnl=round(worst, 4),
            avg_trade_duration_seconds=avg_duration,
            consecutive_wins=self._max_consecutive_wins,
            consecutive_losses=self._max_consecutive_losses,
        )

    # ── Analytics Computations ─────────────────

    def _compute_max_drawdown(self) -> float:
        """Compute max drawdown from equity curve."""
        if len(self._equity_curve) < 2:
            return 0.0

        peak = self._equity_curve[0]
        max_dd = 0.0
        for equity in self._equity_curve:
            peak = max(peak, equity)
            dd = (peak - equity) / peak if peak > 0 else 0.0
            max_dd = max(max_dd, dd)
        return max_dd

    def _compute_sharpe(self, risk_free: float = 0.0) -> float:
        """Compute Sharpe ratio from trade returns."""
        if len(self._journal) < 2:
            return 0.0

        returns = [e.get("pnl_percent", 0.0) for e in self._journal]
        mean_ret = sum(returns) / len(returns)
        var = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
        std = math.sqrt(var) if var > 0 else 0.0

        if std == 0:
            return 0.0

        # Annualize (assuming ~252 trading days)
        sharpe = (mean_ret - risk_free) / std * math.sqrt(252)
        return round(sharpe, 2)

    # ── Query ──────────────────────────────────

    def get_journal(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._journal[-limit:]

    @property
    def po_snapshot(self) -> dict[str, Any]:
        return {
            "total_trades": len(self._journal),
            "open_trades": len(self._open_trades),
            "total_pnl": round(self._total_pnl, 4),
            "current_equity": round(self._current_equity, 4),
            "peak_equity": round(self._peak_equity, 4),
            "win_rate": round(
                len(self._winning_pnls) / max(len(self._journal), 1) * 100, 2
            ),
            "max_drawdown": round(self._compute_max_drawdown() * 100, 2),
            "sharpe_ratio": self._compute_sharpe(),
            "consecutive_wins": self._max_consecutive_wins,
            "consecutive_losses": self._max_consecutive_losses,
        }
