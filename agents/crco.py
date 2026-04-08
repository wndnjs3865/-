"""QuantPulse v3 - CRCO (Chief Risk & Compliance Officer) Agent.

The most powerful agent in the system. Has absolute veto authority.
Performs 11 risk validation checks on every trade signal.
Manages circuit breaker for catastrophic loss prevention.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Any

from core.base_agent import BaseAgent
from core.enums import AgentRole, AlertSeverity, Direction, Priority, TradeMode
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from core.models import (
    Message, RiskAssessment, RiskValidation, TradeSignal, TradeRecord,
)
from config import Config


class CRCOAgent(BaseAgent):
    """
    Chief Risk & Compliance Officer — 시스템 최고 리스크 관리 에이전트.

    - 11개 리스크 체크 수행 (하나라도 실패 시 veto)
    - Circuit Breaker: 일일 최대 손실 초과 시 전체 거래 중단
    - 모든 결정은 AuditLog에 기록
    - CSO도 CRCO의 veto를 절대 무시할 수 없음
    """

    def __init__(self, bus: MessageBus, audit: AuditLogger, config: Config) -> None:
        super().__init__(AgentRole.CRCO, bus, audit)
        self.config = config

        # ── Portfolio state (실시간 추적) ──
        self._portfolio_value: float = config.trading.initial_capital
        self._daily_pnl: float = 0.0
        self._daily_start_value: float = config.trading.initial_capital
        self._peak_value: float = config.trading.initial_capital
        self._open_positions: dict[str, dict[str, Any]] = {}  # symbol → position_info
        self._trade_history: list[TradeRecord] = []
        self._consecutive_losses: int = 0
        self._trades_today: int = 0

        # ── Circuit Breaker state ──
        self._circuit_breaker_active: bool = False
        self._circuit_breaker_activated_at: float = 0.0

        # ── Daily reset tracking ──
        self._last_daily_reset: str = ""

    # ── Lifecycle ──────────────────────────────

    async def setup_subscriptions(self) -> None:
        self.subscribe_safe("cso.request_risk_assessment", self._handle_risk_request)
        self.subscribe_safe("es.order_filled", self._handle_order_filled)
        self.subscribe_safe("es.order_closed", self._handle_order_closed)
        self.subscribe_safe("es.order_failed", self._handle_order_failed)

    async def on_start(self) -> None:
        self._reset_daily_stats()
        self.create_task(self._daily_reset_loop(), name="crco_daily_reset")

    # ── Main Risk Assessment Handler ──────────

    async def _handle_risk_request(self, message: Message) -> None:
        """CSO가 리스크 평가를 요청할 때 호출."""
        payload = message.payload
        signal_id = payload.get("signal_id", "unknown")
        signal_data = payload.get("trade_signal", {})
        correlation_id = message.correlation_id

        signal = TradeSignal(**signal_data) if signal_data else None

        # Check for daily reset
        self._check_daily_reset()

        # Run all 11 risk validations
        validations = self._run_all_checks(signal, payload)

        veto_reasons = [v.detail for v in validations if not v.passed]
        approved = len(veto_reasons) == 0 and not self._circuit_breaker_active

        # Calculate position sizing if approved
        max_size = 0.0
        rec_leverage = 1.0
        risk_score = self._compute_risk_score(validations)

        if approved and signal:
            max_size = self._calculate_position_size(signal)
            rec_leverage = min(
                self.config.trading.default_leverage,
                self.config.trading.max_leverage,
            )

        if self._circuit_breaker_active:
            veto_reasons.insert(0, "circuit_breaker_active")

        assessment = RiskAssessment(
            signal_id=signal_id,
            symbol=signal.symbol if signal else payload.get("symbol", "UNKNOWN"),
            approved=approved,
            validations=validations,
            veto_reasons=veto_reasons,
            risk_score=risk_score,
            max_position_size=max_size,
            recommended_leverage=rec_leverage,
            circuit_breaker_active=self._circuit_breaker_active,
        )

        # ── AuditLog 기록 (무조건) ──
        await self.audit_log(
            "risk_assessment",
            {
                "signal_id": signal_id,
                "approved": approved,
                "veto_reasons": veto_reasons,
                "risk_score": risk_score,
                "max_position_size": max_size,
                "circuit_breaker": self._circuit_breaker_active,
                "validations_summary": [
                    {"check": v.check_name, "passed": v.passed, "detail": v.detail}
                    for v in validations
                ],
            },
            correlation_id=correlation_id,
        )

        # ── 결과 발행 → CSO에게 반환 ──
        await self.publish(
            topic="crco.risk_assessment",
            payload=assessment.model_dump(mode="json"),
            priority=Priority.URGENT,
            correlation_id=correlation_id,
        )

        status = "APPROVED" if approved else f"VETOED ({len(veto_reasons)} reasons)"
        self.logger.info(f"[CRCO] {signal_id}: {status} | risk_score={risk_score:.1f}")

    # ── 11 Risk Validation Checks ─────────────

    def _run_all_checks(
        self, signal: TradeSignal | None, payload: dict[str, Any]
    ) -> list[RiskValidation]:
        """11개 리스크 체크를 순서대로 수행."""
        checks: list[RiskValidation] = []

        checks.append(self._check_01_circuit_breaker())
        checks.append(self._check_02_daily_loss_limit())
        checks.append(self._check_03_max_drawdown())
        checks.append(self._check_04_position_limit())
        checks.append(self._check_05_single_asset_exposure(signal))
        checks.append(self._check_06_risk_reward_ratio(signal))
        checks.append(self._check_07_leverage_limit(payload))
        checks.append(self._check_08_consecutive_loss_limit())
        checks.append(self._check_09_daily_trade_count())
        checks.append(self._check_10_portfolio_heat())
        checks.append(self._check_11_liquidation_risk(signal, payload))

        return checks

    def _check_01_circuit_breaker(self) -> RiskValidation:
        """Check 1: Circuit Breaker가 활성화되어 있는가?"""
        # Circuit breaker cooldown 체크
        if self._circuit_breaker_active:
            elapsed = time.time() - self._circuit_breaker_activated_at
            cooldown = self.config.trading.circuit_breaker_cooldown
            if elapsed >= cooldown:
                self._circuit_breaker_active = False
                self.logger.info("[CRCO] Circuit breaker cooldown expired, trading resumed")
            else:
                remaining = int(cooldown - elapsed)
                return RiskValidation(
                    check_name="circuit_breaker",
                    passed=False,
                    detail=f"Circuit breaker active, {remaining}s remaining",
                )
        return RiskValidation(check_name="circuit_breaker", passed=True, detail="No circuit breaker")

    def _check_02_daily_loss_limit(self) -> RiskValidation:
        """Check 2: 일일 손실 한도 초과 여부."""
        threshold = self.config.trading.max_daily_loss
        daily_loss_pct = abs(min(0.0, self._daily_pnl)) / max(self._daily_start_value, 1.0)

        if daily_loss_pct >= threshold:
            self._activate_circuit_breaker("daily_loss_limit_exceeded")
            return RiskValidation(
                check_name="daily_loss_limit",
                passed=False,
                value=round(daily_loss_pct * 100, 2),
                threshold=round(threshold * 100, 2),
                detail=f"Daily loss {daily_loss_pct*100:.2f}% >= limit {threshold*100:.1f}%",
            )
        return RiskValidation(
            check_name="daily_loss_limit",
            passed=True,
            value=round(daily_loss_pct * 100, 2),
            threshold=round(threshold * 100, 2),
            detail=f"Daily loss {daily_loss_pct*100:.2f}% within limit",
        )

    def _check_03_max_drawdown(self) -> RiskValidation:
        """Check 3: 최대 낙폭(Drawdown) 체크."""
        max_dd_threshold = self.config.trading.max_drawdown
        current_dd = (self._peak_value - self._portfolio_value) / max(self._peak_value, 1.0)

        if current_dd >= max_dd_threshold:
            return RiskValidation(
                check_name="max_drawdown",
                passed=False,
                value=round(current_dd * 100, 2),
                threshold=round(max_dd_threshold * 100, 2),
                detail=f"Drawdown {current_dd*100:.2f}% >= limit {max_dd_threshold*100:.1f}%",
            )
        return RiskValidation(
            check_name="max_drawdown",
            passed=True,
            value=round(current_dd * 100, 2),
            threshold=round(max_dd_threshold * 100, 2),
        )

    def _check_04_position_limit(self) -> RiskValidation:
        """Check 4: 최대 동시 포지션 수 제한."""
        current = len(self._open_positions)
        max_pos = self.config.trading.max_open_positions

        if current >= max_pos:
            return RiskValidation(
                check_name="position_limit",
                passed=False,
                value=float(current),
                threshold=float(max_pos),
                detail=f"Open positions {current} >= max {max_pos}",
            )
        return RiskValidation(
            check_name="position_limit",
            passed=True,
            value=float(current),
            threshold=float(max_pos),
        )

    def _check_05_single_asset_exposure(self, signal: TradeSignal | None) -> RiskValidation:
        """Check 5: 단일 자산 노출 한도 (포트폴리오의 20% 이하)."""
        max_single_exposure = self.config.trading.max_single_exposure
        if not signal:
            return RiskValidation(check_name="single_asset_exposure", passed=True, detail="No signal")

        existing_exposure = 0.0
        if signal.symbol in self._open_positions:
            pos = self._open_positions[signal.symbol]
            existing_exposure = pos.get("size_usd", 0.0) / max(self._portfolio_value, 1.0)

        if existing_exposure >= max_single_exposure:
            return RiskValidation(
                check_name="single_asset_exposure",
                passed=False,
                value=round(existing_exposure * 100, 2),
                threshold=round(max_single_exposure * 100, 2),
                detail=f"{signal.symbol} exposure {existing_exposure*100:.1f}% >= {max_single_exposure*100:.0f}%",
            )
        return RiskValidation(
            check_name="single_asset_exposure",
            passed=True,
            value=round(existing_exposure * 100, 2),
            threshold=round(max_single_exposure * 100, 2),
        )

    def _check_06_risk_reward_ratio(self, signal: TradeSignal | None) -> RiskValidation:
        """Check 6: 최소 Risk:Reward 비율 충족 여부."""
        min_rr = self.config.trading.min_risk_reward
        if not signal:
            return RiskValidation(check_name="risk_reward_ratio", passed=True, detail="No signal")

        rr = signal.risk_reward_ratio
        if rr < min_rr:
            return RiskValidation(
                check_name="risk_reward_ratio",
                passed=False,
                value=round(rr, 2),
                threshold=min_rr,
                detail=f"RR {rr:.2f} < min {min_rr:.1f}",
            )
        return RiskValidation(
            check_name="risk_reward_ratio",
            passed=True,
            value=round(rr, 2),
            threshold=min_rr,
        )

    def _check_07_leverage_limit(self, payload: dict[str, Any]) -> RiskValidation:
        """Check 7: 레버리지 한도 초과 여부."""
        max_lev = self.config.trading.max_leverage
        requested = payload.get("leverage", self.config.trading.default_leverage)

        if requested > max_lev:
            return RiskValidation(
                check_name="leverage_limit",
                passed=False,
                value=requested,
                threshold=max_lev,
                detail=f"Leverage {requested}x > max {max_lev}x",
            )
        return RiskValidation(
            check_name="leverage_limit",
            passed=True,
            value=requested,
            threshold=max_lev,
        )

    def _check_08_consecutive_loss_limit(self) -> RiskValidation:
        """Check 8: 연속 손실 횟수 제한 (5회 이상이면 거래 중단)."""
        max_consecutive = self.config.trading.max_consecutive_losses
        if self._consecutive_losses >= max_consecutive:
            return RiskValidation(
                check_name="consecutive_loss_limit",
                passed=False,
                value=float(self._consecutive_losses),
                threshold=float(max_consecutive),
                detail=f"Consecutive losses {self._consecutive_losses} >= {max_consecutive}",
            )
        return RiskValidation(
            check_name="consecutive_loss_limit",
            passed=True,
            value=float(self._consecutive_losses),
            threshold=float(max_consecutive),
        )

    def _check_09_daily_trade_count(self) -> RiskValidation:
        """Check 9: 일일 최대 거래 횟수 제한."""
        max_daily = self.config.trading.max_daily_trades
        if self._trades_today >= max_daily:
            return RiskValidation(
                check_name="daily_trade_count",
                passed=False,
                value=float(self._trades_today),
                threshold=float(max_daily),
                detail=f"Daily trades {self._trades_today} >= max {max_daily}",
            )
        return RiskValidation(
            check_name="daily_trade_count",
            passed=True,
            value=float(self._trades_today),
            threshold=float(max_daily),
        )

    def _check_10_portfolio_heat(self) -> RiskValidation:
        """Check 10: 포트폴리오 히트 (전체 오픈 포지션의 합산 리스크 비율)."""
        max_heat = self.config.trading.max_portfolio_heat
        total_risk = sum(
            pos.get("risk_pct", 0.0) for pos in self._open_positions.values()
        )
        if total_risk >= max_heat:
            return RiskValidation(
                check_name="portfolio_heat",
                passed=False,
                value=round(total_risk * 100, 2),
                threshold=round(max_heat * 100, 2),
                detail=f"Portfolio heat {total_risk*100:.2f}% >= {max_heat*100:.0f}%",
            )
        return RiskValidation(
            check_name="portfolio_heat",
            passed=True,
            value=round(total_risk * 100, 2),
            threshold=round(max_heat * 100, 2),
        )

    def _check_11_liquidation_risk(
        self, signal: TradeSignal | None, payload: dict[str, Any]
    ) -> RiskValidation:
        """Check 11: 청산 리스크 — SL이 청산가보다 먼저 트리거되는지 확인."""
        if not signal:
            return RiskValidation(check_name="liquidation_risk", passed=True, detail="No signal")

        leverage = payload.get("leverage", self.config.trading.default_leverage)
        entry = signal.entry_price
        sl = signal.stop_loss

        # Liquidation price estimation (simplified)
        if signal.direction == Direction.LONG:
            liq_price = entry * (1 - 1.0 / leverage) if leverage > 0 else 0
            sl_before_liq = sl > liq_price
        else:
            liq_price = entry * (1 + 1.0 / leverage) if leverage > 0 else float("inf")
            sl_before_liq = sl < liq_price

        if not sl_before_liq:
            return RiskValidation(
                check_name="liquidation_risk",
                passed=False,
                value=round(liq_price, 2),
                threshold=round(sl, 2),
                detail=f"SL {sl} does not trigger before liquidation at {liq_price:.2f}",
            )
        return RiskValidation(
            check_name="liquidation_risk",
            passed=True,
            value=round(liq_price, 2),
            threshold=round(sl, 2),
            detail="SL triggers before liquidation",
        )

    # ── Position Sizing ────────────────────────

    def _calculate_position_size(self, signal: TradeSignal) -> float:
        """Kelly-fraction-inspired position sizing."""
        risk_per_trade = self.config.trading.max_risk_per_trade
        risk_amount = self._portfolio_value * risk_per_trade

        entry = signal.entry_price
        sl = signal.stop_loss
        sl_distance_pct = abs(entry - sl) / entry if entry > 0 else 1.0

        if sl_distance_pct <= 0:
            return 0.0

        position_size = risk_amount / sl_distance_pct
        max_single = self._portfolio_value * self.config.trading.max_single_exposure
        return round(min(position_size, max_single), 2)

    # ── Risk Score Computation ─────────────────

    def _compute_risk_score(self, validations: list[RiskValidation]) -> float:
        """Compute aggregate risk score 0-100 (higher = riskier)."""
        if not validations:
            return 0.0
        failed = sum(1 for v in validations if not v.passed)
        return round((failed / len(validations)) * 100, 2)

    # ── Circuit Breaker ────────────────────────

    def _activate_circuit_breaker(self, reason: str) -> None:
        """Activate circuit breaker — halts all trading."""
        if self._circuit_breaker_active:
            return
        self._circuit_breaker_active = True
        self._circuit_breaker_activated_at = time.time()
        self.logger.critical(f"[CRCO] CIRCUIT BREAKER ACTIVATED: {reason}")

        # Fire-and-forget broadcast (will be picked up by dispatcher)
        asyncio.ensure_future(self._broadcast_circuit_breaker(reason))

    async def _broadcast_circuit_breaker(self, reason: str) -> None:
        """Broadcast circuit breaker to all agents."""
        await self.publish(
            topic="crco.circuit_breaker",
            payload={
                "active": True,
                "reason": reason,
                "activated_at": datetime.now(timezone.utc).isoformat(),
                "cooldown_seconds": self.config.trading.circuit_breaker_cooldown,
            },
            priority=Priority.URGENT,
        )
        await self.audit_log("circuit_breaker_activated", {"reason": reason})
        await self._emit_alert(
            AlertSeverity.FATAL,
            "Circuit Breaker Activated",
            reason,
        )

    # ── Order Event Handlers ───────────────────

    async def _handle_order_filled(self, message: Message) -> None:
        """Track opened positions for risk calculations."""
        p = message.payload
        symbol = p.get("symbol", "")
        self._open_positions[symbol] = {
            "signal_id": p.get("signal_id", ""),
            "direction": p.get("direction", ""),
            "entry_price": p.get("filled_price", 0.0),
            "quantity": p.get("filled_quantity", 0.0),
            "size_usd": p.get("filled_price", 0.0) * p.get("filled_quantity", 0.0),
            "risk_pct": self.config.trading.max_risk_per_trade,
            "opened_at": p.get("timestamp", ""),
        }
        self._trades_today += 1
        await self.audit_log("position_opened", {"symbol": symbol, "position": self._open_positions[symbol]})

    async def _handle_order_closed(self, message: Message) -> None:
        """Update portfolio state when a position is closed."""
        p = message.payload
        symbol = p.get("symbol", "")
        pnl = p.get("pnl", 0.0)

        # Update portfolio
        self._daily_pnl += pnl
        self._portfolio_value += pnl
        self._peak_value = max(self._peak_value, self._portfolio_value)

        # Track consecutive losses
        if pnl < 0:
            self._consecutive_losses += 1
        else:
            self._consecutive_losses = 0

        # Remove from open positions
        self._open_positions.pop(symbol, None)

        # Store in history
        self._trade_history.append(TradeRecord(
            signal_id=p.get("signal_id", ""),
            symbol=symbol,
            direction=Direction(p.get("direction", "LONG")),
            entry_price=p.get("entry_price", 0.0),
            exit_price=p.get("exit_price", 0.0),
            stop_loss=p.get("stop_loss", 0.0),
            take_profit_1=p.get("take_profit_1", 0.0),
            quantity=p.get("quantity", 0.0),
            pnl=pnl,
            pnl_percent=p.get("pnl_percent", 0.0),
        ))

        await self.audit_log("position_closed", {
            "symbol": symbol,
            "pnl": pnl,
            "daily_pnl": self._daily_pnl,
            "portfolio_value": self._portfolio_value,
            "consecutive_losses": self._consecutive_losses,
        })

        # Check if circuit breaker should trigger
        daily_loss_pct = abs(min(0.0, self._daily_pnl)) / max(self._daily_start_value, 1.0)
        if daily_loss_pct >= self.config.trading.circuit_breaker_loss:
            self._activate_circuit_breaker(
                f"Daily loss {daily_loss_pct*100:.2f}% exceeded circuit breaker threshold "
                f"{self.config.trading.circuit_breaker_loss*100:.1f}%"
            )

    async def _handle_order_failed(self, message: Message) -> None:
        """Log failed orders."""
        await self.audit_log("order_failed_observed", message.payload)

    # ── Daily Reset ────────────────────────────

    def _check_daily_reset(self) -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._last_daily_reset != today:
            self._reset_daily_stats()

    def _reset_daily_stats(self) -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self._daily_pnl = 0.0
        self._daily_start_value = self._portfolio_value
        self._trades_today = 0
        self._last_daily_reset = today

    async def _daily_reset_loop(self) -> None:
        """Periodically check for daily reset."""
        while self._running:
            self._check_daily_reset()
            await asyncio.sleep(60)

    # ── External State Setters (for API control) ──

    def update_portfolio_value(self, value: float) -> None:
        self._portfolio_value = value
        self._peak_value = max(self._peak_value, value)

    def set_circuit_breaker(self, active: bool) -> None:
        if active and not self._circuit_breaker_active:
            self._activate_circuit_breaker("manual_activation")
        elif not active:
            self._circuit_breaker_active = False

    # ── 3-Stage Live Trading Safety Gate ─────────

    def run_pre_live_checklist(self) -> dict[str, Any]:
        """
        3-stage safety gate that MUST pass before live trading is allowed.

        Gate 1 (Configuration): Env vars, API keys, risk parameters
        Gate 2 (System State): Circuit breaker off, no open positions, agents healthy
        Gate 3 (Risk Parameters): Conservative limits enforced for live

        Returns: {"passed": bool, "gates": [...], "blockers": [...]}
        """
        gates: list[dict[str, Any]] = []
        blockers: list[str] = []

        # ── Gate 1: Configuration Validation ──
        g1_checks: list[tuple[str, bool, str]] = []

        has_exchange_keys = bool(
            self.config.exchange.binance_api_key or self.config.exchange.bybit_api_key
        )
        g1_checks.append(("exchange_api_keys", has_exchange_keys,
                          "At least one exchange API key must be configured"))

        mode_is_live = self.config.trading.mode == "LIVE"
        g1_checks.append(("trade_mode_live", mode_is_live,
                          "TRADE_MODE must be set to LIVE"))

        risk_sane = self.config.trading.max_risk_per_trade <= 0.02
        g1_checks.append(("risk_per_trade_sane", risk_sane,
                          f"max_risk_per_trade={self.config.trading.max_risk_per_trade} must be <= 2%"))

        leverage_sane = self.config.trading.max_leverage <= 20.0
        g1_checks.append(("leverage_sane", leverage_sane,
                          f"max_leverage={self.config.trading.max_leverage} must be <= 20x"))

        daily_loss_sane = self.config.trading.max_daily_loss <= 0.05
        g1_checks.append(("daily_loss_sane", daily_loss_sane,
                          f"max_daily_loss={self.config.trading.max_daily_loss} must be <= 5%"))

        g1_passed = all(ok for _, ok, _ in g1_checks)
        for name, ok, msg in g1_checks:
            if not ok:
                blockers.append(f"[Gate1] {msg}")
        gates.append({"gate": 1, "name": "Configuration", "passed": g1_passed,
                      "checks": [{"name": n, "passed": p, "detail": d} for n, p, d in g1_checks]})

        # ── Gate 2: System State ──
        g2_checks: list[tuple[str, bool, str]] = []

        g2_checks.append(("circuit_breaker_off", not self._circuit_breaker_active,
                          "Circuit breaker must be inactive"))

        no_positions = len(self._open_positions) == 0
        g2_checks.append(("no_open_positions", no_positions,
                          f"Must have 0 open positions, currently {len(self._open_positions)}"))

        no_daily_loss = self._daily_pnl >= 0
        g2_checks.append(("no_daily_loss", no_daily_loss,
                          f"Daily PnL must be >= 0 before going live, currently {self._daily_pnl}"))

        g2_passed = all(ok for _, ok, _ in g2_checks)
        for name, ok, msg in g2_checks:
            if not ok:
                blockers.append(f"[Gate2] {msg}")
        gates.append({"gate": 2, "name": "System State", "passed": g2_passed,
                      "checks": [{"name": n, "passed": p, "detail": d} for n, p, d in g2_checks]})

        # ── Gate 3: Risk Parameters Hardened ──
        g3_checks: list[tuple[str, bool, str]] = []

        cb_set = self.config.trading.circuit_breaker_loss <= 0.05
        g3_checks.append(("circuit_breaker_threshold", cb_set,
                          f"Circuit breaker must be <= 5%, currently {self.config.trading.circuit_breaker_loss*100}%"))

        max_pos = self.config.trading.max_open_positions <= 10
        g3_checks.append(("max_positions_sane", max_pos,
                          f"Max positions must be <= 10, currently {self.config.trading.max_open_positions}"))

        min_rr = self.config.trading.min_risk_reward >= 1.5
        g3_checks.append(("min_rr_sane", min_rr,
                          f"Min RR must be >= 1.5, currently {self.config.trading.min_risk_reward}"))

        g3_passed = all(ok for _, ok, _ in g3_checks)
        for name, ok, msg in g3_checks:
            if not ok:
                blockers.append(f"[Gate3] {msg}")
        gates.append({"gate": 3, "name": "Risk Parameters", "passed": g3_passed,
                      "checks": [{"name": n, "passed": p, "detail": d} for n, p, d in g3_checks]})

        all_passed = g1_passed and g2_passed and g3_passed
        return {
            "passed": all_passed,
            "gates": gates,
            "blockers": blockers,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    @property
    def risk_snapshot(self) -> dict[str, Any]:
        """Current risk state for monitoring/API."""
        daily_loss_pct = abs(min(0.0, self._daily_pnl)) / max(self._daily_start_value, 1.0)
        drawdown = (self._peak_value - self._portfolio_value) / max(self._peak_value, 1.0)
        return {
            "portfolio_value": self._portfolio_value,
            "daily_pnl": self._daily_pnl,
            "daily_loss_pct": round(daily_loss_pct * 100, 2),
            "drawdown_pct": round(drawdown * 100, 2),
            "peak_value": self._peak_value,
            "open_positions": len(self._open_positions),
            "consecutive_losses": self._consecutive_losses,
            "trades_today": self._trades_today,
            "circuit_breaker_active": self._circuit_breaker_active,
            "portfolio_heat": round(
                sum(p.get("risk_pct", 0) for p in self._open_positions.values()) * 100, 2
            ),
        }
