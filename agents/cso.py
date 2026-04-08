"""QuantPulse v3 - CSO (Chief Strategy Officer) Agent.

The orchestrator / supervisor. Coordinates the full pipeline:
MIA signal → QR scoring → CRCO risk assessment → ES execution.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from core.base_agent import BaseAgent
from core.enums import (
    AgentRole, Direction, OrderType, Priority, TradeMode,
)
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from core.models import (
    Message, QuantScore, RiskAssessment, TradeDecision, TradeSignal,
)
from config import Config


class CSOAgent(BaseAgent):
    """
    Chief Strategy Officer — 시스템 핵심 오케스트레이터.

    매 신호 수신 시 정확한 순서:
    1. MIA의 mia.trade_signal 수신
    2. QR에게 QuantScore 요청 (cso.request_quant_score → qr.quant_score)
    3. CRCO에게 RiskAssessment 요청 (cso.request_risk_assessment → crco.risk_assessment)
    4. CRCO approved=True인 경우에만 TradeDecision 생성
    5. cso.trade_decision → ES에게 전달

    모든 단계에서 AuditLog에 기록.
    CRCO veto는 절대 무시 불가.
    """

    def __init__(self, bus: MessageBus, audit: AuditLogger, config: Config) -> None:
        super().__init__(AgentRole.CSO, bus, audit)
        self.config = config

        # Pipeline state
        self._pending_signals: dict[str, TradeSignal] = {}
        self._decisions_made: int = 0
        self._decisions_approved: int = 0
        self._decisions_vetoed: int = 0
        self._circuit_breaker_active: bool = False

    async def setup_subscriptions(self) -> None:
        self.subscribe_safe("mia.trade_signal", self._handle_trade_signal)
        self.subscribe_safe("crco.circuit_breaker", self._handle_circuit_breaker)

    async def on_start(self) -> None:
        pass

    # ── Main Pipeline ──────────────────────────

    async def _handle_trade_signal(self, message: Message) -> None:
        """MIA로부터 TradeSignal 수신 → 파이프라인을 별도 태스크로 실행.

        Important: 핸들러 안에서 request()를 호출하면 MessageBus dispatcher가
        블로킹되어 데드락이 발생한다. 따라서 파이프라인을 create_task로 분리한다.
        """
        signal = TradeSignal(**message.payload)

        if self._circuit_breaker_active:
            self.logger.warning(f"[CSO] Circuit breaker active, ignoring signal {signal.id}")
            await self.audit_log("signal_ignored_circuit_breaker", {"signal_id": signal.id})
            return

        # Spawn pipeline as background task to avoid blocking the bus dispatcher
        self.create_task(
            self._run_pipeline(signal),
            name=f"cso_pipeline_{signal.id}",
        )

    async def _run_pipeline(self, signal: TradeSignal) -> None:
        """Full pipeline: QR scoring → CRCO risk check → decision."""
        self._pending_signals[signal.id] = signal
        await self.audit_log("signal_received", {
            "signal_id": signal.id,
            "symbol": signal.symbol,
            "direction": signal.direction.value,
            "score": signal.score,
        })

        self.logger.info(
            f"[CSO] Signal received: {signal.symbol} {signal.direction.value} "
            f"score={signal.score:.1f} — starting pipeline"
        )

        # ── Step 1: Request QR QuantScore ──
        quant_score = await self._request_quant_score(signal)
        if not quant_score:
            await self.audit_log("pipeline_abort", {
                "signal_id": signal.id, "reason": "qr_timeout",
            })
            self._pending_signals.pop(signal.id, None)
            return

        # Filter: minimum quant score check
        if quant_score.total_score < self.config.trading.min_quant_score:
            await self.audit_log("signal_rejected_low_score", {
                "signal_id": signal.id,
                "quant_score": quant_score.total_score,
                "min_required": self.config.trading.min_quant_score,
            })
            self.logger.info(
                f"[CSO] Signal {signal.id} rejected: quant_score "
                f"{quant_score.total_score:.1f} < min {self.config.trading.min_quant_score}"
            )
            self._pending_signals.pop(signal.id, None)
            return

        # ── Step 2: Request CRCO RiskAssessment ──
        risk = await self._request_risk_assessment(signal)
        if not risk:
            await self.audit_log("pipeline_abort", {
                "signal_id": signal.id, "reason": "crco_timeout",
            })
            self._pending_signals.pop(signal.id, None)
            return

        self._decisions_made += 1

        # ── Step 3: CRCO approved? ──
        if not risk.approved:
            self._decisions_vetoed += 1
            await self.audit_log("trade_vetoed_by_crco", {
                "signal_id": signal.id,
                "veto_reasons": risk.veto_reasons,
                "risk_score": risk.risk_score,
            })
            self.logger.info(
                f"[CSO] Signal {signal.id} VETOED by CRCO: {risk.veto_reasons}"
            )
            self._pending_signals.pop(signal.id, None)
            return

        # ── Step 4: Generate TradeDecision ──
        decision = self._build_decision(signal, quant_score, risk)
        self._decisions_approved += 1

        await self.audit_log("trade_decision_approved", {
            "decision_id": decision.decision_id,
            "signal_id": signal.id,
            "symbol": signal.symbol,
            "direction": signal.direction.value,
            "position_size": decision.position_size,
            "leverage": decision.leverage,
            "quant_score": quant_score.total_score,
            "quant_grade": quant_score.grade,
            "risk_score": risk.risk_score,
        })

        # ── Step 5: Send to ES ──
        await self.publish(
            topic="cso.trade_decision",
            payload=decision.model_dump(mode="json"),
            priority=Priority.URGENT,
        )

        self.logger.info(
            f"[CSO] DECISION APPROVED: {signal.symbol} {signal.direction.value} "
            f"size={decision.position_size} lev={decision.leverage}x "
            f"QR={quant_score.total_score:.1f}({quant_score.grade}) "
            f"risk={risk.risk_score:.1f}"
        )
        self._pending_signals.pop(signal.id, None)

    # ── QR Request ─────────────────────────────

    async def _request_quant_score(self, signal: TradeSignal) -> QuantScore | None:
        """Request quantitative scoring from QR agent."""
        response = await self.request(
            topic="cso.request_quant_score",
            payload={
                "signal_id": signal.id,
                "trade_signal": signal.model_dump(mode="json"),
                "analysis_data": {},  # MIA analysis data could be attached here
            },
            response_topic="qr.quant_score",
            timeout=15.0,
        )
        if response:
            return QuantScore(**response.payload)
        self.logger.warning(f"[CSO] QR timeout for signal {signal.id}")
        return None

    # ── CRCO Request ───────────────────────────

    async def _request_risk_assessment(self, signal: TradeSignal) -> RiskAssessment | None:
        """Request risk assessment from CRCO agent (mandatory, cannot skip)."""
        response = await self.request(
            topic="cso.request_risk_assessment",
            payload={
                "signal_id": signal.id,
                "trade_signal": signal.model_dump(mode="json"),
                "request_type": "full_risk_assessment",
            },
            response_topic="crco.risk_assessment",
            timeout=15.0,
        )
        if response:
            return RiskAssessment(**response.payload)
        self.logger.warning(f"[CSO] CRCO timeout for signal {signal.id}")
        return None

    # ── Decision Builder ───────────────────────

    def _build_decision(
        self,
        signal: TradeSignal,
        quant_score: QuantScore,
        risk: RiskAssessment,
    ) -> TradeDecision:
        """Build final TradeDecision from signal + QR + CRCO data."""
        # Position size from CRCO recommendation
        position_size = risk.max_position_size
        leverage = risk.recommended_leverage

        # Trade mode from config
        trade_mode = TradeMode(self.config.trading.mode)

        return TradeDecision(
            signal_id=signal.id,
            symbol=signal.symbol,
            direction=signal.direction,
            approved=True,
            entry_price=signal.entry_price,
            stop_loss=signal.stop_loss,
            take_profit_1=signal.take_profit_1,
            take_profit_2=signal.take_profit_2,
            take_profit_3=signal.take_profit_3,
            position_size=position_size,
            leverage=leverage,
            order_type=OrderType.LIMIT,
            trade_mode=trade_mode,
            quant_score=quant_score.total_score,
            quant_grade=quant_score.grade,
            risk_score=risk.risk_score,
            reasoning=(
                f"QR={quant_score.total_score:.1f}({quant_score.grade}) "
                f"Risk={risk.risk_score:.1f} "
                f"RR={signal.risk_reward_ratio:.2f} "
                f"Factors: {', '.join(signal.confluence_factors[:5])}"
            ),
        )

    # ── Circuit Breaker ────────────────────────

    async def _handle_circuit_breaker(self, message: Message) -> None:
        active = message.payload.get("active", True)
        self._circuit_breaker_active = active
        if active:
            self.logger.critical("[CSO] Circuit breaker received, halting all pipelines")
            self._pending_signals.clear()
        else:
            self.logger.info("[CSO] Circuit breaker cleared, resuming")

    # ── Query ──────────────────────────────────

    @property
    def cso_snapshot(self) -> dict[str, Any]:
        return {
            "decisions_made": self._decisions_made,
            "decisions_approved": self._decisions_approved,
            "decisions_vetoed": self._decisions_vetoed,
            "approval_rate": round(
                self._decisions_approved / max(self._decisions_made, 1) * 100, 2
            ),
            "pending_signals": len(self._pending_signals),
            "circuit_breaker_active": self._circuit_breaker_active,
        }
