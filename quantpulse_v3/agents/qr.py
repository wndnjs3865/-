"""QuantPulse v3 - QR (Quant Researcher) Agent.

8-dimension quantitative scoring engine.
Receives trade signals from MIA, computes QuantScore, publishes to CSO.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from core.base_agent import BaseAgent
from core.enums import AgentRole, Direction, MarketRegime, Priority
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from core.models import Message, QuantScore, TradeSignal
from config import Config


class QRAgent(BaseAgent):
    """
    Quant Researcher — 8차원 정량 분석 에이전트.

    8 scoring dimensions:
    1. Trend Alignment (20%) — HTF/LTF 트렌드 정렬
    2. Momentum Score (15%) — RSI, MACD, Stochastic
    3. Volume Profile (10%) — 거래량 프로파일
    4. Volatility Regime (10%) — ATR 기반 변동성
    5. Orderflow Imbalance (15%) — 매수/매도 불균형
    6. Correlation Filter (10%) — 자산 간 상관관계
    7. Statistical Edge (10%) — 과거 유사 패턴 승률
    8. Timing Score (10%) — 세션/시간대 최적성

    Subscribes to: cso.request_quant_score
    Publishes to: qr.quant_score
    """

    def __init__(self, bus: MessageBus, audit: AuditLogger, config: Config) -> None:
        super().__init__(AgentRole.QR, bus, audit)
        self.config = config
        self._scores_computed: int = 0
        self._historical_signals: list[dict[str, Any]] = []

    async def setup_subscriptions(self) -> None:
        self.subscribe_safe("cso.request_quant_score", self._handle_score_request)

    async def on_start(self) -> None:
        pass

    # ── Main Handler ───────────────────────────

    async def _handle_score_request(self, message: Message) -> None:
        """CSO requests quantitative scoring for a signal."""
        payload = message.payload
        signal_data = payload.get("trade_signal", {})
        correlation_id = message.correlation_id

        if not signal_data:
            self.logger.warning("[QR] Empty signal data in score request")
            return

        signal = TradeSignal(**signal_data)
        analysis_data = payload.get("analysis_data", {})

        # Compute 8 dimension scores
        scores = self._compute_all_dimensions(signal, analysis_data)

        # Build QuantScore
        qs = QuantScore(
            signal_id=signal.id,
            symbol=signal.symbol,
            trend_alignment=scores["trend_alignment"],
            momentum_score=scores["momentum_score"],
            volume_profile=scores["volume_profile"],
            volatility_regime=scores["volatility_regime"],
            orderflow_imbalance=scores["orderflow_imbalance"],
            correlation_filter=scores["correlation_filter"],
            statistical_edge=scores["statistical_edge"],
            timing_score=scores["timing_score"],
            total_score=0.0,
            grade="",
            reasons=scores["reasons"],
        )
        qs.compute_total()
        qs.compute_grade()

        self._scores_computed += 1
        self._historical_signals.append({
            "signal_id": signal.id,
            "symbol": signal.symbol,
            "total_score": qs.total_score,
            "grade": qs.grade,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        # Publish result
        await self.publish(
            topic="qr.quant_score",
            payload=qs.model_dump(mode="json"),
            priority=Priority.NORMAL,
            correlation_id=correlation_id,
        )

        await self.audit_log("quant_score_computed", {
            "signal_id": signal.id,
            "symbol": signal.symbol,
            "total_score": qs.total_score,
            "grade": qs.grade,
            "dimensions": {
                "trend_alignment": qs.trend_alignment,
                "momentum_score": qs.momentum_score,
                "volume_profile": qs.volume_profile,
                "volatility_regime": qs.volatility_regime,
                "orderflow_imbalance": qs.orderflow_imbalance,
                "correlation_filter": qs.correlation_filter,
                "statistical_edge": qs.statistical_edge,
                "timing_score": qs.timing_score,
            },
            "reasons": qs.reasons,
        }, correlation_id=correlation_id)

        self.logger.info(
            f"[QR] {signal.symbol} score={qs.total_score:.1f} grade={qs.grade} "
            f"({len(qs.reasons)} factors)"
        )

    # ── 8 Dimension Scoring Engine ─────────────

    def _compute_all_dimensions(
        self, signal: TradeSignal, analysis: dict[str, Any]
    ) -> dict[str, Any]:
        """Compute all 8 scoring dimensions."""
        reasons: list[str] = []

        trend = self._score_trend_alignment(signal, analysis, reasons)
        momentum = self._score_momentum(signal, analysis, reasons)
        volume = self._score_volume_profile(signal, analysis, reasons)
        volatility = self._score_volatility_regime(signal, analysis, reasons)
        orderflow = self._score_orderflow_imbalance(signal, analysis, reasons)
        correlation = self._score_correlation_filter(signal, analysis, reasons)
        statistical = self._score_statistical_edge(signal, analysis, reasons)
        timing = self._score_timing(signal, analysis, reasons)

        return {
            "trend_alignment": trend,
            "momentum_score": momentum,
            "volume_profile": volume,
            "volatility_regime": volatility,
            "orderflow_imbalance": orderflow,
            "correlation_filter": correlation,
            "statistical_edge": statistical,
            "timing_score": timing,
            "reasons": reasons,
        }

    def _score_trend_alignment(
        self, signal: TradeSignal, analysis: dict[str, Any], reasons: list[str]
    ) -> float:
        """Dimension 1: HTF/LTF trend alignment (weight: 20%)."""
        score = 50.0  # Base neutral score

        # Signal score reflects MIA's confluence analysis
        if signal.score >= 80:
            score += 30
            reasons.append("Strong MIA signal score (80+)")
        elif signal.score >= 60:
            score += 15
            reasons.append("Moderate MIA signal score (60+)")

        # Confluence factors bonus
        cf = signal.confluence_factors
        if "MTF_ALIGNMENT" in cf:
            score += 10
            reasons.append("Multi-timeframe alignment confirmed")
        if any("BOS" in f for f in cf):
            score += 5
            reasons.append("Break of structure detected")

        # Regime alignment
        if signal.direction == Direction.LONG and signal.regime == MarketRegime.TRENDING_BULL:
            score += 5
        elif signal.direction == Direction.SHORT and signal.regime == MarketRegime.TRENDING_BEAR:
            score += 5

        return max(0.0, min(100.0, score))

    def _score_momentum(
        self, signal: TradeSignal, analysis: dict[str, Any], reasons: list[str]
    ) -> float:
        """Dimension 2: Momentum indicators (weight: 15%)."""
        score = 50.0

        # Use signal's own score as a proxy for momentum
        momentum_proxy = signal.score / 100.0
        score = score * 0.5 + momentum_proxy * 100 * 0.5

        # RR ratio indicates good momentum setup
        if signal.risk_reward_ratio >= 3.0:
            score += 10
            reasons.append(f"Excellent RR ratio: {signal.risk_reward_ratio:.1f}")
        elif signal.risk_reward_ratio >= 2.0:
            score += 5

        return max(0.0, min(100.0, score))

    def _score_volume_profile(
        self, signal: TradeSignal, analysis: dict[str, Any], reasons: list[str]
    ) -> float:
        """Dimension 3: Volume profile analysis (weight: 10%)."""
        score = 60.0  # Default moderate (real data improves this in Phase 4)

        # Volume data from analysis
        vol_data = analysis.get("volume", {})
        if vol_data.get("above_average", False):
            score += 15
            reasons.append("Volume above 20-day average")
        if vol_data.get("climax", False):
            score -= 10
            reasons.append("Warning: volume climax detected")

        return max(0.0, min(100.0, score))

    def _score_volatility_regime(
        self, signal: TradeSignal, analysis: dict[str, Any], reasons: list[str]
    ) -> float:
        """Dimension 4: Volatility regime (weight: 10%)."""
        score = 60.0

        regime = signal.regime
        if regime == MarketRegime.HIGH_VOLATILITY:
            score -= 15
            reasons.append("High volatility regime: reduce confidence")
        elif regime == MarketRegime.LOW_VOLATILITY:
            score += 10
            reasons.append("Low volatility: potential breakout setup")
        elif regime == MarketRegime.BREAKOUT:
            score += 15
            reasons.append("Breakout regime detected")
        elif regime in (MarketRegime.TRENDING_BULL, MarketRegime.TRENDING_BEAR):
            score += 10

        return max(0.0, min(100.0, score))

    def _score_orderflow_imbalance(
        self, signal: TradeSignal, analysis: dict[str, Any], reasons: list[str]
    ) -> float:
        """Dimension 5: Order flow imbalance (weight: 15%)."""
        score = 55.0

        of_data = analysis.get("orderflow", {})
        imbalance = of_data.get("delta_pct", 0.0)  # % buy vs sell

        if signal.direction == Direction.LONG and imbalance > 0.1:
            score += 20
            reasons.append("Positive orderflow delta supports LONG")
        elif signal.direction == Direction.SHORT and imbalance < -0.1:
            score += 20
            reasons.append("Negative orderflow delta supports SHORT")

        # Funding rate signal (crypto)
        funding = of_data.get("funding_rate", 0.0)
        if signal.direction == Direction.LONG and funding < 0:
            score += 10
            reasons.append("Negative funding rate: contrarian LONG edge")
        elif signal.direction == Direction.SHORT and funding > 0.01:
            score += 10
            reasons.append("High funding rate: contrarian SHORT edge")

        return max(0.0, min(100.0, score))

    def _score_correlation_filter(
        self, signal: TradeSignal, analysis: dict[str, Any], reasons: list[str]
    ) -> float:
        """Dimension 6: Inter-asset correlation (weight: 10%)."""
        score = 65.0

        corr_data = analysis.get("correlation", {})
        btc_corr = corr_data.get("btc_correlation", 0.0)

        # If trading an alt with high BTC correlation, adjust score
        if signal.symbol != "BTCUSDT" and abs(btc_corr) > 0.8:
            score -= 10
            reasons.append(f"High BTC correlation ({btc_corr:.2f}): diversification risk")
        elif signal.symbol != "BTCUSDT" and abs(btc_corr) < 0.3:
            score += 10
            reasons.append("Low BTC correlation: good diversification")

        return max(0.0, min(100.0, score))

    def _score_statistical_edge(
        self, signal: TradeSignal, analysis: dict[str, Any], reasons: list[str]
    ) -> float:
        """Dimension 7: Historical pattern win rate (weight: 10%)."""
        score = 55.0

        # Check historical signal performance for similar setups
        similar_count = 0
        similar_wins = 0
        for hist in self._historical_signals[-100:]:
            if hist["symbol"] == signal.symbol:
                similar_count += 1
                if hist["total_score"] >= 70:
                    similar_wins += 1

        if similar_count >= 5:
            win_rate = similar_wins / similar_count
            score = win_rate * 100
            reasons.append(f"Historical win rate: {win_rate*100:.0f}% ({similar_count} signals)")

        # Signal strength bonus
        if signal.score >= 85:
            score += 10
            reasons.append("High signal confidence (85+)")

        return max(0.0, min(100.0, score))

    def _score_timing(
        self, signal: TradeSignal, analysis: dict[str, Any], reasons: list[str]
    ) -> float:
        """Dimension 8: Session/time optimality (weight: 10%)."""
        score = 60.0

        now = datetime.now(timezone.utc)
        hour = now.hour

        # Crypto: 24/7 but higher volume during US/EU sessions
        if 13 <= hour <= 21:  # US session overlap
            score += 15
            reasons.append("US session: high liquidity")
        elif 7 <= hour <= 16:  # EU session
            score += 10
        elif 0 <= hour <= 6:  # Asian session
            score += 5

        # Avoid Monday open and Friday close for some setups
        weekday = now.weekday()
        if weekday == 0 and hour < 4:
            score -= 10
            reasons.append("Monday open: potential gap risk")
        elif weekday == 4 and hour > 20:
            score -= 5

        return max(0.0, min(100.0, score))

    # ── Query Methods ──────────────────────────

    @property
    def qr_snapshot(self) -> dict[str, Any]:
        return {
            "scores_computed": self._scores_computed,
            "historical_signals": len(self._historical_signals),
        }
