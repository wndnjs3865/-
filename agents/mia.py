"""QuantPulse v3 - MIA (Market Intelligence Analyst) Agent.

Multi-source intelligence gathering + SMC/ICT analysis + signal generation.
Analyzes: chart structure, order blocks, FVG, liquidity, news sentiment,
macro indicators, on-chain data, and trader psychology.
"""

from __future__ import annotations

import asyncio
import math
import random
import uuid
from datetime import datetime, timezone
from typing import Any

from core.base_agent import BaseAgent
from core.enums import (
    AgentRole, Direction, MarketRegime, Priority, Timeframe,
)
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from core.models import (
    MarketStructureReport, Message, MultiTimeframeAnalysis,
    TimeframeAnalysis, TradeSignal,
)
from config import Config

# Timeframes for multi-timeframe analysis (HTF → LTF)
MTF_CHAIN: list[Timeframe] = [Timeframe.D1, Timeframe.H4, Timeframe.H1, Timeframe.M15]


class MIAAgent(BaseAgent):
    """
    Market Intelligence Analyst — 시장 분석의 최전선.

    Multi-source intelligence:
    - SMC/ICT 구조 분석 (BOS, CHOCH, OrderBlock, FVG, Liquidity Sweep)
    - Multi-Timeframe 분석 (D1 → H4 → H1 → M15)
    - 뉴스 센티먼트 + 거시경제 지표
    - On-chain data (funding rate, whale movement)
    - 트레이더 심리 (FOMO/FUD indicator)

    Publishes:
    - mia.mtf_analysis: 멀티타임프레임 종합 분석
    - mia.trade_signal: 거래 신호 (CSO/QR에게 전달)
    """

    def __init__(self, bus: MessageBus, audit: AuditLogger, config: Config) -> None:
        super().__init__(AgentRole.MIA, bus, audit)
        self.config = config
        self._watchlist: list[str] = ["BTCUSDT", "ETHUSDT"]
        self._analysis_interval: float = config.trading.analysis_interval
        self._latest_analyses: dict[str, MultiTimeframeAnalysis] = {}
        self._market_data_cache: dict[str, dict[str, Any]] = {}
        self._signal_count: int = 0
        # Exchange reference for fetching real OHLCV (injected by JWQuantSystem)
        self._exchange: Any | None = None

    async def setup_subscriptions(self) -> None:
        self.subscribe_safe("crco.circuit_breaker", self._handle_circuit_breaker)

    async def on_start(self) -> None:
        self.create_task(self._analysis_loop(), name="mia_analysis_loop")

    # ── Main Analysis Loop ─────────────────────

    async def _analysis_loop(self) -> None:
        """Periodic analysis cycle for all watchlist symbols."""
        while self._running:
            for symbol in self._watchlist:
                try:
                    await self._analyze_symbol(symbol)
                except Exception as e:
                    self.logger.error(f"[MIA] Analysis failed for {symbol}: {e}")
                    await self.audit_log("analysis_error", {"symbol": symbol, "error": str(e)})
            await asyncio.sleep(self._analysis_interval)

    async def _analyze_symbol(self, symbol: str) -> None:
        """Full multi-source analysis pipeline for a single symbol."""

        # ── Step 1: Gather multi-source intelligence ──
        market_data = await self._gather_market_data(symbol)

        # ── Step 2: SMC/ICT structure analysis per timeframe ──
        tf_analyses: list[TimeframeAnalysis] = []
        for tf in MTF_CHAIN:
            structure = self._analyze_structure(symbol, tf, market_data)
            bias = self._determine_bias(structure)
            confidence = self._compute_confidence(structure, market_data)
            tf_analyses.append(TimeframeAnalysis(
                timeframe=tf,
                structure=structure,
                bias=bias,
                confidence=confidence,
            ))

        # ── Step 3: Multi-timeframe confluence ──
        htf_bias = tf_analyses[0].bias  # D1 bias is HTF
        ltf_bias = tf_analyses[-1].bias  # M15 is LTF
        ltf_confirmation = htf_bias == ltf_bias and htf_bias != Direction.NEUTRAL
        confluence_score = self._compute_confluence(tf_analyses, market_data)
        regime = self._detect_regime(tf_analyses, market_data)

        mtf = MultiTimeframeAnalysis(
            symbol=symbol,
            analyses=tf_analyses,
            htf_bias=htf_bias,
            ltf_confirmation=ltf_confirmation,
            confluence_score=confluence_score,
            regime=regime,
        )
        self._latest_analyses[symbol] = mtf

        # ── Step 4: Publish MTF analysis ──
        await self.publish(
            topic="mia.mtf_analysis",
            payload=mtf.model_dump(mode="json"),
            priority=Priority.HIGH,
        )
        await self.audit_log("mtf_analysis_published", {
            "symbol": symbol,
            "htf_bias": htf_bias.value,
            "ltf_confirmation": ltf_confirmation,
            "confluence": confluence_score,
            "regime": regime.value,
        })

        # ── Step 5: Generate trade signal if conditions met ──
        if self._should_generate_signal(mtf, market_data):
            signal = self._generate_signal(mtf, market_data)
            if signal:
                await self.publish(
                    topic="mia.trade_signal",
                    payload=signal.model_dump(mode="json"),
                    priority=Priority.HIGH,
                )
                await self.audit_log("trade_signal_generated", {
                    "signal_id": signal.id,
                    "symbol": signal.symbol,
                    "direction": signal.direction.value,
                    "score": signal.score,
                    "entry": signal.entry_price,
                    "sl": signal.stop_loss,
                    "tp1": signal.take_profit_1,
                    "rr": signal.risk_reward_ratio,
                    "confluence_factors": signal.confluence_factors,
                })
                self._signal_count += 1
                self.logger.info(
                    f"[MIA] SIGNAL: {signal.symbol} {signal.direction.value} "
                    f"score={signal.score:.1f} RR={signal.risk_reward_ratio:.2f} "
                    f"entry={signal.entry_price} sl={signal.stop_loss} tp1={signal.take_profit_1}"
                )

    # ── Multi-Source Data Gathering ────────────

    async def _gather_market_data(self, symbol: str) -> dict[str, Any]:
        """
        Gather intelligence from multiple sources.
        In production, each section calls real APIs.
        Currently returns structured placeholders that the analysis engine can consume.
        """
        data: dict[str, Any] = {
            "symbol": symbol,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            # Price data (would come from exchange websocket/REST)
            "price": self._market_data_cache.get(symbol, {}).get("price", 0.0),
            "ohlcv": {},  # tf → list of candles (Phase 4: exchange integration)
            # SMC/ICT indicators (computed from price data)
            "smc": {
                "order_blocks": [],
                "fair_value_gaps": [],
                "liquidity_zones": [],
                "break_of_structure": False,
                "change_of_character": False,
            },
            # News & sentiment (would come from news API)
            "sentiment": {
                "score": 0.0,       # -1.0 (extreme fear) to 1.0 (extreme greed)
                "source": "aggregated",
                "headlines": [],
                "fear_greed_index": 50,
            },
            # Macro indicators
            "macro": {
                "fed_rate_decision_near": False,
                "cpi_release_near": False,
                "geopolitical_risk": "LOW",
                "dxy_trend": "NEUTRAL",
            },
            # On-chain data (crypto-specific)
            "onchain": {
                "funding_rate": 0.0,
                "open_interest_change": 0.0,
                "whale_flow": "NEUTRAL",
                "exchange_netflow": 0.0,
            },
            # Trader psychology
            "psychology": {
                "fomo_level": 0.0,   # 0-1
                "fud_level": 0.0,    # 0-1
                "retail_positioning": "NEUTRAL",
            },
        }
        self._market_data_cache[symbol] = data
        return data

    # ── SMC/ICT Structure Analysis ─────────────

    def _analyze_structure(
        self, symbol: str, tf: Timeframe, data: dict[str, Any]
    ) -> MarketStructureReport:
        """
        SMC/ICT market structure analysis for a single timeframe.
        Detects: trend, BOS, CHOCH, order blocks, FVG, liquidity zones.
        """
        smc = data.get("smc", {})
        sentiment = data.get("sentiment", {})
        onchain = data.get("onchain", {})

        # In production, these are computed from actual OHLCV data.
        # The structure is ready for real data integration.
        trend_direction = Direction.NEUTRAL
        trend_strength = 0.5
        bos = smc.get("break_of_structure", False)
        choch = smc.get("change_of_character", False)

        return MarketStructureReport(
            symbol=symbol,
            timeframe=tf,
            regime=self._detect_regime_single(data),
            trend_direction=trend_direction,
            trend_strength=trend_strength,
            key_levels=self._extract_key_levels(symbol, tf, data),
            order_blocks=smc.get("order_blocks", []),
            fair_value_gaps=smc.get("fair_value_gaps", []),
            liquidity_zones=smc.get("liquidity_zones", []),
            break_of_structure=bos,
            change_of_character=choch,
        )

    def _extract_key_levels(
        self, symbol: str, tf: Timeframe, data: dict[str, Any]
    ) -> list[float]:
        """Extract support/resistance key levels."""
        # Phase 4: compute from actual OHLCV data using swing high/low detection
        return []

    # ── Bias & Confidence ──────────────────────

    def _determine_bias(self, structure: MarketStructureReport) -> Direction:
        """Determine directional bias from market structure."""
        if structure.trend_strength >= 0.6:
            return structure.trend_direction
        if structure.break_of_structure:
            return structure.trend_direction
        if structure.change_of_character:
            # CHOCH suggests reversal
            if structure.trend_direction == Direction.LONG:
                return Direction.SHORT
            elif structure.trend_direction == Direction.SHORT:
                return Direction.LONG
        return Direction.NEUTRAL

    def _compute_confidence(
        self, structure: MarketStructureReport, data: dict[str, Any]
    ) -> float:
        """Compute confidence score combining structure + external factors."""
        base = structure.trend_strength

        # Sentiment adjustment
        sentiment_score = data.get("sentiment", {}).get("score", 0.0)
        if structure.trend_direction == Direction.LONG and sentiment_score > 0:
            base += 0.05
        elif structure.trend_direction == Direction.SHORT and sentiment_score < 0:
            base += 0.05

        # On-chain confirmation
        funding = data.get("onchain", {}).get("funding_rate", 0.0)
        if structure.trend_direction == Direction.LONG and funding < 0.01:
            base += 0.03  # Not overheated
        elif structure.trend_direction == Direction.SHORT and funding > -0.01:
            base += 0.03

        # Macro headwind penalty
        macro = data.get("macro", {})
        if macro.get("fed_rate_decision_near") or macro.get("cpi_release_near"):
            base -= 0.10  # Reduce confidence near major events

        return max(0.0, min(1.0, base))

    # ── Confluence & Regime ─────────────────────

    def _compute_confluence(
        self, analyses: list[TimeframeAnalysis], data: dict[str, Any]
    ) -> float:
        """Compute multi-timeframe confluence score."""
        if not analyses:
            return 0.0

        htf_bias = analyses[0].bias
        aligned = sum(1 for a in analyses if a.bias == htf_bias and htf_bias != Direction.NEUTRAL)
        structural_score = aligned / len(analyses)

        # Bonus for BOS/CHOCH alignment
        structure_bonus = 0.0
        for a in analyses:
            if a.structure.break_of_structure:
                structure_bonus += 0.05
            if a.structure.change_of_character:
                structure_bonus += 0.03

        # Sentiment alignment bonus
        sentiment = data.get("sentiment", {}).get("score", 0.0)
        sentiment_bonus = 0.0
        if htf_bias == Direction.LONG and sentiment > 0.3:
            sentiment_bonus = 0.05
        elif htf_bias == Direction.SHORT and sentiment < -0.3:
            sentiment_bonus = 0.05

        return min(1.0, structural_score + structure_bonus + sentiment_bonus)

    def _detect_regime(
        self, analyses: list[TimeframeAnalysis], data: dict[str, Any]
    ) -> MarketRegime:
        """Detect market regime from multi-source data."""
        return self._detect_regime_single(data)

    def _detect_regime_single(self, data: dict[str, Any]) -> MarketRegime:
        """Single-source regime detection."""
        sentiment = data.get("sentiment", {})
        fgi = sentiment.get("fear_greed_index", 50)
        macro = data.get("macro", {})

        if macro.get("geopolitical_risk") == "HIGH":
            return MarketRegime.HIGH_VOLATILITY
        if fgi > 75:
            return MarketRegime.TRENDING_BULL
        if fgi < 25:
            return MarketRegime.TRENDING_BEAR
        return MarketRegime.RANGING

    # ── Signal Generation ──────────────────────

    def _should_generate_signal(
        self, mtf: MultiTimeframeAnalysis, data: dict[str, Any]
    ) -> bool:
        """Determine if conditions warrant a trade signal."""
        # Require HTF bias not neutral + LTF confirmation + minimum confluence
        if mtf.htf_bias == Direction.NEUTRAL:
            return False
        if not mtf.ltf_confirmation:
            return False
        if mtf.confluence_score < 0.5:
            return False
        # Don't trade during high-impact macro events
        macro = data.get("macro", {})
        if macro.get("fed_rate_decision_near") or macro.get("cpi_release_near"):
            return False
        return True

    def _generate_signal(
        self, mtf: MultiTimeframeAnalysis, data: dict[str, Any]
    ) -> TradeSignal | None:
        """Generate a TradeSignal from the analysis."""
        price = data.get("price", 0.0)
        if price <= 0:
            return None

        direction = mtf.htf_bias
        # Use LTF structure for entry precision
        ltf = mtf.analyses[-1] if mtf.analyses else None
        if not ltf:
            return None

        # SL/TP calculation based on ATR-like volatility estimation
        volatility_pct = 0.02  # 2% default (Phase 4: compute from real ATR)

        if direction == Direction.LONG:
            entry = price
            sl = round(entry * (1 - volatility_pct), 8)
            tp1 = round(entry * (1 + volatility_pct * 2), 8)
            tp2 = round(entry * (1 + volatility_pct * 3), 8)
            tp3 = round(entry * (1 + volatility_pct * 5), 8)
        elif direction == Direction.SHORT:
            entry = price
            sl = round(entry * (1 + volatility_pct), 8)
            tp1 = round(entry * (1 - volatility_pct * 2), 8)
            tp2 = round(entry * (1 - volatility_pct * 3), 8)
            tp3 = round(entry * (1 - volatility_pct * 5), 8)
        else:
            return None

        risk = abs(entry - sl)
        reward = abs(tp1 - entry)
        rr = round(reward / risk, 2) if risk > 0 else 0.0

        # Confluence factors
        factors = self._collect_confluence_factors(mtf, data)

        # Score based on confluence + confidence
        score = round(mtf.confluence_score * 100 * 0.7 + ltf.confidence * 100 * 0.3, 1)
        score = max(0.0, min(100.0, score))

        return TradeSignal(
            symbol=mtf.symbol,
            direction=direction,
            score=score,
            entry_price=entry,
            stop_loss=sl,
            take_profit_1=tp1,
            take_profit_2=tp2,
            take_profit_3=tp3,
            timeframe=ltf.timeframe,
            regime=mtf.regime,
            risk_reward_ratio=rr,
            confluence_factors=factors,
            analysis_summary=(
                f"{mtf.symbol} {direction.value} | HTF={mtf.htf_bias.value} "
                f"LTF_conf={mtf.ltf_confirmation} | "
                f"Confluence={mtf.confluence_score:.2f} Regime={mtf.regime.value}"
            ),
        )

    def _collect_confluence_factors(
        self, mtf: MultiTimeframeAnalysis, data: dict[str, Any]
    ) -> list[str]:
        """Collect all active confluence factors for the signal."""
        factors: list[str] = []
        if mtf.ltf_confirmation:
            factors.append("MTF_ALIGNMENT")
        for a in mtf.analyses:
            if a.structure.break_of_structure:
                factors.append(f"BOS_{a.timeframe.value}")
            if a.structure.change_of_character:
                factors.append(f"CHOCH_{a.timeframe.value}")
            if a.structure.order_blocks:
                factors.append(f"OB_{a.timeframe.value}")
            if a.structure.fair_value_gaps:
                factors.append(f"FVG_{a.timeframe.value}")
            if a.structure.liquidity_zones:
                factors.append(f"LIQ_{a.timeframe.value}")
        sentiment = data.get("sentiment", {}).get("score", 0.0)
        if abs(sentiment) > 0.3:
            factors.append("SENTIMENT_ALIGNED" if sentiment > 0 else "SENTIMENT_BEARISH")
        funding = data.get("onchain", {}).get("funding_rate", 0.0)
        if abs(funding) > 0.005:
            factors.append("FUNDING_SIGNAL")
        return factors

    # ── Circuit Breaker ────────────────────────

    async def _handle_circuit_breaker(self, message: Message) -> None:
        active = message.payload.get("active", True)
        if active:
            self.logger.warning("[MIA] Circuit breaker received, pausing signal generation")
            self._analysis_interval = 300.0  # Slow down to 5min
        else:
            self._analysis_interval = 60.0

    # ── External API ───────────────────────────

    def set_watchlist(self, symbols: list[str]) -> None:
        self._watchlist = symbols

    def inject_price(self, symbol: str, price: float) -> None:
        """Inject price data for testing/paper mode."""
        if symbol not in self._market_data_cache:
            self._market_data_cache[symbol] = {}
        self._market_data_cache[symbol]["price"] = price

    @property
    def analysis_snapshot(self) -> dict[str, Any]:
        return {
            "watchlist": self._watchlist,
            "signal_count": self._signal_count,
            "analysis_interval": self._analysis_interval,
            "latest_analyses": {
                sym: {
                    "htf_bias": a.htf_bias.value,
                    "ltf_confirmation": a.ltf_confirmation,
                    "confluence": a.confluence_score,
                    "regime": a.regime.value,
                }
                for sym, a in self._latest_analyses.items()
            },
        }
