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
        """Gather market data — uses exchange OHLCV if available, else cached/injected."""
        cached = self._market_data_cache.get(symbol, {})
        price = cached.get("price", 0.0)

        # Fetch real OHLCV from exchange if available
        ohlcv_data: dict[str, list[dict[str, float]]] = {}
        if self._exchange and self._exchange.is_connected:
            ccxt_symbol = self._to_ccxt_symbol(symbol)
            for tf in ["1d", "4h", "1h", "15m"]:
                try:
                    candles = await asyncio.wait_for(
                        self._exchange.fetch_ohlcv(ccxt_symbol, tf, limit=50),
                        timeout=10.0,
                    )
                    ohlcv_data[tf] = [
                        {"t": c.timestamp, "o": c.open, "h": c.high,
                         "l": c.low, "c": c.close, "v": c.volume}
                        for c in candles
                    ]
                    if candles and price <= 0:
                        price = candles[-1].close
                except Exception as e:
                    self.logger.debug(f"[MIA] OHLCV fetch failed {symbol}/{tf}: {e}")

        # Compute SMC indicators from real candles
        smc = self._compute_smc_from_ohlcv(ohlcv_data)

        data: dict[str, Any] = {
            "symbol": symbol,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "price": price,
            "ohlcv": ohlcv_data,
            "smc": smc,
            "sentiment": cached.get("sentiment", {
                "score": 0.0, "source": "aggregated",
                "headlines": [], "fear_greed_index": 50,
            }),
            "macro": cached.get("macro", {
                "fed_rate_decision_near": False, "cpi_release_near": False,
                "geopolitical_risk": "LOW", "dxy_trend": "NEUTRAL",
            }),
            "onchain": cached.get("onchain", {
                "funding_rate": 0.0, "open_interest_change": 0.0,
                "whale_flow": "NEUTRAL", "exchange_netflow": 0.0,
            }),
            "psychology": cached.get("psychology", {
                "fomo_level": 0.0, "fud_level": 0.0, "retail_positioning": "NEUTRAL",
            }),
        }
        self._market_data_cache[symbol] = data
        return data

    def _to_ccxt_symbol(self, symbol: str) -> str:
        """Convert internal symbol (BTCUSDT) to CCXT futures format (BTC/USDT:USDT)."""
        if "/" in symbol:
            return symbol
        base = symbol.replace("USDT", "")
        return f"{base}/USDT:USDT"

    def _compute_smc_from_ohlcv(self, ohlcv: dict[str, list[dict[str, float]]]) -> dict[str, Any]:
        """Compute SMC/ICT indicators from real OHLCV candle data."""
        result: dict[str, Any] = {
            "order_blocks": [],
            "fair_value_gaps": [],
            "liquidity_zones": [],
            "break_of_structure": False,
            "change_of_character": False,
        }
        # Use highest-resolution available data
        candles = ohlcv.get("1h") or ohlcv.get("4h") or ohlcv.get("1d") or []
        if len(candles) < 5:
            return result

        # Detect swing highs/lows (simplified 3-bar pivot)
        swing_highs: list[float] = []
        swing_lows: list[float] = []
        for i in range(1, len(candles) - 1):
            if candles[i]["h"] > candles[i-1]["h"] and candles[i]["h"] > candles[i+1]["h"]:
                swing_highs.append(candles[i]["h"])
            if candles[i]["l"] < candles[i-1]["l"] and candles[i]["l"] < candles[i+1]["l"]:
                swing_lows.append(candles[i]["l"])

        # BOS: price breaks above most recent swing high (bullish) or below swing low (bearish)
        if swing_highs and swing_lows:
            last_close = candles[-1]["c"]
            recent_high = swing_highs[-1]
            recent_low = swing_lows[-1]
            if last_close > recent_high:
                result["break_of_structure"] = True
            elif last_close < recent_low:
                result["break_of_structure"] = True

        # CHOCH: direction reversal — higher low after downtrend or lower high after uptrend
        if len(swing_lows) >= 2 and len(swing_highs) >= 2:
            if swing_lows[-1] > swing_lows[-2] and swing_highs[-1] < swing_highs[-2]:
                result["change_of_character"] = True
            elif swing_lows[-1] < swing_lows[-2] and swing_highs[-1] > swing_highs[-2]:
                result["change_of_character"] = True

        # FVG: gap between candle[i-1].high and candle[i+1].low (bullish)
        for i in range(1, len(candles) - 1):
            gap_up = candles[i+1]["l"] - candles[i-1]["h"]
            gap_down = candles[i-1]["l"] - candles[i+1]["h"]
            if gap_up > 0:
                result["fair_value_gaps"].append({
                    "type": "bullish", "low": candles[i-1]["h"], "high": candles[i+1]["l"],
                })
            elif gap_down > 0:
                result["fair_value_gaps"].append({
                    "type": "bearish", "low": candles[i+1]["h"], "high": candles[i-1]["l"],
                })

        # Order blocks: last bearish candle before a bullish BOS (simplified)
        for i in range(2, len(candles)):
            if candles[i]["c"] > candles[i-1]["h"] and candles[i-1]["c"] < candles[i-1]["o"]:
                result["order_blocks"].append({
                    "type": "bullish", "price": candles[i-1]["l"],
                })
            elif candles[i]["c"] < candles[i-1]["l"] and candles[i-1]["c"] > candles[i-1]["o"]:
                result["order_blocks"].append({
                    "type": "bearish", "price": candles[i-1]["h"],
                })

        # Liquidity zones: cluster of swing highs/lows
        if swing_highs:
            result["liquidity_zones"].append({"type": "sell_side", "price": max(swing_highs)})
        if swing_lows:
            result["liquidity_zones"].append({"type": "buy_side", "price": min(swing_lows)})

        return result

    # ── SMC/ICT Structure Analysis ─────────────

    def _analyze_structure(
        self, symbol: str, tf: Timeframe, data: dict[str, Any]
    ) -> MarketStructureReport:
        """SMC/ICT market structure analysis — computes trend from real OHLCV."""
        smc = data.get("smc", {})
        ohlcv = data.get("ohlcv", {})

        # Compute trend from candle data
        trend_direction, trend_strength = self._compute_trend(ohlcv, tf)
        bos = smc.get("break_of_structure", False)
        choch = smc.get("change_of_character", False)

        return MarketStructureReport(
            symbol=symbol,
            timeframe=tf,
            regime=self._detect_regime_single(data),
            trend_direction=trend_direction,
            trend_strength=trend_strength,
            key_levels=self._extract_key_levels(ohlcv, tf),
            order_blocks=smc.get("order_blocks", []),
            fair_value_gaps=smc.get("fair_value_gaps", []),
            liquidity_zones=smc.get("liquidity_zones", []),
            break_of_structure=bos,
            change_of_character=choch,
        )

    def _compute_trend(
        self, ohlcv: dict[str, list[dict[str, float]]], tf: Timeframe
    ) -> tuple[Direction, float]:
        """Compute trend direction and strength from OHLCV candles."""
        tf_key = tf.value  # "1d", "4h", "1h", "15m"
        candles = ohlcv.get(tf_key, [])
        if len(candles) < 10:
            return Direction.NEUTRAL, 0.5

        # Simple EMA crossover: fast(8) vs slow(21) on close prices
        closes = [c["c"] for c in candles]
        ema_fast = self._ema(closes, min(8, len(closes)))
        ema_slow = self._ema(closes, min(21, len(closes)))

        if ema_fast <= 0 or ema_slow <= 0:
            return Direction.NEUTRAL, 0.5

        spread = (ema_fast - ema_slow) / ema_slow
        if spread > 0.001:
            direction = Direction.LONG
        elif spread < -0.001:
            direction = Direction.SHORT
        else:
            direction = Direction.NEUTRAL

        strength = min(1.0, abs(spread) * 50)  # Normalize to 0-1
        return direction, round(max(0.1, strength), 2)

    @staticmethod
    def _ema(values: list[float], period: int) -> float:
        """Compute EMA of the last `period` values."""
        if not values or period <= 0:
            return 0.0
        k = 2.0 / (period + 1)
        ema = values[0]
        for v in values[1:]:
            ema = v * k + ema * (1 - k)
        return ema

    def _extract_key_levels(
        self, ohlcv: dict[str, list[dict[str, float]]], tf: Timeframe
    ) -> list[float]:
        """Extract support/resistance from swing highs/lows."""
        candles = ohlcv.get(tf.value, [])
        if len(candles) < 5:
            return []
        levels: list[float] = []
        for i in range(1, len(candles) - 1):
            if candles[i]["h"] > candles[i-1]["h"] and candles[i]["h"] > candles[i+1]["h"]:
                levels.append(round(candles[i]["h"], 2))
            if candles[i]["l"] < candles[i-1]["l"] and candles[i]["l"] < candles[i+1]["l"]:
                levels.append(round(candles[i]["l"], 2))
        return sorted(set(levels))[-10:]  # Keep top 10

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
        # Compute ATR from candles if available, else use config default
        volatility_pct = self._compute_atr_pct(data.get("ohlcv", {}))

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

    def _compute_atr_pct(self, ohlcv: dict[str, list[dict[str, float]]]) -> float:
        """Compute ATR as percentage from candle data, fallback to config default."""
        candles = ohlcv.get("1h") or ohlcv.get("4h") or []
        if len(candles) < 14:
            return self.config.trading.default_volatility_pct
        trs: list[float] = []
        for i in range(1, len(candles)):
            h, l, pc = candles[i]["h"], candles[i]["l"], candles[i-1]["c"]
            tr = max(h - l, abs(h - pc), abs(l - pc))
            trs.append(tr)
        atr = sum(trs[-14:]) / 14
        mid = candles[-1]["c"]
        if mid <= 0:
            return self.config.trading.default_volatility_pct
        return max(0.005, min(0.10, atr / mid))  # Clamp 0.5%-10%

    async def _handle_circuit_breaker(self, message: Message) -> None:
        active = message.payload.get("active", True)
        if active:
            self.logger.warning("[MIA] Circuit breaker received, pausing signal generation")
            self._analysis_interval = self.config.trading.analysis_interval * 5
        else:
            self._analysis_interval = self.config.trading.analysis_interval

    # ── External API ───────────────────────────

    def set_exchange(self, exchange: Any) -> None:
        """Set exchange for fetching real OHLCV data."""
        self._exchange = exchange

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
