"""
Technical Indicators Engine
============================
Quant-grade 기술적 지표 계산 엔진.
모든 주요 지표를 벡터화된 연산으로 고속 처리.

Indicators:
- Trend: SMA, EMA, MACD, ADX, Supertrend
- Momentum: RSI, Stochastic, CCI, Williams %R, ROC
- Volatility: Bollinger Bands, ATR, Keltner Channel
- Volume: OBV, VWAP, MFI, A/D Line
- Custom: Z-Score, Hurst Exponent, Mean Reversion Score
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


class TechnicalIndicators:
    """High-performance technical indicator calculator."""

    # ========================
    # Trend Indicators
    # ========================

    @staticmethod
    def sma(series: pd.Series, period: int = 20) -> pd.Series:
        """Simple Moving Average."""
        return series.rolling(window=period, min_periods=1).mean()

    @staticmethod
    def ema(series: pd.Series, period: int = 20) -> pd.Series:
        """Exponential Moving Average."""
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def macd(
        close: pd.Series,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        """
        MACD (Moving Average Convergence Divergence).
        Returns: (macd_line, signal_line, histogram)
        """
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def adx(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 14,
    ) -> pd.Series:
        """Average Directional Index - trend strength."""
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()

        up_move = high - high.shift(1)
        down_move = low.shift(1) - low

        plus_dm = pd.Series(
            np.where((up_move > down_move) & (up_move > 0), up_move, 0),
            index=close.index,
        )
        minus_dm = pd.Series(
            np.where((down_move > up_move) & (down_move > 0), down_move, 0),
            index=close.index,
        )

        plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(period).mean() / atr)

        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.rolling(window=period).mean()
        return adx

    @staticmethod
    def supertrend(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 10,
        multiplier: float = 3.0,
    ) -> tuple[pd.Series, pd.Series]:
        """
        Supertrend indicator.
        Returns: (supertrend_line, direction) where direction: 1=bullish, -1=bearish
        """
        hl2 = (high + low) / 2
        atr = TechnicalIndicators.atr(high, low, close, period)

        upper = hl2 + multiplier * atr
        lower = hl2 - multiplier * atr

        supertrend = pd.Series(index=close.index, dtype=float)
        direction = pd.Series(index=close.index, dtype=float)

        supertrend.iloc[0] = upper.iloc[0]
        direction.iloc[0] = 1

        for i in range(1, len(close)):
            if close.iloc[i] > supertrend.iloc[i - 1]:
                supertrend.iloc[i] = max(lower.iloc[i], supertrend.iloc[i - 1]) if direction.iloc[i - 1] == 1 else lower.iloc[i]
                direction.iloc[i] = 1
            else:
                supertrend.iloc[i] = min(upper.iloc[i], supertrend.iloc[i - 1]) if direction.iloc[i - 1] == -1 else upper.iloc[i]
                direction.iloc[i] = -1

        return supertrend, direction

    # ========================
    # Momentum Indicators
    # ========================

    @staticmethod
    def rsi(close: pd.Series, period: int = 14) -> pd.Series:
        """Relative Strength Index."""
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()

        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(50)

    @staticmethod
    def stochastic(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        k_period: int = 14,
        d_period: int = 3,
    ) -> tuple[pd.Series, pd.Series]:
        """
        Stochastic Oscillator.
        Returns: (%K, %D)
        """
        lowest_low = low.rolling(window=k_period).min()
        highest_high = high.rolling(window=k_period).max()

        k_line = 100 * (close - lowest_low) / (highest_high - lowest_low).replace(0, np.nan)
        d_line = k_line.rolling(window=d_period).mean()
        return k_line.fillna(50), d_line.fillna(50)

    @staticmethod
    def cci(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 20,
    ) -> pd.Series:
        """Commodity Channel Index."""
        tp = (high + low + close) / 3
        sma = tp.rolling(window=period).mean()
        mad = tp.rolling(window=period).apply(lambda x: np.abs(x - x.mean()).mean())
        cci = (tp - sma) / (0.015 * mad)
        return cci.fillna(0)

    @staticmethod
    def williams_r(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 14,
    ) -> pd.Series:
        """Williams %R."""
        highest_high = high.rolling(window=period).max()
        lowest_low = low.rolling(window=period).min()
        wr = -100 * (highest_high - close) / (highest_high - lowest_low).replace(0, np.nan)
        return wr.fillna(-50)

    @staticmethod
    def roc(close: pd.Series, period: int = 12) -> pd.Series:
        """Rate of Change."""
        return close.pct_change(periods=period) * 100

    # ========================
    # Volatility Indicators
    # ========================

    @staticmethod
    def bollinger_bands(
        close: pd.Series,
        period: int = 20,
        std_dev: float = 2.0,
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        """
        Bollinger Bands.
        Returns: (upper, middle, lower)
        """
        middle = close.rolling(window=period).mean()
        std = close.rolling(window=period).std()
        upper = middle + std_dev * std
        lower = middle - std_dev * std
        return upper, middle, lower

    @staticmethod
    def atr(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 14,
    ) -> pd.Series:
        """Average True Range."""
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(window=period).mean()

    @staticmethod
    def keltner_channel(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        ema_period: int = 20,
        atr_period: int = 10,
        multiplier: float = 2.0,
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        """
        Keltner Channel.
        Returns: (upper, middle, lower)
        """
        middle = close.ewm(span=ema_period, adjust=False).mean()
        atr_val = TechnicalIndicators.atr(high, low, close, atr_period)
        upper = middle + multiplier * atr_val
        lower = middle - multiplier * atr_val
        return upper, middle, lower

    @staticmethod
    def bollinger_bandwidth(close: pd.Series, period: int = 20, std_dev: float = 2.0) -> pd.Series:
        """Bollinger Bandwidth - volatility squeeze detection."""
        upper, middle, lower = TechnicalIndicators.bollinger_bands(close, period, std_dev)
        return ((upper - lower) / middle) * 100

    # ========================
    # Volume Indicators
    # ========================

    @staticmethod
    def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
        """On-Balance Volume."""
        direction = pd.Series(
            np.where(close > close.shift(1), 1, np.where(close < close.shift(1), -1, 0)),
            index=close.index,
        )
        return (direction * volume).cumsum()

    @staticmethod
    def vwap(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        volume: pd.Series,
    ) -> pd.Series:
        """Volume Weighted Average Price."""
        tp = (high + low + close) / 3
        return (tp * volume).cumsum() / volume.cumsum()

    @staticmethod
    def mfi(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        volume: pd.Series,
        period: int = 14,
    ) -> pd.Series:
        """Money Flow Index."""
        tp = (high + low + close) / 3
        mf = tp * volume

        pos_mf = pd.Series(
            np.where(tp > tp.shift(1), mf, 0), index=close.index
        )
        neg_mf = pd.Series(
            np.where(tp < tp.shift(1), mf, 0), index=close.index
        )

        pos_sum = pos_mf.rolling(window=period).sum()
        neg_sum = neg_mf.rolling(window=period).sum()

        mr = pos_sum / neg_sum.replace(0, np.nan)
        mfi = 100 - (100 / (1 + mr))
        return mfi.fillna(50)

    @staticmethod
    def ad_line(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        volume: pd.Series,
    ) -> pd.Series:
        """Accumulation/Distribution Line."""
        hl_range = (high - low).replace(0, np.nan)
        clv = ((close - low) - (high - close)) / hl_range
        ad = (clv * volume).cumsum()
        return ad.fillna(0)

    # ========================
    # Custom / Quant Indicators
    # ========================

    @staticmethod
    def zscore(series: pd.Series, period: int = 20) -> pd.Series:
        """Z-Score - mean reversion signal."""
        mean = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        return ((series - mean) / std.replace(0, np.nan)).fillna(0)

    @staticmethod
    def hurst_exponent(series: pd.Series, max_lag: int = 20) -> float:
        """
        Hurst Exponent - trend vs mean reversion detection.
        H > 0.5: trending, H < 0.5: mean reverting, H = 0.5: random walk
        """
        lags = range(2, max_lag)
        tau = [np.sqrt(np.std(np.subtract(series.values[lag:], series.values[:-lag])))
               for lag in lags]

        if not all(t > 0 for t in tau):
            return 0.5

        poly = np.polyfit(np.log(list(lags)), np.log(tau), 1)
        return poly[0] * 2.0

    @staticmethod
    def mean_reversion_score(close: pd.Series, period: int = 20) -> pd.Series:
        """
        Combined mean reversion score.
        Combines Z-score, RSI deviation, and Bollinger %B.
        Range: -1 (oversold/buy) to +1 (overbought/sell)
        """
        z = TechnicalIndicators.zscore(close, period)
        rsi = TechnicalIndicators.rsi(close, period)
        rsi_norm = (rsi - 50) / 50  # Normalize to [-1, 1]

        upper, middle, lower = TechnicalIndicators.bollinger_bands(close, period)
        bb_range = (upper - lower).replace(0, np.nan)
        pct_b = ((close - lower) / bb_range).fillna(0.5)
        bb_norm = (pct_b - 0.5) * 2  # Normalize to [-1, 1]

        score = (z * 0.4 + rsi_norm * 0.3 + bb_norm * 0.3).clip(-1, 1)
        return score

    @staticmethod
    def momentum_score(close: pd.Series, volume: pd.Series) -> pd.Series:
        """
        Composite momentum score.
        Combines multiple timeframe momentum + volume confirmation.
        Range: -1 (bearish) to +1 (bullish)
        """
        # Multi-timeframe returns
        ret_5 = close.pct_change(5)
        ret_10 = close.pct_change(10)
        ret_20 = close.pct_change(20)

        # Rank-normalize returns
        def rank_norm(s):
            return s.rank(pct=True) * 2 - 1

        mom = rank_norm(ret_5) * 0.4 + rank_norm(ret_10) * 0.35 + rank_norm(ret_20) * 0.25

        # Volume confirmation
        vol_sma = volume.rolling(20).mean()
        vol_ratio = (volume / vol_sma.replace(0, np.nan)).fillna(1)
        vol_factor = vol_ratio.clip(0.5, 2.0) / 2.0

        return (mom * vol_factor).clip(-1, 1)

    @staticmethod
    def calculate_all(df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate all indicators at once.
        Input: DataFrame with columns [open, high, low, close, volume]
        Output: DataFrame with all indicator columns added
        """
        result = df.copy()
        c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
        ti = TechnicalIndicators

        # Trend
        result["sma_20"] = ti.sma(c, 20)
        result["sma_50"] = ti.sma(c, 50)
        result["sma_200"] = ti.sma(c, 200)
        result["ema_12"] = ti.ema(c, 12)
        result["ema_26"] = ti.ema(c, 26)
        result["macd"], result["macd_signal"], result["macd_hist"] = ti.macd(c)
        result["adx"] = ti.adx(h, l, c)

        # Momentum
        result["rsi_14"] = ti.rsi(c, 14)
        result["stoch_k"], result["stoch_d"] = ti.stochastic(h, l, c)
        result["cci"] = ti.cci(h, l, c)
        result["williams_r"] = ti.williams_r(h, l, c)
        result["roc_12"] = ti.roc(c, 12)

        # Volatility
        result["bb_upper"], result["bb_middle"], result["bb_lower"] = ti.bollinger_bands(c)
        result["atr_14"] = ti.atr(h, l, c)
        result["bb_bandwidth"] = ti.bollinger_bandwidth(c)

        # Volume
        result["obv"] = ti.obv(c, v)
        result["vwap"] = ti.vwap(h, l, c, v)
        result["mfi"] = ti.mfi(h, l, c, v)

        # Custom Quant
        result["zscore"] = ti.zscore(c)
        result["mean_rev_score"] = ti.mean_reversion_score(c)
        result["momentum_score"] = ti.momentum_score(c, v)

        return result
