"""
Utility Functions
=================
공통 유틸리티 함수들.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd


def format_currency(value: float, currency: str = "KRW") -> str:
    """Format currency value."""
    if currency == "KRW":
        if abs(value) >= 1_0000_0000:
            return f"{value / 1_0000_0000:,.1f}억원"
        elif abs(value) >= 1_0000:
            return f"{value / 1_0000:,.0f}만원"
        return f"{value:,.0f}원"
    elif currency == "USD":
        return f"${value:,.2f}"
    return f"{value:,.2f} {currency}"


def format_pct(value: float, decimals: int = 2) -> str:
    """Format percentage value."""
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.{decimals}f}%"


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safe division with default value."""
    if denominator == 0 or np.isnan(denominator):
        return default
    return numerator / denominator


def calculate_returns(prices: pd.Series, method: str = "log") -> pd.Series:
    """Calculate returns from price series."""
    if method == "log":
        return np.log(prices / prices.shift(1))
    return prices.pct_change()


def annualize_return(total_return: float, days: int) -> float:
    """Annualize a total return."""
    if days <= 0:
        return 0.0
    return (1 + total_return) ** (252 / days) - 1


def annualize_volatility(daily_vol: float) -> float:
    """Annualize daily volatility."""
    return daily_vol * np.sqrt(252)


def sharpe_ratio(
    returns: pd.Series,
    risk_free_rate: float = 0.04,
    periods_per_year: int = 252,
) -> float:
    """Calculate annualized Sharpe ratio."""
    if returns.std() == 0:
        return 0.0
    excess = returns.mean() - risk_free_rate / periods_per_year
    return excess / returns.std() * np.sqrt(periods_per_year)


def sortino_ratio(
    returns: pd.Series,
    risk_free_rate: float = 0.04,
    periods_per_year: int = 252,
) -> float:
    """Calculate Sortino ratio (downside risk only)."""
    downside = returns[returns < 0]
    if len(downside) == 0 or downside.std() == 0:
        return 0.0
    excess = returns.mean() - risk_free_rate / periods_per_year
    return excess / downside.std() * np.sqrt(periods_per_year)


def max_drawdown(equity_curve: pd.Series) -> tuple[float, int]:
    """
    Calculate maximum drawdown.
    Returns: (max_drawdown_pct, duration_in_bars)
    """
    peak = equity_curve.cummax()
    drawdown = (equity_curve - peak) / peak
    max_dd = drawdown.min()

    # Duration
    underwater = drawdown < 0
    groups = (~underwater).cumsum()
    if underwater.any():
        durations = underwater.groupby(groups).sum()
        max_duration = int(durations.max()) if len(durations) > 0 else 0
    else:
        max_duration = 0

    return float(max_dd), max_duration


def calmar_ratio(returns: pd.Series, periods_per_year: int = 252) -> float:
    """Calmar Ratio = Annualized Return / Max Drawdown."""
    equity = (1 + returns).cumprod()
    mdd, _ = max_drawdown(equity)
    if mdd == 0:
        return 0.0
    ann_ret = returns.mean() * periods_per_year
    return abs(ann_ret / mdd)


def information_ratio(
    returns: pd.Series,
    benchmark_returns: pd.Series,
    periods_per_year: int = 252,
) -> float:
    """Information Ratio vs benchmark."""
    active_return = returns - benchmark_returns
    tracking_error = active_return.std()
    if tracking_error == 0:
        return 0.0
    return active_return.mean() / tracking_error * np.sqrt(periods_per_year)


def win_rate(pnl_series: pd.Series) -> float:
    """Calculate win rate from P&L series."""
    if len(pnl_series) == 0:
        return 0.0
    wins = (pnl_series > 0).sum()
    return wins / len(pnl_series)


def profit_factor(pnl_series: pd.Series) -> float:
    """Profit Factor = Gross Profit / Gross Loss."""
    gross_profit = pnl_series[pnl_series > 0].sum()
    gross_loss = abs(pnl_series[pnl_series < 0].sum())
    return safe_divide(gross_profit, gross_loss, default=0.0)


def kelly_criterion(win_rate_val: float, avg_win: float, avg_loss: float) -> float:
    """Kelly Criterion for optimal position sizing."""
    if avg_loss == 0:
        return 0.0
    win_loss_ratio = avg_win / abs(avg_loss)
    kelly = win_rate_val - (1 - win_rate_val) / win_loss_ratio
    return max(0, min(kelly, 0.25))  # Cap at 25%
