"""
Configuration Management
========================
Pydantic-based settings with environment variable support.
모든 설정값을 중앙 관리하며, .env 파일에서 자동 로드.
"""

from __future__ import annotations

import os
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class TradingEnv(str, Enum):
    REAL = "REAL"
    VIRTUAL = "VIRTUAL"


class TradingUniverse(str, Enum):
    US = "US"
    KR = "KR"
    GLOBAL = "GLOBAL"


class StrategyType(str, Enum):
    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"
    MULTI_FACTOR = "multi_factor"
    PAIRS_TRADING = "pairs_trading"


class Settings(BaseSettings):
    """Central configuration for the Quant Trading Bot."""

    # === 한국투자증권 API ===
    kis_app_key: str = Field(default="", description="KIS API App Key")
    kis_app_secret: str = Field(default="", description="KIS API App Secret")
    kis_account_no: str = Field(default="", description="KIS Account Number (XXXXXXXX-XX)")
    kis_env: TradingEnv = Field(default=TradingEnv.VIRTUAL, description="Trading environment")

    # === KIS API URLs ===
    @property
    def kis_base_url(self) -> str:
        if self.kis_env == TradingEnv.REAL:
            return "https://openapi.koreainvestment.com:9443"
        return "https://openapivts.koreainvestment.com:29443"

    @property
    def kis_ws_url(self) -> str:
        if self.kis_env == TradingEnv.REAL:
            return "ws://ops.koreainvestment.com:21000"
        return "ws://ops.koreainvestment.com:31000"

    # === AI API Keys ===
    openai_api_key: str = Field(default="", description="OpenAI API Key")
    anthropic_api_key: str = Field(default="", description="Anthropic API Key")
    xai_api_key: str = Field(default="", description="xAI Grok API Key")

    # === Telegram ===
    telegram_bot_token: str = Field(default="", description="Telegram Bot Token")
    telegram_chat_id: str = Field(default="", description="Telegram Chat ID")

    # === Database ===
    database_url: str = Field(
        default="sqlite+aiosqlite:///./quant_bot.db",
        description="Database connection URL",
    )
    redis_url: str = Field(default="redis://localhost:6379/0", description="Redis URL")

    # === Risk Parameters ===
    max_portfolio_risk: float = Field(default=0.02, description="Max portfolio risk per trade (2%)")
    max_single_position_pct: float = Field(default=0.10, description="Max single position (10%)")
    max_daily_loss_pct: float = Field(default=0.03, description="Max daily loss (3%)")
    max_drawdown_pct: float = Field(default=0.15, description="Max drawdown limit (15%)")
    var_confidence: float = Field(default=0.99, description="VaR confidence level")
    stop_loss_pct: float = Field(default=0.05, description="Default stop-loss (5%)")
    take_profit_pct: float = Field(default=0.15, description="Default take-profit (15%)")
    trailing_stop_pct: float = Field(default=0.03, description="Trailing stop (3%)")

    # === Trading Parameters ===
    trading_universe: TradingUniverse = Field(default=TradingUniverse.US)
    default_strategy: StrategyType = Field(default=StrategyType.MULTI_FACTOR)
    rebalance_frequency: str = Field(default="daily")
    market_open_hour: int = Field(default=9)
    market_close_hour: int = Field(default=16)
    initial_capital: float = Field(default=100_000_000.0, description="Initial capital (KRW)")
    min_trade_amount: float = Field(default=100_000.0, description="Minimum trade amount")
    max_open_positions: int = Field(default=20, description="Max concurrent positions")
    slippage_bps: float = Field(default=5.0, description="Expected slippage in bps")
    commission_bps: float = Field(default=3.0, description="Commission in bps")

    # === Agent Parameters ===
    agent_heartbeat_sec: int = Field(default=30, description="Agent heartbeat interval")
    data_refresh_sec: int = Field(default=60, description="Market data refresh interval")
    analysis_interval_sec: int = Field(default=300, description="Analysis cycle interval")
    risk_check_interval_sec: int = Field(default=30, description="Risk check interval")

    # === Logging ===
    log_level: str = Field(default="INFO")
    log_file: str = Field(default="logs/quant_bot.log")

    # === Feature Flags ===
    enable_live_trading: bool = Field(default=False, description="Enable live trading")
    enable_backtesting: bool = Field(default=True, description="Enable backtesting mode")
    enable_paper_trading: bool = Field(default=True, description="Enable paper trading")
    enable_telegram: bool = Field(default=True, description="Enable Telegram notifications")
    enable_sentiment: bool = Field(default=True, description="Enable sentiment analysis")

    @field_validator("kis_account_no")
    @classmethod
    def validate_account_no(cls, v: str) -> str:
        if v and "-" not in v and len(v) == 10:
            return f"{v[:8]}-{v[8:]}"
        return v

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",
    }


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    env_path = Path(".env")
    if not env_path.exists():
        project_root = Path(__file__).parent.parent.parent
        env_path = project_root / ".env"
    return Settings(_env_file=str(env_path) if env_path.exists() else None)
