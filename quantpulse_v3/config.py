"""QuantPulse v3 - Global configuration (env-based)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class TradingConfig:
    """Trading parameters."""
    mode: str = "PAPER"  # PAPER or LIVE
    max_risk_per_trade: float = 0.01  # 1% of portfolio
    max_daily_loss: float = 0.03  # 3% daily loss limit
    max_open_positions: int = 5
    max_leverage: float = 10.0
    default_leverage: float = 3.0
    min_quant_score: float = 60.0  # Minimum QR score to trade
    min_risk_reward: float = 2.0
    max_correlation: float = 0.7
    circuit_breaker_loss: float = 0.05  # 5% triggers circuit breaker
    circuit_breaker_cooldown: int = 3600  # 1 hour cooldown


@dataclass
class ExchangeConfig:
    """Exchange API configuration."""
    binance_api_key: str = ""
    binance_api_secret: str = ""
    bybit_api_key: str = ""
    bybit_api_secret: str = ""
    alpaca_api_key: str = ""
    alpaca_api_secret: str = ""


@dataclass
class NotificationConfig:
    """Notification settings."""
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    enable_telegram: bool = False


@dataclass
class SystemConfig:
    """System-level settings."""
    log_level: str = "INFO"
    audit_db_path: str = "data/audit.db"
    audit_max_entries: int = 100_000
    health_check_interval: int = 30  # seconds
    api_host: str = "0.0.0.0"
    api_port: int = 8000


@dataclass
class Config:
    """Master configuration."""
    trading: TradingConfig = field(default_factory=TradingConfig)
    exchange: ExchangeConfig = field(default_factory=ExchangeConfig)
    notification: NotificationConfig = field(default_factory=NotificationConfig)
    system: SystemConfig = field(default_factory=SystemConfig)

    @classmethod
    def from_env(cls) -> Config:
        """Load configuration from environment variables."""
        return cls(
            trading=TradingConfig(
                mode=os.getenv("TRADE_MODE", "PAPER"),
                max_risk_per_trade=float(os.getenv("MAX_RISK_PER_TRADE", "0.01")),
                max_daily_loss=float(os.getenv("MAX_DAILY_LOSS", "0.03")),
                max_open_positions=int(os.getenv("MAX_OPEN_POSITIONS", "5")),
                max_leverage=float(os.getenv("MAX_LEVERAGE", "10.0")),
                default_leverage=float(os.getenv("DEFAULT_LEVERAGE", "3.0")),
                min_quant_score=float(os.getenv("MIN_QUANT_SCORE", "60.0")),
                min_risk_reward=float(os.getenv("MIN_RISK_REWARD", "2.0")),
                circuit_breaker_loss=float(os.getenv("CIRCUIT_BREAKER_LOSS", "0.05")),
                circuit_breaker_cooldown=int(os.getenv("CIRCUIT_BREAKER_COOLDOWN", "3600")),
            ),
            exchange=ExchangeConfig(
                binance_api_key=os.getenv("BINANCE_API_KEY", ""),
                binance_api_secret=os.getenv("BINANCE_API_SECRET", ""),
                bybit_api_key=os.getenv("BYBIT_API_KEY", ""),
                bybit_api_secret=os.getenv("BYBIT_API_SECRET", ""),
                alpaca_api_key=os.getenv("ALPACA_API_KEY", ""),
                alpaca_api_secret=os.getenv("ALPACA_API_SECRET", ""),
            ),
            notification=NotificationConfig(
                telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
                telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
                enable_telegram=os.getenv("ENABLE_TELEGRAM", "false").lower() == "true",
            ),
            system=SystemConfig(
                log_level=os.getenv("LOG_LEVEL", "INFO"),
                audit_db_path=os.getenv("AUDIT_DB_PATH", "data/audit.db"),
                api_host=os.getenv("API_HOST", "0.0.0.0"),
                api_port=int(os.getenv("API_PORT", "8000")),
            ),
        )
