"""
Logging Configuration
=====================
Loguru-based structured logging with rotation and Telegram sink.
"""

import sys
from pathlib import Path

from loguru import logger


def setup_logging(log_level: str = "INFO", log_file: str = "logs/quant_bot.log") -> None:
    """Configure application-wide logging."""
    logger.remove()

    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<blue>[{extra[agent_name]}]</blue> "
        "<level>{message}</level>"
    )

    simple_format = (
        "<green>{time:HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<level>{message}</level>"
    )

    # Console output
    logger.add(
        sys.stderr,
        format=simple_format,
        level=log_level,
        colorize=True,
    )

    # File output with rotation
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger.add(
        str(log_path),
        format=log_format,
        level=log_level,
        rotation="100 MB",
        retention="30 days",
        compression="gz",
        serialize=False,
        enqueue=True,  # Thread-safe
    )

    # Separate error log
    error_log = log_path.parent / "errors.log"
    logger.add(
        str(error_log),
        format=log_format,
        level="ERROR",
        rotation="50 MB",
        retention="90 days",
        compression="gz",
        enqueue=True,
    )

    # Trade-specific log
    trade_log = log_path.parent / "trades.log"
    logger.add(
        str(trade_log),
        format=log_format,
        level="INFO",
        rotation="50 MB",
        retention="365 days",
        filter=lambda record: record["extra"].get("trade", False),
        enqueue=True,
    )

    # Configure default extras
    logger.configure(extra={"agent_name": "SYSTEM", "trade": False})

    logger.info("Logging initialized | level={} | file={}", log_level, log_file)
