"""
Database Manager
================
SQLAlchemy async database for trade history, signals, and performance tracking.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Boolean,
    Text,
    create_engine,
)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from loguru import logger


class Base(DeclarativeBase):
    pass


class TradeRecord(Base):
    """Trade execution records."""

    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True)
    side: Mapped[str] = mapped_column(String(10))  # BUY / SELL
    quantity: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)
    total_value: Mapped[float] = mapped_column(Float)
    commission: Mapped[float] = mapped_column(Float, default=0.0)
    strategy: Mapped[str] = mapped_column(String(50), default="manual")
    order_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="FILLED")
    pnl: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pnl_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc)
    )


class SignalRecord(Base):
    """Trading signal records."""

    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True)
    signal_type: Mapped[str] = mapped_column(String(20))  # BUY / SELL / HOLD
    strength: Mapped[float] = mapped_column(Float)  # -1.0 to 1.0
    strategy: Mapped[str] = mapped_column(String(50))
    price_at_signal: Mapped[float] = mapped_column(Float)
    target_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    stop_loss: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.5)
    metadata_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    acted_on: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc)
    )


class PortfolioSnapshot(Base):
    """Daily portfolio snapshots for tracking performance."""

    __tablename__ = "portfolio_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    total_value: Mapped[float] = mapped_column(Float)
    cash: Mapped[float] = mapped_column(Float)
    invested: Mapped[float] = mapped_column(Float)
    unrealized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    daily_return: Mapped[float] = mapped_column(Float, default=0.0)
    num_positions: Mapped[int] = mapped_column(Integer, default=0)
    max_drawdown: Mapped[float] = mapped_column(Float, default=0.0)
    sharpe_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc)
    )


class RiskEvent(Base):
    """Risk event log."""

    __tablename__ = "risk_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(50))
    severity: Mapped[str] = mapped_column(String(20))  # LOW, MEDIUM, HIGH, CRITICAL
    symbol: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    message: Mapped[str] = mapped_column(Text)
    data_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc)
    )


class DatabaseManager:
    """Async database manager."""

    def __init__(self, database_url: str = "sqlite+aiosqlite:///./quant_bot.db"):
        self._engine = create_async_engine(database_url, echo=False)
        self._session_factory = async_sessionmaker(self._engine, expire_on_commit=False)
        self._log = logger.bind(agent_name="DATABASE")

    async def initialize(self) -> None:
        """Create all tables."""
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self._log.info("Database initialized")

    async def get_session(self) -> AsyncSession:
        """Get a new database session."""
        return self._session_factory()

    async def save_trade(self, trade: TradeRecord) -> None:
        """Save a trade record."""
        async with self._session_factory() as session:
            session.add(trade)
            await session.commit()
            self._log.info(
                "Trade saved: {} {} {} @ {}",
                trade.side,
                trade.quantity,
                trade.symbol,
                trade.price,
            )

    async def save_signal(self, signal: SignalRecord) -> None:
        """Save a trading signal."""
        async with self._session_factory() as session:
            session.add(signal)
            await session.commit()

    async def save_portfolio_snapshot(self, snapshot: PortfolioSnapshot) -> None:
        """Save portfolio snapshot."""
        async with self._session_factory() as session:
            session.add(snapshot)
            await session.commit()

    async def save_risk_event(self, event: RiskEvent) -> None:
        """Save risk event."""
        async with self._session_factory() as session:
            session.add(event)
            await session.commit()

    async def get_recent_trades(self, limit: int = 50) -> list[TradeRecord]:
        """Get recent trade records."""
        from sqlalchemy import select

        async with self._session_factory() as session:
            result = await session.execute(
                select(TradeRecord)
                .order_by(TradeRecord.created_at.desc())
                .limit(limit)
            )
            return list(result.scalars().all())

    async def get_portfolio_history(self, days: int = 30) -> list[PortfolioSnapshot]:
        """Get portfolio history."""
        from sqlalchemy import select
        from datetime import timedelta

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        async with self._session_factory() as session:
            result = await session.execute(
                select(PortfolioSnapshot)
                .where(PortfolioSnapshot.created_at >= cutoff)
                .order_by(PortfolioSnapshot.created_at.asc())
            )
            return list(result.scalars().all())

    async def close(self) -> None:
        """Close database connections."""
        await self._engine.dispose()
        self._log.info("Database connections closed")
