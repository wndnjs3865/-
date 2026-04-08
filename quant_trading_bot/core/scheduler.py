"""
Trading Scheduler
=================
시장 시간 기반 작업 스케줄링.
에이전트 작업 주기 관리 및 마켓 타이밍.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, time, timezone, timedelta
from enum import Enum, auto
from typing import Any, Callable, Coroutine, Optional

from loguru import logger


class MarketSession(Enum):
    PRE_MARKET = auto()
    REGULAR = auto()
    AFTER_HOURS = auto()
    CLOSED = auto()


@dataclass
class ScheduledTask:
    name: str
    callback: Callable[..., Coroutine[Any, Any, None]]
    interval_sec: float
    enabled: bool = True
    run_on_start: bool = False
    market_hours_only: bool = False
    last_run: Optional[datetime] = None
    run_count: int = 0
    error_count: int = 0
    _task: Optional[asyncio.Task] = field(default=None, repr=False)


# US market hours in UTC
US_MARKET_SCHEDULE = {
    "pre_market": (time(8, 0), time(9, 30)),    # 4:00-9:30 ET → UTC
    "regular": (time(13, 30), time(20, 0)),       # 9:30-16:00 ET → UTC
    "after_hours": (time(20, 0), time(24, 0)),    # 16:00-20:00 ET → UTC
}

# KR market hours in UTC
KR_MARKET_SCHEDULE = {
    "regular": (time(0, 0), time(6, 30)),         # 09:00-15:30 KST → UTC
}


class TradingScheduler:
    """
    Trading-aware task scheduler.

    Features:
    - Market-hours-aware scheduling
    - Periodic task management
    - Graceful start/stop
    - Error tracking per task
    """

    def __init__(self, market: str = "US"):
        self._tasks: dict[str, ScheduledTask] = {}
        self._running = False
        self._market = market
        self._schedule = US_MARKET_SCHEDULE if market == "US" else KR_MARKET_SCHEDULE
        self._log = logger.bind(agent_name="SCHEDULER")

    def add_task(
        self,
        name: str,
        callback: Callable[..., Coroutine[Any, Any, None]],
        interval_sec: float,
        market_hours_only: bool = False,
        run_on_start: bool = False,
    ) -> None:
        """Register a periodic task."""
        self._tasks[name] = ScheduledTask(
            name=name,
            callback=callback,
            interval_sec=interval_sec,
            market_hours_only=market_hours_only,
            run_on_start=run_on_start,
        )
        self._log.info("Task registered: {} (interval={}s)", name, interval_sec)

    def remove_task(self, name: str) -> None:
        """Remove a scheduled task."""
        if name in self._tasks:
            task = self._tasks.pop(name)
            if task._task:
                task._task.cancel()
            self._log.info("Task removed: {}", name)

    async def start(self) -> None:
        """Start all scheduled tasks."""
        self._running = True
        for name, task in self._tasks.items():
            if task.enabled:
                task._task = asyncio.create_task(self._run_task_loop(task))
        self._log.info("Scheduler started with {} tasks", len(self._tasks))

    async def stop(self) -> None:
        """Stop all tasks gracefully."""
        self._running = False
        for task in self._tasks.values():
            if task._task:
                task._task.cancel()
                try:
                    await task._task
                except asyncio.CancelledError:
                    pass
        self._log.info("Scheduler stopped")

    async def _run_task_loop(self, task: ScheduledTask) -> None:
        """Run a single task in a loop."""
        if task.run_on_start:
            await self._execute_task(task)

        while self._running:
            try:
                await asyncio.sleep(task.interval_sec)

                if not self._running:
                    break

                if task.market_hours_only and not self.is_market_open():
                    continue

                await self._execute_task(task)
            except asyncio.CancelledError:
                break
            except Exception as e:
                task.error_count += 1
                self._log.error("Task loop error [{}]: {}", task.name, e)
                await asyncio.sleep(min(task.interval_sec, 60))

    async def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a single task with error handling."""
        try:
            await task.callback()
            task.last_run = datetime.now(timezone.utc)
            task.run_count += 1
        except Exception as e:
            task.error_count += 1
            self._log.error("Task execution error [{}]: {}", task.name, e)

    def is_market_open(self) -> bool:
        """Check if market is currently open."""
        now = datetime.now(timezone.utc).time()
        regular = self._schedule.get("regular")
        if regular:
            return regular[0] <= now <= regular[1]
        return False

    def get_market_session(self) -> MarketSession:
        """Get current market session."""
        now = datetime.now(timezone.utc).time()

        for session_name, (start, end) in self._schedule.items():
            if start <= now <= end:
                if session_name == "pre_market":
                    return MarketSession.PRE_MARKET
                elif session_name == "regular":
                    return MarketSession.REGULAR
                elif session_name == "after_hours":
                    return MarketSession.AFTER_HOURS

        return MarketSession.CLOSED

    def get_next_market_open(self) -> Optional[datetime]:
        """Get next market open time."""
        now = datetime.now(timezone.utc)
        regular = self._schedule.get("regular")
        if not regular:
            return None

        open_time = datetime.combine(now.date(), regular[0], tzinfo=timezone.utc)
        if now.time() > regular[1]:
            open_time += timedelta(days=1)
            # Skip weekends
            while open_time.weekday() >= 5:
                open_time += timedelta(days=1)
        return open_time

    def get_stats(self) -> dict:
        """Get scheduler statistics."""
        return {
            "running": self._running,
            "market": self._market,
            "market_session": self.get_market_session().name,
            "tasks": {
                name: {
                    "enabled": t.enabled,
                    "interval_sec": t.interval_sec,
                    "run_count": t.run_count,
                    "error_count": t.error_count,
                    "last_run": t.last_run.isoformat() if t.last_run else None,
                }
                for name, t in self._tasks.items()
            },
        }
