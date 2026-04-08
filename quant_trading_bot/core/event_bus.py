"""
Event Bus - Inter-Agent Communication
======================================
비동기 이벤트 기반 에이전트 간 통신 시스템.
Pub/Sub 패턴으로 느슨한 결합 유지.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Callable, Coroutine, Optional
from uuid import uuid4

from loguru import logger


class EventType(Enum):
    """All event types in the system."""

    # Market Data Events
    MARKET_DATA_UPDATE = auto()
    MARKET_DATA_ERROR = auto()
    PRICE_ALERT = auto()
    VOLUME_SPIKE = auto()

    # Analysis Events
    ANALYSIS_COMPLETE = auto()
    SIGNAL_GENERATED = auto()
    INDICATOR_UPDATE = auto()

    # Sentiment Events
    SENTIMENT_UPDATE = auto()
    NEWS_ALERT = auto()

    # Risk Events
    RISK_CHECK_PASSED = auto()
    RISK_CHECK_FAILED = auto()
    RISK_LIMIT_BREACH = auto()
    DRAWDOWN_WARNING = auto()
    STOP_LOSS_TRIGGERED = auto()

    # Execution Events
    ORDER_SUBMITTED = auto()
    ORDER_FILLED = auto()
    ORDER_CANCELLED = auto()
    ORDER_REJECTED = auto()
    ORDER_PARTIAL_FILL = auto()

    # Portfolio Events
    PORTFOLIO_UPDATE = auto()
    REBALANCE_SIGNAL = auto()
    POSITION_OPENED = auto()
    POSITION_CLOSED = auto()

    # System Events
    SYSTEM_START = auto()
    SYSTEM_STOP = auto()
    AGENT_HEARTBEAT = auto()
    AGENT_ERROR = auto()
    HEALTH_CHECK = auto()

    # Strategy Events
    STRATEGY_SIGNAL = auto()
    STRATEGY_UPDATE = auto()


@dataclass
class Event:
    """Immutable event object for inter-agent communication."""

    event_type: EventType
    data: dict[str, Any] = field(default_factory=dict)
    source: str = "SYSTEM"
    target: Optional[str] = None  # None = broadcast
    event_id: str = field(default_factory=lambda: str(uuid4())[:8])
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    priority: int = 5  # 1 = highest, 10 = lowest
    correlation_id: Optional[str] = None  # For tracking related events

    def __str__(self) -> str:
        return f"Event({self.event_type.name}, src={self.source}, id={self.event_id})"


# Type alias for event handlers
EventHandler = Callable[[Event], Coroutine[Any, Any, None]]


class EventBus:
    """
    Async Event Bus for Multi-Agent Communication.

    Features:
    - Pub/Sub pattern with topic-based routing
    - Priority queue for event ordering
    - Dead letter queue for failed events
    - Event history for debugging
    - Async processing with backpressure
    """

    def __init__(self, max_queue_size: int = 10_000, max_history: int = 1_000):
        self._subscribers: dict[EventType, list[EventHandler]] = {}
        self._global_subscribers: list[EventHandler] = []
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max_queue_size)
        self._dead_letter: list[Event] = []
        self._history: list[Event] = []
        self._max_history = max_history
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
        self._event_count = 0
        self._error_count = 0
        self._log = logger.bind(agent_name="EVENT_BUS")

    def subscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """Subscribe a handler to a specific event type."""
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(handler)
        self._log.debug("Subscribed handler to {}", event_type.name)

    def subscribe_all(self, handler: EventHandler) -> None:
        """Subscribe a handler to ALL events."""
        self._global_subscribers.append(handler)

    def unsubscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """Unsubscribe a handler from an event type."""
        if event_type in self._subscribers:
            self._subscribers[event_type] = [
                h for h in self._subscribers[event_type] if h != handler
            ]

    async def publish(self, event: Event) -> None:
        """Publish an event to the bus."""
        await self._queue.put((event.priority, self._event_count, event))
        self._event_count += 1

    def publish_sync(self, event: Event) -> None:
        """Synchronous publish for non-async contexts."""
        try:
            self._queue.put_nowait((event.priority, self._event_count, event))
            self._event_count += 1
        except asyncio.QueueFull:
            self._log.error("Event queue full, dropping event: {}", event)
            self._dead_letter.append(event)

    async def start(self) -> None:
        """Start the event processing loop."""
        self._running = True
        self._processor_task = asyncio.create_task(self._process_events())
        self._log.info("Event Bus started")

    async def stop(self) -> None:
        """Stop the event processing loop."""
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        self._log.info(
            "Event Bus stopped | processed={} errors={}",
            self._event_count,
            self._error_count,
        )

    async def _process_events(self) -> None:
        """Main event processing loop."""
        while self._running:
            try:
                priority, seq, event = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
                await self._dispatch(event)
                self._record_history(event)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._error_count += 1
                self._log.error("Event processing error: {}", e)

    async def _dispatch(self, event: Event) -> None:
        """Dispatch event to all registered handlers."""
        handlers = list(self._global_subscribers)

        if event.event_type in self._subscribers:
            handlers.extend(self._subscribers[event.event_type])

        if not handlers:
            return

        tasks = []
        for handler in handlers:
            tasks.append(self._safe_call(handler, event))

        await asyncio.gather(*tasks)

    async def _safe_call(self, handler: EventHandler, event: Event) -> None:
        """Safely call a handler with error handling."""
        try:
            await asyncio.wait_for(handler(event), timeout=30.0)
        except asyncio.TimeoutError:
            self._log.warning("Handler timeout for event: {}", event)
            self._dead_letter.append(event)
        except Exception as e:
            self._error_count += 1
            self._log.error("Handler error for {}: {}", event, e)
            self._dead_letter.append(event)

    def _record_history(self, event: Event) -> None:
        """Record event in history ring buffer."""
        self._history.append(event)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]

    def get_stats(self) -> dict:
        """Get event bus statistics."""
        return {
            "total_events": self._event_count,
            "errors": self._error_count,
            "queue_size": self._queue.qsize(),
            "dead_letters": len(self._dead_letter),
            "subscribers": {
                et.name: len(handlers)
                for et, handlers in self._subscribers.items()
            },
            "global_subscribers": len(self._global_subscribers),
        }
