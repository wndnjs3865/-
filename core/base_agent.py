"""QuantPulse v3 - BaseAgent abstract class for all 7 agents."""

from __future__ import annotations

import asyncio
import logging
import time
import traceback
from abc import ABC, abstractmethod
from typing import Any

from .enums import AgentRole, AlertSeverity, Priority
from .message_bus import MessageBus
from .audit_logger import AuditLogger
from .models import (
    AuditEntry, AlertEvent, HealthStatus, Message,
)

logger = logging.getLogger("quantpulse.agent")

MAX_RETRIES = 3
RETRY_DELAYS = [1.0, 2.0, 4.0]  # Exponential backoff


class BaseAgent(ABC):
    """
    Abstract base class for all QuantPulse agents.

    Provides:
    - MessageBus integration (subscribe/publish)
    - AuditLog integration (every action logged)
    - Health reporting (heartbeat for IMA)
    - Error handling with retry + alert
    - Graceful start/stop lifecycle
    """

    def __init__(
        self,
        role: AgentRole,
        bus: MessageBus,
        audit: AuditLogger,
    ) -> None:
        self.role = role
        self.bus = bus
        self.audit = audit
        self._running = False
        self._start_time: float = 0.0
        self._messages_processed: int = 0
        self._errors_count: int = 0
        self._tasks: list[asyncio.Task[Any]] = []
        self.logger = logging.getLogger(f"quantpulse.agent.{role.value}")

    # ── Lifecycle ──────────────────────────────

    async def start(self) -> None:
        """Start the agent: register subscriptions and background tasks."""
        if self._running:
            return

        self._running = True
        self._start_time = time.time()

        # Let subclass register its topic subscriptions
        await self.setup_subscriptions()

        # Let subclass start its own background tasks
        await self.on_start()

        await self.audit_log("agent_started", {"role": self.role.value})
        self.logger.info(f"[{self.role.value}] Agent started")

    async def stop(self) -> None:
        """Gracefully stop the agent."""
        self._running = False

        # Cancel background tasks
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()

        await self.on_stop()
        await self.audit_log("agent_stopped", {
            "role": self.role.value,
            "uptime": int(time.time() - self._start_time),
            "messages_processed": self._messages_processed,
            "errors": self._errors_count,
        })
        self.logger.info(f"[{self.role.value}] Agent stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    # ── Abstract methods for subclasses ────────

    @abstractmethod
    async def setup_subscriptions(self) -> None:
        """Register MessageBus topic subscriptions. Called on start()."""
        ...

    @abstractmethod
    async def on_start(self) -> None:
        """Custom initialization (background loops, etc). Called after subscriptions."""
        ...

    async def on_stop(self) -> None:
        """Custom cleanup. Override if needed."""
        pass

    # ── MessageBus Helpers ─────────────────────

    async def publish(
        self,
        topic: str,
        payload: dict[str, Any],
        priority: Priority = Priority.NORMAL,
        correlation_id: str | None = None,
    ) -> None:
        """Publish a message to the MessageBus."""
        msg = Message(
            topic=topic,
            sender=self.role,
            priority=priority,
            payload=payload,
            correlation_id=correlation_id,
        )
        await self.bus.publish(msg)

    async def request(
        self,
        topic: str,
        payload: dict[str, Any],
        response_topic: str,
        priority: Priority = Priority.HIGH,
        timeout: float = 30.0,
    ) -> Message | None:
        """Send a request and wait for a correlated response."""
        msg = Message(
            topic=topic,
            sender=self.role,
            priority=priority,
            payload=payload,
        )
        return await self.bus.publish_and_wait(msg, response_topic, timeout=timeout)

    def subscribe(self, topic: str, callback: Any) -> None:
        """Subscribe to a MessageBus topic."""
        self.bus.subscribe(topic, self.role, callback)

    # ── Message Handler Wrapper ────────────────

    def _wrap_handler(self, handler: Any) -> Any:
        """Wrap a message handler with error handling and metrics."""

        async def wrapped(message: Message) -> None:
            try:
                await handler(message)
                self._messages_processed += 1
            except Exception as e:
                self._errors_count += 1
                self.logger.error(
                    f"[{self.role.value}] Error handling '{message.topic}': {e}\n"
                    f"{traceback.format_exc()}"
                )
                await self.audit_log("message_handler_error", {
                    "topic": message.topic,
                    "error": str(e),
                    "message_id": message.id,
                })
                await self._emit_alert(
                    AlertSeverity.WARNING,
                    f"Handler error on '{message.topic}'",
                    str(e),
                )

        return wrapped

    def subscribe_safe(self, topic: str, handler: Any) -> None:
        """Subscribe with automatic error handling wrapper."""
        self.bus.subscribe(topic, self.role, self._wrap_handler(handler))

    # ── Retry Helper ───────────────────────────

    async def retry_async(
        self,
        coro_factory: Any,
        action_name: str,
        max_retries: int = MAX_RETRIES,
    ) -> Any:
        """
        Retry an async operation with exponential backoff.
        Logs each failure to AuditLog and alerts IMA on final failure.
        """
        last_error: Exception | None = None
        for attempt in range(max_retries):
            try:
                return await coro_factory()
            except Exception as e:
                last_error = e
                delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                self.logger.warning(
                    f"[{self.role.value}] {action_name} attempt {attempt+1}/{max_retries} "
                    f"failed: {e}. Retrying in {delay}s..."
                )
                await self.audit_log("retry_attempt", {
                    "action": action_name,
                    "attempt": attempt + 1,
                    "error": str(e),
                })
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)

        # All retries exhausted
        await self.audit_log("retry_exhausted", {
            "action": action_name,
            "max_retries": max_retries,
            "final_error": str(last_error),
        })
        await self._emit_alert(
            AlertSeverity.CRITICAL,
            f"Retry exhausted: {action_name}",
            str(last_error),
        )
        raise last_error  # type: ignore[misc]

    # ── Audit Helpers ──────────────────────────

    async def audit_log(
        self,
        action: str,
        detail: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Write an audit entry."""
        entry = AuditEntry(
            agent=self.role,
            action=action,
            detail=detail or {},
            correlation_id=correlation_id,
        )
        await self.audit.log(entry)

    # ── Health ─────────────────────────────────

    def get_health(self) -> HealthStatus:
        """Generate current health status for IMA."""
        import psutil
        process = psutil.Process()
        return HealthStatus(
            agent=self.role,
            alive=self._running,
            uptime_seconds=int(time.time() - self._start_time) if self._start_time else 0,
            messages_processed=self._messages_processed,
            errors_count=self._errors_count,
            memory_mb=round(process.memory_info().rss / (1024 * 1024), 2),
            cpu_percent=process.cpu_percent(),
        )

    # ── Background Task Helper ─────────────────

    def create_task(self, coro: Any, name: str | None = None) -> asyncio.Task[Any]:
        """Create a tracked background task."""
        task = asyncio.create_task(coro, name=name or f"{self.role.value}_task")
        self._tasks.append(task)

        def _on_done(t: asyncio.Task[Any]) -> None:
            if t in self._tasks:
                self._tasks.remove(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc:
                self.logger.error(f"[{self.role.value}] Background task '{t.get_name()}' failed: {exc}")

        task.add_done_callback(_on_done)
        return task

    # ── Alert Helper ───────────────────────────

    async def _emit_alert(self, severity: AlertSeverity, title: str, detail: str = "") -> None:
        """Publish an alert event for IMA."""
        alert = AlertEvent(
            severity=severity,
            source=self.role,
            title=title,
            detail=detail,
        )
        await self.publish(
            topic="ima.alert",
            payload=alert.model_dump(mode="json"),
            priority=Priority.HIGH if severity in (AlertSeverity.CRITICAL, AlertSeverity.FATAL) else Priority.NORMAL,
        )
