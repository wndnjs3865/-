"""QuantPulse v3 - Async MessageBus with PubSub, priority queue, and request-response."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from .enums import AgentRole, Priority
from .models import AuditEntry, Message

logger = logging.getLogger("quantpulse.bus")

# Priority weights (higher = processed first)
PRIORITY_WEIGHT: dict[Priority, int] = {
    Priority.LOW: 1,
    Priority.NORMAL: 2,
    Priority.HIGH: 3,
    Priority.URGENT: 4,
}

# Type alias for subscriber callbacks
SubscriberCallback = Callable[[Message], Awaitable[None]]


@dataclass
class Subscription:
    """A single topic subscription."""
    subscriber: AgentRole
    callback: SubscriberCallback
    topic_pattern: str  # Exact match or wildcard (e.g. "mia.*")
    created_at: float = field(default_factory=time.time)


class MessageBus:
    """
    Async PubSub MessageBus.

    Features:
    - Topic-based publish/subscribe
    - Priority-ordered delivery (URGENT first)
    - Wildcard subscriptions (e.g. "es.*" matches "es.order_filled")
    - Request-response pattern via correlation_id
    - Dead letter queue for failed deliveries
    - Message history for replay
    - Metrics tracking
    """

    def __init__(self, audit_logger: Any | None = None, max_history: int = 10000) -> None:
        self._subscriptions: dict[str, list[Subscription]] = defaultdict(list)
        self._wildcard_subs: list[Subscription] = []
        self._pending_responses: dict[str, tuple[asyncio.Future[Message], str]] = {}  # correlation_id → (future, response_topic)
        self._queue: asyncio.PriorityQueue[tuple[int, float, Message]] = asyncio.PriorityQueue()
        self._history: list[Message] = []
        self._dead_letters: list[tuple[Message, str]] = []
        self._max_history = max_history
        self._audit_logger = audit_logger
        self._running = False
        self._dispatcher_task: asyncio.Task[None] | None = None

        # Metrics
        self._metrics = {
            "total_published": 0,
            "total_delivered": 0,
            "total_failed": 0,
            "by_topic": defaultdict(int),
            "by_sender": defaultdict(int),
        }

    # ── Lifecycle ──────────────────────────────

    async def start(self) -> None:
        """Start the message dispatcher loop."""
        if self._running:
            return
        self._running = True
        self._dispatcher_task = asyncio.create_task(self._dispatch_loop())
        logger.info("MessageBus started")

    async def stop(self) -> None:
        """Gracefully stop the dispatcher."""
        self._running = False
        if self._dispatcher_task:
            self._dispatcher_task.cancel()
            try:
                await self._dispatcher_task
            except asyncio.CancelledError:
                pass
        # Resolve any pending response futures
        for fut, _ in self._pending_responses.values():
            if not fut.done():
                fut.cancel()
        self._pending_responses.clear()
        logger.info("MessageBus stopped")

    # ── Subscribe ──────────────────────────────

    def subscribe(
        self,
        topic: str,
        subscriber: AgentRole,
        callback: SubscriberCallback,
    ) -> None:
        """Subscribe to a topic. Supports wildcards: 'mia.*' matches 'mia.trade_signal'."""
        sub = Subscription(
            subscriber=subscriber,
            callback=callback,
            topic_pattern=topic,
        )
        if "*" in topic:
            self._wildcard_subs.append(sub)
        else:
            self._subscriptions[topic].append(sub)
        logger.debug(f"{subscriber.value} subscribed to '{topic}'")

    def unsubscribe(self, topic: str, subscriber: AgentRole) -> None:
        """Remove all subscriptions for a subscriber on a topic."""
        if "*" in topic:
            self._wildcard_subs = [
                s for s in self._wildcard_subs
                if not (s.topic_pattern == topic and s.subscriber == subscriber)
            ]
        else:
            self._subscriptions[topic] = [
                s for s in self._subscriptions[topic]
                if s.subscriber != subscriber
            ]

    # ── Publish ────────────────────────────────

    async def publish(self, message: Message) -> None:
        """Publish a message to the bus. Enqueued by priority."""
        # Priority queue uses negative weight so URGENT is dequeued first
        weight = -PRIORITY_WEIGHT.get(message.priority, 2)
        await self._queue.put((weight, time.time(), message))

        # Track
        self._metrics["total_published"] += 1
        self._metrics["by_topic"][message.topic] += 1
        self._metrics["by_sender"][message.sender.value] += 1

        # History
        self._history.append(message)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]

        logger.debug(
            f"[PUB] {message.sender.value} → '{message.topic}' "
            f"(priority={message.priority.value}, id={message.id})"
        )

    async def publish_and_wait(
        self,
        message: Message,
        response_topic: str,
        timeout: float = 30.0,
    ) -> Message | None:
        """
        Publish a message and wait for a correlated response.
        Used for request-response patterns (e.g. CSO → CRCO risk check).
        """
        correlation_id = message.correlation_id or uuid.uuid4().hex[:16]
        message.correlation_id = correlation_id

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Message] = loop.create_future()
        self._pending_responses[correlation_id] = (future, response_topic)

        await self.publish(message)

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(
                f"[TIMEOUT] No response on '{response_topic}' "
                f"for correlation_id={correlation_id} within {timeout}s"
            )
            self._dead_letters.append((message, f"Timeout waiting for {response_topic}"))
            return None
        finally:
            self._pending_responses.pop(correlation_id, None)

    # ── Dispatch Loop ──────────────────────────

    async def _dispatch_loop(self) -> None:
        """Main dispatcher: dequeue messages and deliver to subscribers."""
        while self._running:
            try:
                weight, ts, message = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            await self._deliver(message)

    async def _deliver(self, message: Message) -> None:
        """Deliver a message to all matching subscribers."""
        # Resolve pending response futures first (before subscriber check)
        if message.correlation_id and message.correlation_id in self._pending_responses:
            fut, expected_topic = self._pending_responses[message.correlation_id]
            if not fut.done() and message.topic == expected_topic:
                fut.set_result(message)

        subscribers = list(self._subscriptions.get(message.topic, []))

        # Check wildcard matches
        for sub in self._wildcard_subs:
            pattern = sub.topic_pattern
            prefix = pattern.replace("*", "")
            if message.topic.startswith(prefix):
                subscribers.append(sub)

        if not subscribers:
            logger.debug(f"[NOOP] No subscribers for '{message.topic}'")
            return

        # Deliver to all subscribers concurrently
        tasks = []
        for sub in subscribers:
            tasks.append(self._safe_deliver(sub, message))

        if tasks:
            await asyncio.gather(*tasks)

    async def _safe_deliver(self, sub: Subscription, message: Message) -> None:
        """Deliver with error handling."""
        try:
            await sub.callback(message)
            self._metrics["total_delivered"] += 1
            logger.debug(
                f"[DELIVER] '{message.topic}' → {sub.subscriber.value}"
            )
        except Exception as e:
            self._metrics["total_failed"] += 1
            error_detail = f"{sub.subscriber.value} failed on '{message.topic}': {e}"
            logger.error(f"[FAIL] {error_detail}")
            self._dead_letters.append((message, error_detail))

            if self._audit_logger:
                await self._audit_logger.log(AuditEntry(
                    agent=sub.subscriber,
                    action="message_delivery_failed",
                    detail={"topic": message.topic, "error": str(e), "message_id": message.id},
                ))

    # ── Query ──────────────────────────────────

    def get_history(
        self,
        topic: str | None = None,
        sender: AgentRole | None = None,
        limit: int = 50,
    ) -> list[Message]:
        """Query message history with optional filters."""
        msgs = self._history
        if topic:
            msgs = [m for m in msgs if m.topic == topic]
        if sender:
            msgs = [m for m in msgs if m.sender == sender]
        return msgs[-limit:]

    @property
    def dead_letters(self) -> list[tuple[Message, str]]:
        return list(self._dead_letters)

    @property
    def metrics(self) -> dict[str, Any]:
        return {
            "total_published": self._metrics["total_published"],
            "total_delivered": self._metrics["total_delivered"],
            "total_failed": self._metrics["total_failed"],
            "queue_size": self._queue.qsize(),
            "subscriptions": sum(len(v) for v in self._subscriptions.values()) + len(self._wildcard_subs),
            "dead_letters": len(self._dead_letters),
            "history_size": len(self._history),
        }

    @property
    def subscription_map(self) -> dict[str, list[str]]:
        """Return topic → list of subscriber names."""
        result: dict[str, list[str]] = {}
        for topic, subs in self._subscriptions.items():
            result[topic] = [s.subscriber.value for s in subs]
        for sub in self._wildcard_subs:
            result.setdefault(sub.topic_pattern, []).append(sub.subscriber.value)
        return result
