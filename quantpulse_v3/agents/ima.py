"""QuantPulse v3 - IMA (Infrastructure & Monitoring Agent).

System health monitoring, alert management, and Telegram notifications.
Collects heartbeats from all agents every 30 seconds.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from core.base_agent import BaseAgent
from core.enums import AgentRole, AlertSeverity, Priority
from core.message_bus import MessageBus
from core.audit_logger import AuditLogger
from core.models import AlertEvent, HealthStatus, Message
from config import Config

logger = logging.getLogger("quantpulse.ima")


class IMAAgent(BaseAgent):
    """
    Infrastructure & Monitoring Agent — 시스템 감시 + 알림.

    - 모든 에이전트 Health Status 수집 (30초 간격)
    - AlertEvent 수신 시 Telegram + 내부 알림
    - Topic: ima.alert, ima.health_report
    """

    def __init__(
        self,
        bus: MessageBus,
        audit: AuditLogger,
        config: Config,
        agents: list[Any] | None = None,
    ) -> None:
        super().__init__(AgentRole.IMA, bus, audit)
        self.config = config
        self._monitored_agents: list[Any] = agents or []

        # Alert history
        self._alerts: list[AlertEvent] = []
        self._unacknowledged_alerts: list[AlertEvent] = []

        # Health snapshots
        self._latest_health: dict[str, HealthStatus] = {}

        # Telegram state
        self._telegram_enabled = config.notification.enable_telegram
        self._telegram_queue: list[str] = []

    async def setup_subscriptions(self) -> None:
        self.subscribe_safe("ima.alert", self._handle_alert)

    async def on_start(self) -> None:
        interval = self.config.system.health_check_interval
        self.create_task(self._health_check_loop(interval), name="ima_health_check")

    # ── Alert Handler ──────────────────────────

    async def _handle_alert(self, message: Message) -> None:
        """Process incoming alert events from any agent."""
        alert = AlertEvent(**message.payload)
        self._alerts.append(alert)
        self._unacknowledged_alerts.append(alert)

        await self.audit_log("alert_received", {
            "alert_id": alert.alert_id,
            "severity": alert.severity.value,
            "source": alert.source.value,
            "title": alert.title,
            "detail": alert.detail,
        })

        self.logger.warning(
            f"[IMA] ALERT [{alert.severity.value}] from {alert.source.value}: "
            f"{alert.title}"
        )

        # Send Telegram for CRITICAL/FATAL
        if alert.severity in (AlertSeverity.CRITICAL, AlertSeverity.FATAL):
            await self._send_telegram(
                f"[{alert.severity.value}] {alert.source.value}: {alert.title}\n"
                f"{alert.detail}"
            )

    # ── Health Check Loop ──────────────────────

    async def _health_check_loop(self, interval: int) -> None:
        """Collect health from all monitored agents periodically."""
        while self._running:
            await self._collect_health()
            await asyncio.sleep(interval)

    async def _collect_health(self) -> None:
        """Collect health status from all registered agents."""
        report: dict[str, Any] = {}
        all_healthy = True

        for agent in self._monitored_agents:
            try:
                health = agent.get_health()
                self._latest_health[health.agent.value] = health
                report[health.agent.value] = {
                    "alive": health.alive,
                    "uptime": health.uptime_seconds,
                    "messages": health.messages_processed,
                    "errors": health.errors_count,
                }
                if not health.alive:
                    all_healthy = False
                    await self._emit_alert(
                        AlertSeverity.CRITICAL,
                        f"Agent {health.agent.value} is not alive",
                        f"Last heartbeat: {health.last_heartbeat.isoformat()}",
                    )
                elif health.errors_count > 10:
                    await self._emit_alert(
                        AlertSeverity.WARNING,
                        f"Agent {health.agent.value} has {health.errors_count} errors",
                    )
            except Exception as e:
                all_healthy = False
                report[getattr(agent, 'role', 'unknown')] = {"error": str(e)}
                self.logger.error(f"[IMA] Health check failed for agent: {e}")

        if report:
            await self.publish(
                topic="ima.health_report",
                payload={
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "all_healthy": all_healthy,
                    "agents": report,
                },
                priority=Priority.LOW,
            )

    # ── Telegram ───────────────────────────────

    async def _send_telegram(self, text: str) -> None:
        """Send a Telegram notification (actual sending in Phase 5)."""
        self._telegram_queue.append(text)

        if not self._telegram_enabled:
            self.logger.debug(f"[IMA] Telegram disabled, queued: {text[:80]}...")
            return

        token = self.config.notification.telegram_bot_token
        chat_id = self.config.notification.telegram_chat_id
        if not token or not chat_id:
            return

        # Phase 5: actual HTTP request via aiohttp
        # async with aiohttp.ClientSession() as session:
        #     await session.post(
        #         f"https://api.telegram.org/bot{token}/sendMessage",
        #         json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
        #     )
        self.logger.info(f"[IMA] Telegram notification queued: {text[:80]}...")

    # ── Alert Management ───────────────────────

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert by ID."""
        for alert in self._unacknowledged_alerts:
            if alert.alert_id == alert_id:
                alert.acknowledged = True
                self._unacknowledged_alerts.remove(alert)
                return True
        return False

    def acknowledge_all(self) -> int:
        """Acknowledge all pending alerts."""
        count = len(self._unacknowledged_alerts)
        for alert in self._unacknowledged_alerts:
            alert.acknowledged = True
        self._unacknowledged_alerts.clear()
        return count

    # ── Agent Registration ─────────────────────

    def register_agent(self, agent: Any) -> None:
        """Register an agent for health monitoring."""
        if agent not in self._monitored_agents:
            self._monitored_agents.append(agent)

    # ── Query ──────────────────────────────────

    @property
    def ima_snapshot(self) -> dict[str, Any]:
        return {
            "monitored_agents": len(self._monitored_agents),
            "total_alerts": len(self._alerts),
            "unacknowledged_alerts": len(self._unacknowledged_alerts),
            "telegram_queue_size": len(self._telegram_queue),
            "latest_health": {
                name: {
                    "alive": h.alive,
                    "uptime": h.uptime_seconds,
                    "errors": h.errors_count,
                }
                for name, h in self._latest_health.items()
            },
        }

    def get_alerts(self, limit: int = 50, severity: AlertSeverity | None = None) -> list[AlertEvent]:
        alerts = self._alerts
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        return alerts[-limit:]
