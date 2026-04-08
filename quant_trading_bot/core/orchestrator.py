"""
Orchestrator - Multi-Agent Coordinator
=======================================
모든 에이전트의 생명주기 관리, 워크플로우 조율,
시스템 상태 모니터링을 담당하는 중앙 제어 시스템.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Optional, TYPE_CHECKING

from loguru import logger

from .event_bus import EventBus, Event, EventType
from .scheduler import TradingScheduler

if TYPE_CHECKING:
    from ..agents.base_agent import BaseAgent


class SystemState(Enum):
    INITIALIZING = auto()
    RUNNING = auto()
    PAUSED = auto()
    STOPPING = auto()
    STOPPED = auto()
    ERROR = auto()


@dataclass
class AgentStatus:
    name: str
    state: str
    last_heartbeat: Optional[datetime] = None
    error_count: int = 0
    task_count: int = 0


class Orchestrator:
    """
    Central coordinator for the Multi-Agent Trading System.

    Responsibilities:
    1. Agent lifecycle management (init, start, stop, restart)
    2. Workflow coordination (data → analysis → risk → execution)
    3. System health monitoring
    4. Emergency shutdown procedures
    5. Performance metrics collection
    """

    def __init__(self, event_bus: EventBus, scheduler: TradingScheduler):
        self.event_bus = event_bus
        self.scheduler = scheduler
        self._agents: dict[str, BaseAgent] = {}
        self._state = SystemState.INITIALIZING
        self._start_time: Optional[datetime] = None
        self._log = logger.bind(agent_name="ORCHESTRATOR")

        # Subscribe to system events
        self.event_bus.subscribe(EventType.AGENT_HEARTBEAT, self._handle_heartbeat)
        self.event_bus.subscribe(EventType.AGENT_ERROR, self._handle_agent_error)
        self.event_bus.subscribe(EventType.RISK_LIMIT_BREACH, self._handle_risk_breach)
        self.event_bus.subscribe(EventType.STOP_LOSS_TRIGGERED, self._handle_stop_loss)

    def register_agent(self, agent: BaseAgent) -> None:
        """Register an agent with the orchestrator."""
        self._agents[agent.name] = agent
        self._log.info("Agent registered: {}", agent.name)

    async def start(self) -> None:
        """Start the entire trading system."""
        self._log.info("=" * 60)
        self._log.info("  QUANT TRADING BOT - STARTING")
        self._log.info("=" * 60)

        self._state = SystemState.INITIALIZING
        self._start_time = datetime.now(timezone.utc)

        # 1. Start Event Bus
        await self.event_bus.start()
        self._log.info("[1/4] Event Bus started")

        # 2. Initialize all agents
        for name, agent in self._agents.items():
            try:
                await agent.initialize()
                self._log.info("  Agent initialized: {}", name)
            except Exception as e:
                self._log.error("  Failed to initialize agent {}: {}", name, e)
                raise

        self._log.info("[2/4] All agents initialized ({} agents)", len(self._agents))

        # 3. Start all agents
        for name, agent in self._agents.items():
            try:
                await agent.start()
                self._log.info("  Agent started: {}", name)
            except Exception as e:
                self._log.error("  Failed to start agent {}: {}", name, e)
                raise

        self._log.info("[3/4] All agents started")

        # 4. Start scheduler
        await self.scheduler.start()
        self._log.info("[4/4] Scheduler started")

        self._state = SystemState.RUNNING

        # Publish system start event
        await self.event_bus.publish(Event(
            event_type=EventType.SYSTEM_START,
            source="ORCHESTRATOR",
            data={"agents": list(self._agents.keys())},
        ))

        self._log.info("=" * 60)
        self._log.info("  SYSTEM RUNNING - {} agents active", len(self._agents))
        self._log.info("=" * 60)

    async def stop(self) -> None:
        """Gracefully stop the entire system."""
        self._log.info("System shutdown initiated...")
        self._state = SystemState.STOPPING

        # Publish stop event
        await self.event_bus.publish(Event(
            event_type=EventType.SYSTEM_STOP,
            source="ORCHESTRATOR",
        ))

        # Allow event to propagate
        await asyncio.sleep(1)

        # Stop in reverse order
        # 1. Stop scheduler
        await self.scheduler.stop()
        self._log.info("Scheduler stopped")

        # 2. Stop agents (execution first, data last)
        stop_order = self._get_stop_order()
        for name in stop_order:
            if name in self._agents:
                try:
                    await self._agents[name].stop()
                    self._log.info("  Agent stopped: {}", name)
                except Exception as e:
                    self._log.error("  Error stopping agent {}: {}", name, e)

        # 3. Stop event bus
        await self.event_bus.stop()
        self._log.info("Event Bus stopped")

        self._state = SystemState.STOPPED
        self._log.info("System shutdown complete")

    async def pause(self) -> None:
        """Pause trading (keep data flowing)."""
        self._state = SystemState.PAUSED
        for agent in self._agents.values():
            if agent.name != "MarketDataAgent":
                await agent.pause()
        self._log.warning("System PAUSED - data collection continues")

    async def resume(self) -> None:
        """Resume trading from paused state."""
        for agent in self._agents.values():
            await agent.resume()
        self._state = SystemState.RUNNING
        self._log.info("System RESUMED")

    async def emergency_stop(self, reason: str) -> None:
        """
        Emergency shutdown - cancel all pending orders, close positions.
        긴급 정지: 모든 주문 취소 및 포지션 정리
        """
        self._log.critical("EMERGENCY STOP: {}", reason)
        self._state = SystemState.ERROR

        # Publish emergency event
        await self.event_bus.publish(Event(
            event_type=EventType.SYSTEM_STOP,
            source="ORCHESTRATOR",
            data={"reason": reason, "emergency": True},
            priority=1,
        ))

        await asyncio.sleep(2)
        await self.stop()

    async def _handle_heartbeat(self, event: Event) -> None:
        """Process agent heartbeat events."""
        agent_name = event.source
        if agent_name in self._agents:
            self._agents[agent_name]._last_heartbeat = event.timestamp

    async def _handle_agent_error(self, event: Event) -> None:
        """Handle agent error events."""
        agent_name = event.source
        error = event.data.get("error", "Unknown")
        self._log.error("Agent error [{}]: {}", agent_name, error)

        # Auto-restart if not critical
        if not event.data.get("critical", False):
            if agent_name in self._agents:
                self._log.info("Attempting to restart agent: {}", agent_name)
                try:
                    await self._agents[agent_name].stop()
                    await asyncio.sleep(2)
                    await self._agents[agent_name].start()
                    self._log.info("Agent restarted: {}", agent_name)
                except Exception as e:
                    self._log.error("Failed to restart agent {}: {}", agent_name, e)

    async def _handle_risk_breach(self, event: Event) -> None:
        """Handle risk limit breach - pause trading."""
        self._log.critical("RISK LIMIT BREACH: {}", event.data)
        await self.pause()

    async def _handle_stop_loss(self, event: Event) -> None:
        """Handle stop-loss trigger."""
        self._log.warning("STOP LOSS TRIGGERED: {}", event.data)

    def _get_stop_order(self) -> list[str]:
        """Get optimal agent stop order (execution first, data last)."""
        priority = {
            "ExecutionAgent": 0,
            "PortfolioAgent": 1,
            "RiskAgent": 2,
            "AnalysisAgent": 3,
            "SentimentAgent": 4,
            "MarketDataAgent": 5,
        }
        return sorted(
            self._agents.keys(),
            key=lambda n: priority.get(n, 99),
        )

    def get_system_status(self) -> dict:
        """Get comprehensive system status."""
        uptime = None
        if self._start_time:
            uptime = (datetime.now(timezone.utc) - self._start_time).total_seconds()

        return {
            "state": self._state.name,
            "uptime_seconds": uptime,
            "agents": {
                name: {
                    "state": agent.state.name if hasattr(agent, "state") else "UNKNOWN",
                    "last_heartbeat": (
                        agent._last_heartbeat.isoformat()
                        if hasattr(agent, "_last_heartbeat") and agent._last_heartbeat
                        else None
                    ),
                }
                for name, agent in self._agents.items()
            },
            "event_bus": self.event_bus.get_stats(),
            "scheduler": self.scheduler.get_stats(),
        }
