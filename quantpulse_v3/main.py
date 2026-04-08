"""QuantPulse v3 - JWQuantSystem: Full System Orchestrator.

Single entry point to boot, run, and manage all 7 agents.
Designed for 24/7 cloud deployment (Railway, Replit, Docker).
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from config import Config
from core.audit_logger import AuditLogger
from core.message_bus import MessageBus
from core.models import AuditEntry
from core.enums import AgentRole

from agents.mia import MIAAgent
from agents.qr import QRAgent
from agents.cso import CSOAgent
from agents.crco import CRCOAgent
from agents.es import ESAgent
from agents.po import POAgent
from agents.ima import IMAAgent

# ── Logging Setup ──────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-30s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("quantpulse.main")

BANNER = r"""
 ╔══════════════════════════════════════════════════════════╗
 ║       QuantPulse v3 — Multi-Agent Quant Cloud System    ║
 ║       7 Agents · SMC/ICT · Async MessageBus · 24/7      ║
 ╚══════════════════════════════════════════════════════════╝
"""


class JWQuantSystem:
    """
    Master orchestrator that boots and manages the full 7-agent system.

    Lifecycle:
        system = JWQuantSystem.from_env()
        await system.start()   # Boot all infra + agents
        ...
        await system.stop()    # Graceful shutdown
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self._start_time: float = 0.0

        # Core infrastructure
        self.audit: AuditLogger | None = None
        self.bus: MessageBus | None = None

        # Agents (initialized in start)
        self.mia: MIAAgent | None = None
        self.qr: QRAgent | None = None
        self.cso: CSOAgent | None = None
        self.crco: CRCOAgent | None = None
        self.es: ESAgent | None = None
        self.po: POAgent | None = None
        self.ima: IMAAgent | None = None

        self._agents: list[Any] = []
        self._running = False

    @classmethod
    def from_env(cls) -> JWQuantSystem:
        return cls(Config.from_env())

    # ── Start ──────────────────────────────────

    async def start(self) -> None:
        """Boot full system: infrastructure → agents → monitoring."""
        self._start_time = time.time()
        logger.info(BANNER)
        logger.info(f"  Mode: {self.config.trading.mode}")
        logger.info(f"  Risk/Trade: {self.config.trading.max_risk_per_trade*100:.1f}%")
        logger.info(f"  Max Leverage: {self.config.trading.max_leverage}x")
        logger.info(f"  Circuit Breaker: {self.config.trading.circuit_breaker_loss*100:.1f}%")

        # ── Step 1: Core Infrastructure ──
        self.audit = AuditLogger(
            db_path=self.config.system.audit_db_path,
            max_entries=self.config.system.audit_max_entries,
        )
        await self.audit.initialize()

        self.bus = MessageBus(audit_logger=self.audit)
        await self.bus.start()

        logger.info("[BOOT] Core infrastructure ready")

        # ── Step 2: Create Agents ──
        self.mia = MIAAgent(self.bus, self.audit, self.config)
        self.qr = QRAgent(self.bus, self.audit, self.config)
        self.cso = CSOAgent(self.bus, self.audit, self.config)
        self.crco = CRCOAgent(self.bus, self.audit, self.config)
        self.es = ESAgent(self.bus, self.audit, self.config)
        self.po = POAgent(self.bus, self.audit, self.config)
        self.ima = IMAAgent(self.bus, self.audit, self.config)

        self._agents = [self.mia, self.qr, self.cso, self.crco, self.es, self.po, self.ima]

        # ── Step 3: Start Agents (order matters for subscriptions) ──
        # Start consumers first, then producers
        boot_order = [self.ima, self.po, self.es, self.crco, self.qr, self.cso, self.mia]
        for agent in boot_order:
            await agent.start()
            logger.info(f"[BOOT] {agent.role.value} agent started")

        # ── Step 4: Register all agents with IMA ──
        for agent in self._agents:
            self.ima.register_agent(agent)

        self._running = True

        # ── Audit system boot ──
        await self.audit.log(AuditEntry(
            agent=AgentRole.IMA,
            action="system_boot",
            detail={
                "mode": self.config.trading.mode,
                "agents": [a.role.value for a in self._agents],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        ))

        logger.info("=" * 58)
        logger.info("  ALL 7 AGENTS OPERATIONAL")
        logger.info(f"  Pipeline: MIA → QR → CSO → CRCO → ES → PO")
        logger.info(f"  Monitor:  IMA (health every {self.config.system.health_check_interval}s)")
        logger.info("=" * 58)

    # ── Stop ───────────────────────────────────

    async def stop(self) -> None:
        """Graceful shutdown: agents → bus → audit."""
        if not self._running:
            return
        self._running = False
        logger.info("[SHUTDOWN] Stopping system...")

        # Stop agents in reverse boot order
        for agent in reversed(self._agents):
            try:
                await agent.stop()
                logger.info(f"[SHUTDOWN] {agent.role.value} stopped")
            except Exception as e:
                logger.error(f"[SHUTDOWN] Error stopping {agent.role.value}: {e}")

        uptime = int(time.time() - self._start_time)

        if self.audit:
            await self.audit.log(AuditEntry(
                agent=AgentRole.IMA,
                action="system_shutdown",
                detail={"uptime_seconds": uptime},
            ))

        if self.bus:
            await self.bus.stop()
            logger.info("[SHUTDOWN] MessageBus stopped")

        if self.audit:
            await self.audit.close()
            logger.info("[SHUTDOWN] AuditLogger closed")

        logger.info(f"[SHUTDOWN] QuantPulse v3 stopped cleanly. Uptime: {uptime}s")

    # ── System Status ──────────────────────────

    @property
    def system_snapshot(self) -> dict[str, Any]:
        """Full system status for API / monitoring."""
        uptime = int(time.time() - self._start_time) if self._start_time else 0
        return {
            "running": self._running,
            "uptime_seconds": uptime,
            "mode": self.config.trading.mode,
            "bus_metrics": self.bus.metrics if self.bus else {},
            "agents": {
                "mia": self.mia.analysis_snapshot if self.mia else {},
                "qr": self.qr.qr_snapshot if self.qr else {},
                "cso": self.cso.cso_snapshot if self.cso else {},
                "crco": self.crco.risk_snapshot if self.crco else {},
                "es": self.es.execution_snapshot if self.es else {},
                "po": self.po.po_snapshot if self.po else {},
                "ima": self.ima.ima_snapshot if self.ima else {},
            },
        }


# ── CLI Entry Point ───────────────────────────

async def run() -> None:
    """Main entry point for CLI / cloud deployment."""
    system = JWQuantSystem.from_env()
    await system.start()

    # Graceful shutdown on SIGINT/SIGTERM
    shutdown_event = asyncio.Event()

    def handle_signal(sig: int, frame: object) -> None:
        logger.info(f"Received signal {sig}, initiating shutdown...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    await shutdown_event.wait()
    await system.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
