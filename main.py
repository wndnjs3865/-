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

        # Exchange + Price Feed
        self._exchange: Any | None = None
        self._price_feed: Any | None = None

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
        net = "TESTNET" if self.config.exchange.testnet else "MAINNET"
        logger.info(f"  Mode: {self.config.trading.mode} | Network: {net}")
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

        # ── Step 5: Connect exchange + price feed (if API keys present) ──
        await self._init_exchange()

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

    # ── Exchange Init ─────────────────────────

    async def _init_exchange(self) -> None:
        """Initialize exchange connector and price feed if API keys are configured."""
        api_key = self.config.exchange.binance_api_key
        api_secret = self.config.exchange.binance_api_secret

        if not api_key or not api_secret:
            logger.info("[BOOT] No exchange API keys — running in Paper-only mode")
            return

        try:
            from exchanges.binance_futures import BinanceFuturesExchange
            from core.price_feed import PriceFeed

            is_testnet = self.config.exchange.testnet
            self._exchange = BinanceFuturesExchange(
                api_key=api_key,
                api_secret=api_secret,
                testnet=is_testnet,
            )
            await self._exchange.connect()

            # Wire exchange to ES + MIA
            self.es.set_exchange(self._exchange)
            self.mia.set_exchange(self._exchange)

            # Start price feed → MIA + ES
            self._price_feed = PriceFeed(
                exchange=self._exchange,
                symbols=["BTC/USDT:USDT", "ETH/USDT:USDT"],
                interval=self.config.system.price_feed_interval,
            )
            self._price_feed.set_consumers(mia=self.mia, es=self.es)
            await self._price_feed.start()

            logger.info("[BOOT] Exchange connected + price feed started")
        except Exception as e:
            logger.warning(f"[BOOT] Exchange init failed (Paper mode continues): {e}")
            self._exchange = None
            self._price_feed = None

    # ── Stop ───────────────────────────────────

    async def stop(self) -> None:
        """Graceful shutdown: price feed → agents → bus → audit."""
        if not self._running:
            return
        self._running = False
        logger.info("[SHUTDOWN] Stopping system...")

        # Stop price feed first
        if self._price_feed:
            await self._price_feed.stop()
            logger.info("[SHUTDOWN] Price feed stopped")

        # Disconnect exchange
        if self._exchange:
            try:
                await self._exchange.disconnect()
                logger.info("[SHUTDOWN] Exchange disconnected")
            except Exception as e:
                logger.error(f"[SHUTDOWN] Exchange disconnect error: {e}")

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

    # ── Live Mode Safety Gate ─────────────────

    async def validate_live_readiness(self) -> dict[str, Any]:
        """
        Run CRCO's 3-stage safety gate before allowing live trading.
        This MUST be called and pass before any live order is executed.
        """
        if not self.crco:
            return {"passed": False, "blockers": ["System not started"]}

        result = self.crco.run_pre_live_checklist()

        if self.audit:
            await self.audit.log(AuditEntry(
                agent=AgentRole.CRCO,
                action="live_readiness_check",
                detail=result,
            ))

        if result["passed"]:
            logger.info("[SAFETY] Live readiness check PASSED — all 3 gates clear")
        else:
            logger.warning(
                f"[SAFETY] Live readiness check FAILED — "
                f"{len(result['blockers'])} blockers: {result['blockers']}"
            )

        return result

    async def emergency_stop(self) -> dict[str, Any]:
        """
        EMERGENCY STOP: Close all positions + activate circuit breaker + halt system.

        Call this when:
        - Manual panic button
        - Unrecoverable error detected
        - API key compromise suspected
        """
        logger.critical("[EMERGENCY] EMERGENCY STOP INITIATED")

        result: dict[str, Any] = {"timestamp": datetime.now(timezone.utc).isoformat()}

        # Step 1: Activate circuit breaker
        if self.crco:
            self.crco.set_circuit_breaker(True)
            result["circuit_breaker"] = True

        # Step 2: Close all ES positions
        if self.es:
            es_result = await self.es.emergency_stop()
            result["es_emergency"] = es_result

        # Step 3: Log
        if self.audit:
            await self.audit.log(AuditEntry(
                agent=AgentRole.IMA,
                action="emergency_stop",
                detail=result,
            ))

        logger.critical(
            f"[EMERGENCY] Stop complete. "
            f"Positions closed: {result.get('es_emergency', {}).get('positions_closed', 0)}"
        )
        return result

    async def switch_mode(self, new_mode: str) -> dict[str, Any]:
        """
        Safely switch between PAPER and LIVE mode.

        PAPER → LIVE: Requires 3-stage safety gate pass.
        LIVE → PAPER: Always allowed (safe direction).
        """
        old_mode = self.config.trading.mode
        new_mode = new_mode.upper()

        if new_mode not in ("PAPER", "LIVE"):
            return {"success": False, "error": f"Invalid mode: {new_mode}"}

        if old_mode == new_mode:
            return {"success": True, "message": f"Already in {new_mode} mode"}

        # LIVE → PAPER: always allowed
        if new_mode == "PAPER":
            self.config.trading.mode = "PAPER"
            logger.info(f"[MODE] Switched {old_mode} → PAPER")
            if self.audit:
                await self.audit.log(AuditEntry(
                    agent=AgentRole.IMA,
                    action="mode_switch",
                    detail={"from": old_mode, "to": "PAPER"},
                ))
            return {"success": True, "from": old_mode, "to": "PAPER"}

        # PAPER → LIVE: requires safety gate
        self.config.trading.mode = "LIVE"  # Temporarily set for gate check
        gate_result = await self.validate_live_readiness()

        if not gate_result["passed"]:
            self.config.trading.mode = "PAPER"  # Revert
            logger.warning("[MODE] Switch to LIVE blocked by safety gate")
            return {
                "success": False,
                "error": "Safety gate failed",
                "blockers": gate_result["blockers"],
            }

        logger.warning(f"[MODE] Switched {old_mode} → LIVE — REAL MONEY AT RISK")
        if self.audit:
            await self.audit.log(AuditEntry(
                agent=AgentRole.IMA,
                action="mode_switch",
                detail={"from": old_mode, "to": "LIVE", "gate_result": gate_result},
            ))
        return {"success": True, "from": old_mode, "to": "LIVE", "gate_result": gate_result}

    # ── System Status ──────────────────────────

    # ── Agent Watchdog ──────────────────────────

    async def start_watchdog(self, interval: int = 60) -> None:
        """Start background watchdog that monitors agent health and auto-restarts."""
        self._watchdog_task = asyncio.create_task(
            self._watchdog_loop(interval), name="system_watchdog"
        )

    async def _watchdog_loop(self, interval: int) -> None:
        """Periodically check all agents and restart any that have crashed."""
        while self._running:
            for agent in self._agents:
                if not agent.is_running and self._running:
                    logger.warning(f"[WATCHDOG] {agent.role.value} is down — restarting")
                    try:
                        await agent.start()
                        logger.info(f"[WATCHDOG] {agent.role.value} restarted successfully")
                        if self.audit:
                            await self.audit.log(AuditEntry(
                                agent=AgentRole.IMA,
                                action="agent_auto_restarted",
                                detail={"agent": agent.role.value},
                            ))
                    except Exception as e:
                        logger.error(f"[WATCHDOG] Failed to restart {agent.role.value}: {e}")
            await asyncio.sleep(interval)

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
