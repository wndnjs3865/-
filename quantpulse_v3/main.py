"""QuantPulse v3 - Main entry point."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from config import Config
from core.audit_logger import AuditLogger
from core.message_bus import MessageBus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-30s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("quantpulse.main")


async def main() -> None:
    """Boot the QuantPulse v3 system."""
    config = Config.from_env()
    logging.getLogger().setLevel(config.system.log_level)

    logger.info("=" * 60)
    logger.info("  QuantPulse v3 — Multi-Agent Quant Cloud System")
    logger.info(f"  Mode: {config.trading.mode}")
    logger.info("=" * 60)

    # ── Initialize core infrastructure ─────────
    audit = AuditLogger(
        db_path=config.system.audit_db_path,
        max_entries=config.system.audit_max_entries,
    )
    await audit.initialize()

    bus = MessageBus(audit_logger=audit)
    await bus.start()

    logger.info("[CORE] MessageBus started")
    logger.info("[CORE] AuditLogger initialized")
    logger.info(f"[CORE] Audit DB: {config.system.audit_db_path}")

    # ── Phase 2: Agents will be initialized here ─
    # from agents.mia import MIAAgent
    # from agents.qr import QRAgent
    # from agents.cso import CSOAgent
    # from agents.crco import CRCOAgent
    # from agents.es import ESAgent
    # from agents.po import POAgent
    # from agents.ima import IMAAgent
    #
    # agents = [
    #     MIAAgent(bus, audit, config),
    #     QRAgent(bus, audit, config),
    #     CSOAgent(bus, audit, config),
    #     CRCOAgent(bus, audit, config),
    #     ESAgent(bus, audit, config),
    #     POAgent(bus, audit, config),
    #     IMAAgent(bus, audit, config),
    # ]
    # for agent in agents:
    #     await agent.start()

    # ── Phase 5: FastAPI server will be started here ─
    # from api.server import create_app
    # app = create_app(bus, audit, config, agents)

    logger.info("[CORE] System ready. Phase 1 infrastructure operational.")
    logger.info("[CORE] Waiting for Phase 2 (Agents) implementation...")

    # ── Graceful shutdown ──────────────────────
    shutdown_event = asyncio.Event()

    def handle_signal(sig: int, frame: object) -> None:
        logger.info(f"Received signal {sig}, shutting down...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Keep running until shutdown
    await shutdown_event.wait()

    # Cleanup
    logger.info("[CORE] Shutting down...")
    await bus.stop()
    await audit.close()
    logger.info("[CORE] QuantPulse v3 stopped cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
