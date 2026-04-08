"""QuantPulse v3 - AuditLogger with SQLite backend (async-safe)."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .enums import AgentRole
from .models import AuditEntry

logger = logging.getLogger("quantpulse.audit")

DEFAULT_DB_PATH = "data/audit.db"


class AuditLogger:
    """
    Persistent audit log backed by SQLite.

    Every agent decision, signal, veto, order, and error is recorded.
    Runs SQLite writes in a thread executor to avoid blocking the event loop.

    Features:
    - Async-safe write via executor
    - Query by agent, action, time range
    - Export to JSON
    - Auto-rotation (configurable max entries)
    """

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        max_entries: int = 100_000,
    ) -> None:
        self._db_path = db_path
        self._max_entries = max_entries
        self._conn: sqlite3.Connection | None = None
        self._write_lock = asyncio.Lock()
        self._entry_count = 0

    # ── Lifecycle ──────────────────────────────

    async def initialize(self) -> None:
        """Create DB and table if not exists."""
        db_dir = Path(self._db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._init_db)
        logger.info(f"AuditLogger initialized: {self._db_path}")

    def _init_db(self) -> None:
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id TEXT PRIMARY KEY,
                agent TEXT NOT NULL,
                action TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '{}',
                correlation_id TEXT,
                timestamp TEXT NOT NULL,
                created_at REAL NOT NULL
            )
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_audit_agent ON audit_log(agent)
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action)
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(created_at)
        """)
        self._conn.commit()
        cursor = self._conn.execute("SELECT COUNT(*) FROM audit_log")
        self._entry_count = cursor.fetchone()[0]

    async def close(self) -> None:
        if self._conn:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._conn.close)
            self._conn = None
            logger.info("AuditLogger closed")

    # ── Write ──────────────────────────────────

    async def log(self, entry: AuditEntry) -> None:
        """Log an audit entry (async-safe)."""
        if not self._conn:
            logger.warning("AuditLogger not initialized, dropping entry")
            return

        async with self._write_lock:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._insert, entry)
            self._entry_count += 1

            # Auto-rotate if needed
            if self._entry_count > self._max_entries:
                await loop.run_in_executor(None, self._rotate)

        logger.debug(
            f"[AUDIT] {entry.agent.value}/{entry.action}: "
            f"{json.dumps(entry.detail, default=str)[:200]}"
        )

    def _insert(self, entry: AuditEntry) -> None:
        assert self._conn is not None
        self._conn.execute(
            """INSERT INTO audit_log (id, agent, action, detail, correlation_id, timestamp, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                entry.id,
                entry.agent.value,
                entry.action,
                json.dumps(entry.detail, default=str),
                entry.correlation_id,
                entry.timestamp.isoformat(),
                entry.timestamp.timestamp(),
            ),
        )
        self._conn.commit()

    def _rotate(self) -> None:
        """Keep only the most recent max_entries / 2 entries."""
        assert self._conn is not None
        keep = self._max_entries // 2
        self._conn.execute(f"""
            DELETE FROM audit_log WHERE id NOT IN (
                SELECT id FROM audit_log ORDER BY created_at DESC LIMIT {keep}
            )
        """)
        self._conn.commit()
        self._entry_count = keep
        logger.info(f"AuditLogger rotated, kept {keep} entries")

    # ── Query ──────────────────────────────────

    async def query(
        self,
        agent: AgentRole | None = None,
        action: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Query audit log with filters."""
        if not self._conn:
            return []

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._query_sync, agent, action, since, until, limit
        )

    def _query_sync(
        self,
        agent: AgentRole | None,
        action: str | None,
        since: datetime | None,
        until: datetime | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        assert self._conn is not None
        conditions: list[str] = []
        params: list[Any] = []

        if agent:
            conditions.append("agent = ?")
            params.append(agent.value)
        if action:
            conditions.append("action = ?")
            params.append(action)
        if since:
            conditions.append("created_at >= ?")
            params.append(since.timestamp())
        if until:
            conditions.append("created_at <= ?")
            params.append(until.timestamp())

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT * FROM audit_log {where} ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        cursor = self._conn.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        results = []
        for row in rows:
            entry = dict(zip(columns, row))
            entry["detail"] = json.loads(entry["detail"])
            results.append(entry)
        return results

    async def count(self, agent: AgentRole | None = None, action: str | None = None) -> int:
        """Count entries with optional filters."""
        if not self._conn:
            return 0
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._count_sync, agent, action)

    def _count_sync(self, agent: AgentRole | None, action: str | None) -> int:
        assert self._conn is not None
        conditions: list[str] = []
        params: list[Any] = []
        if agent:
            conditions.append("agent = ?")
            params.append(agent.value)
        if action:
            conditions.append("action = ?")
            params.append(action)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        cursor = self._conn.execute(f"SELECT COUNT(*) FROM audit_log {where}", params)
        return cursor.fetchone()[0]

    # ── Export ─────────────────────────────────

    async def export_json(self, filepath: str = "data/audit_export.json") -> str:
        """Export full audit log to JSON."""
        entries = await self.query(limit=self._max_entries)
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        import aiofiles
        async with aiofiles.open(filepath, "w") as f:
            await f.write(json.dumps(entries, indent=2, default=str))
        logger.info(f"Exported {len(entries)} audit entries to {filepath}")
        return filepath

    @property
    def entry_count(self) -> int:
        return self._entry_count
