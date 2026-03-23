"""
Local persistence layer for the on-premise agent.

Architecture notes
------------------
- The agent maintains a lightweight local store (e.g. SQLite via `aiosqlite`)
  to track:
    - Last synced checkpoints (per ledger/voucher type).
    - Outgoing sync queue (payloads waiting to be sent to the server).
    - Mapping from Tally identifiers to platform identifiers, once known.
- This decouples the agent from the cloud backend and allows it to:
    - Recover from connectivity issues.
    - Resume incremental sync without re-reading all history from Tally.

This module provides an abstraction that can be easily swapped out if
the storage backend changes.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import aiosqlite


DEFAULT_DB_PATH = Path("./agent_state.db")


@dataclass
class AgentState:
    """
    In-memory representation of key agent state.

    This may be extended over time, but the core idea is that all persistent
    state required for incremental sync is captured here and mirrored in the DB.
    """

    last_ledger_sync_token: Optional[str] = None
    last_voucher_sync_token: Optional[str] = None
    queue_status: Optional[str] = None  # e.g. NORMAL, RECONCILING, STALE


class Persistence:
    """
    Thin async wrapper around SQLite for agent state and outbound queue.
    """

    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def init(self) -> None:
        """
        Initialize the database schema if needed.
        """
        self._db = await aiosqlite.connect(self.db_path.as_posix())
        await self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS agent_state (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS outbound_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_type TEXT NOT NULL DEFAULT 'NORMAL',
                sync_id TEXT,
                payload TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        # Lightweight migration for older DBs missing queue_type.
        async with self._db.execute("PRAGMA table_info(outbound_queue);") as cursor:
            cols = [row[1] async for row in cursor]
        if "queue_type" not in cols:
            await self._db.execute(
                "ALTER TABLE outbound_queue ADD COLUMN queue_type TEXT NOT NULL DEFAULT 'NORMAL';"
            )
        if "sync_id" not in cols:
            await self._db.execute("ALTER TABLE outbound_queue ADD COLUMN sync_id TEXT;")
            # Backfill existing rows with a deterministic legacy sync id so they remain addressable.
            await self._db.execute(
                "UPDATE outbound_queue SET sync_id = printf('legacy-%d', id) WHERE sync_id IS NULL;"
            )
        await self._db.commit()

    async def get_state(self) -> AgentState:
        """
        Load the current agent state from the database.
        """
        assert self._db is not None, "Database not initialized"
        state = AgentState()
        async with self._db.execute("SELECT key, value FROM agent_state") as cursor:
            async for key, value in cursor:
                if key == "last_ledger_sync_token":
                    state.last_ledger_sync_token = value
                elif key == "last_voucher_sync_token":
                    state.last_voucher_sync_token = value
                elif key == "queue_status":
                    state.queue_status = value
        return state

    async def set_state(self, updates: Dict[str, Any]) -> None:
        """
        Persist a partial update to the agent state.
        """
        assert self._db is not None, "Database not initialized"
        for key, value in updates.items():
            await self._db.execute(
                """
                INSERT INTO agent_state(key, value)
                VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value;
                """,
                (key, str(value)),
            )
        await self._db.commit()

    async def enqueue_payload(
        self, payload: str, *, sync_id: str, queue_type: str = "NORMAL"
    ) -> None:
        """
        Add an encrypted payload to the outbound queue.
        """
        assert self._db is not None, "Database not initialized"
        await self._db.execute(
            "INSERT INTO outbound_queue(queue_type, sync_id, payload) VALUES(?, ?, ?)",
            (queue_type, sync_id, payload),
        )
        await self._db.commit()

    async def fetch_next_payload(
        self, *, queue_type: str = "NORMAL"
    ) -> Optional[Tuple[str, str]]:
        """
        Retrieve and delete the oldest payload from the queue.
        """
        assert self._db is not None, "Database not initialized"
        async with self._db.execute(
            """
            SELECT id, sync_id, payload
            FROM outbound_queue
            WHERE queue_type = ?
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            """,
            (queue_type,),
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            return None
        payload_id, sync_id, payload = row
        await self._db.execute("DELETE FROM outbound_queue WHERE id = ?", (payload_id,))
        await self._db.commit()
        return str(sync_id), str(payload)

    async def clear_agent_state(self) -> None:
        """
        Clear agent state (sync tokens, queue_status).
        """
        assert self._db is not None, "Database not initialized"
        await self._db.executescript(
            """
            DELETE FROM agent_state;
            """
        )
        await self._db.commit()

    async def clear_queue(self, *, queue_type: Optional[str] = None) -> None:
        """
        Clear queued payloads.

        If queue_type is provided, only that queue is cleared.
        """
        assert self._db is not None, "Database not initialized"
        if queue_type is None:
            await self._db.execute("DELETE FROM outbound_queue;")
        else:
            await self._db.execute("DELETE FROM outbound_queue WHERE queue_type = ?", (queue_type,))
        await self._db.commit()

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None


__all__ = ["Persistence", "AgentState", "DEFAULT_DB_PATH"]

