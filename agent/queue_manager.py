"""
Offline-first sync queue using SQLite and aiosqlite.
Durable queue for operations that can be synced when back online.
Payloads are signed with Ed25519 before insertion (see agent.crypto).
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite

from agent.crypto import DEFAULT_KEY_DIR, sign_payload_b64

DB_NAME = "sync_queue.db"
TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS queue (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    signature TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_queue_status_created ON queue(status, created_at);
"""


def _default_encoder(obj: Any) -> str:
    """Encode payload to string for storage."""
    if isinstance(obj, str):
        return obj
    if isinstance(obj, (dict, list)):
        return json.dumps(obj)
    return str(obj)


class QueueManager:
    """
    Async queue manager for offline-first syncing.
    Uses SQLite (sync_queue.db) for durable, local-first storage.
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        key_dir: str | Path | None = None,
    ):
        self._db_path = Path(db_path or DB_NAME)
        self._key_dir = Path(key_dir) if key_dir is not None else DEFAULT_KEY_DIR
        self._conn: aiosqlite.Connection | None = None

    async def _get_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            self._conn = await aiosqlite.connect(self._db_path)
            self._conn.row_factory = aiosqlite.Row
            await self._conn.executescript(TABLE_SCHEMA)
            await self._conn.commit()
        return self._conn

    async def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> "QueueManager":
        await self._get_conn()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def enqueue(self, payload: str | dict | list | Any) -> str:
        """
        Add an item to the queue. Returns the assigned id.
        Payload is stored as text (JSON for dict/list, else string).
        Payload is signed with Ed25519 before insert; signature stored alongside.
        """
        payload_str = _default_encoder(payload)
        payload_bytes = payload_str.encode("utf-8")
        signature = sign_payload_b64(payload_bytes, key_dir=self._key_dir)
        id_ = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc).isoformat()

        conn = await self._get_conn()
        await conn.execute(
            """
            INSERT INTO queue (id, payload, signature, status, created_at)
            VALUES (?, ?, ?, 'pending', ?)
            """,
            (id_, payload_str, signature, created_at),
        )
        await conn.commit()
        return id_

    async def get_pending(self, limit: int = 100) -> list[dict[str, Any]]:
        """
        Return pending items, oldest first, up to limit.
        Suitable for a sync worker to process when online.
        """
        conn = await self._get_conn()
        cursor = await conn.execute(
            """
            SELECT id, payload, signature, status, created_at
            FROM queue
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cursor.fetchall()
        return [
            {
                "id": r["id"],
                "payload": r["payload"],
                "signature": r["signature"],
                "status": r["status"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    async def mark_synced(self, id: str) -> None:
        """Mark the queue item as synced (successfully sent)."""
        conn = await self._get_conn()
        await conn.execute(
            "UPDATE queue SET status = 'synced' WHERE id = ?",
            (id,),
        )
        await conn.commit()
