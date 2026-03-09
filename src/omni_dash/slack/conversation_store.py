"""SQLite-backed per-thread conversation persistence.

Stores Anthropic-format message lists keyed by Slack thread
(``channel_id:thread_ts``).  Thread-safe via :class:`threading.Lock`.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

# Rough token budget — keep conversation under ~150K tokens.
# Estimate: 1 token ≈ 4 chars of JSON.
_MAX_JSON_CHARS = 600_000  # ~150K tokens
_KEEP_RECENT = 10  # Always keep last N messages


class ConversationStore:
    """SQLite-backed conversation persistence.

    Args:
        db_path: Path to SQLite database file.
    """

    def __init__(self, db_path: str = "/app/data/conversations.db") -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self) -> None:
        with self._lock:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS conversations (
                    thread_key TEXT PRIMARY KEY,
                    messages TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.commit()
            conn.close()

    def get(self, thread_key: str) -> list[dict[str, Any]] | None:
        """Load messages for a thread, or ``None`` if not found."""
        with self._lock:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            try:
                row = conn.execute(
                    "SELECT messages FROM conversations WHERE thread_key = ?",
                    (thread_key,),
                ).fetchone()
                if row is None:
                    return None
                return json.loads(row[0])
            finally:
                conn.close()

    def put(self, thread_key: str, messages: list[dict[str, Any]]) -> None:
        """Save messages for a thread (upsert)."""
        data = json.dumps(messages, default=str)
        with self._lock:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            try:
                conn.execute(
                    """
                    INSERT INTO conversations (thread_key, messages, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(thread_key) DO UPDATE SET
                        messages = excluded.messages,
                        updated_at = excluded.updated_at
                    """,
                    (thread_key, data, time.time()),
                )
                conn.commit()
            finally:
                conn.close()

    def cleanup(self, max_age_days: int = 7) -> int:
        """Delete conversations older than *max_age_days*.  Returns count deleted."""
        cutoff = time.time() - max_age_days * 86400
        with self._lock:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            try:
                cursor = conn.execute(
                    "DELETE FROM conversations WHERE updated_at < ?",
                    (cutoff,),
                )
                conn.commit()
                return cursor.rowcount
            finally:
                conn.close()

    @staticmethod
    def trim_to_budget(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Trim messages to fit within the token budget.

        Strategy: keep the first message (original user request) +
        the last ``_KEEP_RECENT`` messages.  Drop the middle.
        """
        serialized = json.dumps(messages, default=str)
        if len(serialized) <= _MAX_JSON_CHARS:
            return messages

        if len(messages) <= _KEEP_RECENT + 1:
            return messages

        trimmed = [messages[0]] + messages[-_KEEP_RECENT:]
        return trimmed
