"""Unified memory store: Convex-backed with local JSONL fallback.

Write-through architecture:
- Writes go to BOTH Convex (durable) and local JSONL (fast reads)
- Reads prefer Convex (full-text search) with JSONL fallback if offline
- The agent works offline via JSONL; Convex syncs when available

This replaces the standalone learnings.py for Convex-enabled deployments.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from omni_dash.memory.convex_client import get_convex_client

logger = logging.getLogger(__name__)


@dataclass
class Learning:
    """A single learning entry (same shape as agent/learnings.py)."""
    skill: str
    type: str
    key: str
    insight: str
    confidence: float = 1.0
    source: str = "user_correction"
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class DashboardLog:
    """Record of a dashboard creation attempt."""
    prompt: str
    status: str  # "success", "partial", "fail", "blocked"
    model: str
    tool_calls: int = 0
    duration_ms: int = 0
    dashboard_id: str = ""
    dashboard_url: str = ""
    error_summary: str = ""
    retry_count: int = 0
    tiles_created: int = 0
    tiles_with_data: int = 0
    slack_user_id: str = ""
    slack_channel: str = ""
    thread_ts: str = ""


class MemoryStore:
    """Unified memory: Convex + local JSONL fallback.

    Args:
        jsonl_path: Path to local JSONL fallback file.
    """

    def __init__(self, jsonl_path: str | None = None) -> None:
        resolved = jsonl_path or os.environ.get(
            "DASH_LEARNINGS_PATH", "/app/data/learnings.jsonl"
        )
        self._jsonl_path = Path(resolved)
        self._convex = get_convex_client()

    @property
    def convex_available(self) -> bool:
        return self._convex.configured

    # --- Learnings ---

    def save_learning(self, learning: Learning) -> None:
        """Save a learning to both Convex and local JSONL."""
        # Convex (durable)
        if self.convex_available:
            try:
                self._convex.mutation("learnings:upsert", {
                    "skill": learning.skill,
                    "type": learning.type,
                    "key": learning.key,
                    "insight": learning.insight,
                    "confidence": learning.confidence,
                    "source": learning.source,
                })
            except Exception as e:
                logger.warning("Convex learning save failed: %s", e)

        # Local JSONL (fast fallback)
        self._append_jsonl(asdict(learning))
        logger.info("Learning saved: [%s] %s", learning.key, learning.insight[:60])

    def save_learning_from_text(
        self,
        insight: str,
        *,
        skill: str = "general",
        learning_type: str = "pitfall",
        source: str = "user_correction",
    ) -> Learning:
        """Create and save a learning from plain text."""
        words = re.findall(r"[a-z0-9]+", insight.lower())
        key = "_".join(words[:8])
        learning = Learning(
            skill=skill, type=learning_type, key=key,
            insight=insight, source=source,
        )
        self.save_learning(learning)
        return learning

    def search_learnings(self, query: str, top_k: int = 8) -> list[dict[str, Any]]:
        """Search learnings. Prefers Convex full-text search, falls back to JSONL."""
        if self.convex_available:
            try:
                results = self._convex.query("learnings:search", {
                    "query": query, "limit": float(top_k),
                })
                if results is not None:
                    return results
            except Exception as e:
                logger.debug("Convex search failed, using JSONL: %s", e)

        # JSONL fallback (keyword match)
        return self._search_jsonl(query, top_k)

    def get_recent_learnings(self, limit: int = 20) -> list[dict[str, Any]]:
        """Get recent learnings for context block."""
        if self.convex_available:
            try:
                results = self._convex.query("learnings:listRecent", {
                    "limit": float(limit),
                })
                if results is not None:
                    return results
            except Exception:
                pass
        return self._search_jsonl("", limit)

    def learnings_context_block(self, query: str = "") -> str:
        """Format learnings for system prompt injection."""
        if query:
            results = self.search_learnings(query, top_k=8)
        else:
            results = self.get_recent_learnings(limit=8)

        if not results:
            return ""

        lines = ["# Relevant Learnings (from past interactions)\n"]
        for r in results:
            conf = r.get("effectiveConfidence", r.get("confidence", 1.0))
            label = "HIGH" if conf >= 0.8 else "MED" if conf >= 0.5 else "LOW"
            insight = r.get("insight", "")
            lines.append(f"- [{label}] {insight}")

        return "\n".join(lines)

    # --- Dashboard Logs ---

    def log_dashboard_creation(self, log: DashboardLog) -> None:
        """Log a dashboard creation attempt to Convex."""
        if not self.convex_available:
            return
        try:
            self._convex.mutation("dashboardLogs:log", {
                "prompt": log.prompt,
                "status": log.status,
                "model": log.model,
                "toolCalls": float(log.tool_calls),
                "durationMs": float(log.duration_ms),
                "dashboardId": log.dashboard_id or None,
                "dashboardUrl": log.dashboard_url or None,
                "errorSummary": log.error_summary or None,
                "retryCount": float(log.retry_count),
                "tilesCreated": float(log.tiles_created),
                "tilesWithData": float(log.tiles_with_data),
                "slackUserId": log.slack_user_id or None,
                "slackChannel": log.slack_channel or None,
                "threadTs": log.thread_ts or None,
            })
        except Exception as e:
            logger.warning("Convex dashboard log failed: %s", e)

    def get_dashboard_stats(self) -> dict[str, Any] | None:
        """Get dashboard creation success rate stats."""
        if not self.convex_available:
            return None
        try:
            return self._convex.query("dashboardLogs:stats")
        except Exception:
            return None

    # --- User Preferences ---

    def get_user_preferences(self, slack_user_id: str) -> dict[str, Any] | None:
        """Get preferences for a Slack user."""
        if not self.convex_available:
            return None
        try:
            return self._convex.query("userPreferences:get", {
                "slackUserId": slack_user_id,
            })
        except Exception:
            return None

    def update_user_preferences(
        self,
        slack_user_id: str,
        slack_user_name: str = "",
        **kwargs: Any,
    ) -> None:
        """Update preferences for a Slack user."""
        if not self.convex_available:
            return
        try:
            args: dict[str, Any] = {
                "slackUserId": slack_user_id,
                "slackUserName": slack_user_name or slack_user_id,
            }
            for k in ("preferredChartTypes", "preferredFolder", "notes"):
                if k in kwargs:
                    args[k] = kwargs[k]
            self._convex.mutation("userPreferences:upsert", args)
        except Exception as e:
            logger.warning("Convex user prefs update failed: %s", e)

    # --- Feedback ---

    def save_feedback(
        self,
        slack_user_id: str,
        message: str,
        feedback_type: str = "correction",
        slack_channel: str = "",
        thread_ts: str = "",
        dashboard_id: str = "",
    ) -> None:
        """Save user feedback to Convex."""
        if not self.convex_available:
            return
        try:
            self._convex.mutation("feedback:add", {
                "slackUserId": slack_user_id,
                "type": feedback_type,
                "message": message,
                "slackChannel": slack_channel or None,
                "threadTs": thread_ts or None,
                "dashboardId": dashboard_id or None,
            })
        except Exception as e:
            logger.warning("Convex feedback save failed: %s", e)

    # --- Local JSONL fallback ---

    def _append_jsonl(self, data: dict[str, Any]) -> None:
        """Append to local JSONL file."""
        try:
            self._jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._jsonl_path, "a") as f:
                f.write(json.dumps(data, default=str) + "\n")
        except OSError as e:
            logger.debug("JSONL write failed: %s", e)

    def _search_jsonl(self, query: str, top_k: int) -> list[dict[str, Any]]:
        """Keyword search over local JSONL (fallback)."""
        if not self._jsonl_path.exists():
            return []

        terms = set(re.findall(r"[a-z0-9]+", query.lower())) if query else set()
        entries: dict[str, dict[str, Any]] = {}  # key:type → latest

        try:
            for line in self._jsonl_path.read_text().splitlines():
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    dedup = f"{data.get('key', '')}:{data.get('type', '')}"
                    entries[dedup] = data
                except json.JSONDecodeError:
                    continue
        except OSError:
            return []

        results = list(entries.values())

        if terms:
            scored = []
            for r in results:
                searchable = f"{r.get('key', '')} {r.get('insight', '')} {r.get('skill', '')}".lower()
                matches = sum(1 for t in terms if t in searchable)
                if matches > 0:
                    scored.append((matches, r))
            scored.sort(key=lambda x: x[0], reverse=True)
            results = [r for _, r in scored[:top_k]]
        else:
            results = results[-top_k:]

        return results


# Module-level singleton
_store: MemoryStore | None = None


def get_memory_store() -> MemoryStore:
    """Get or create the MemoryStore singleton."""
    global _store
    if _store is None:
        _store = MemoryStore()
    return _store
