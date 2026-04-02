"""Persistent learnings system for the Dash agent.

Inspired by Garry Tan's gstack JSONL learnings pattern. Stores actionable
corrections and discovered patterns as append-only JSONL entries with
confidence decay over time. Every request searches past learnings and
injects relevant ones into the system prompt.

The agent gets smarter the more it's used.
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

logger = logging.getLogger(__name__)

# Confidence decays by this much per 30 days
_DECAY_PER_30_DAYS = 0.1

# Minimum confidence to include in context
_MIN_CONFIDENCE = 0.3


@dataclass
class Learning:
    """A single learning entry."""

    skill: str  # "dashboard_building", "data_discovery", "omni_quirk", "metric_def"
    type: str  # "pattern", "pitfall", "metric_definition", "schema_quirk", "preference"
    key: str  # Dedup key: "customer_type_is_plg_slg"
    insight: str  # "customer_type values are PLG and SLG, not free/paid/trial"
    confidence: float = 1.0  # 0.0-1.0
    source: str = "user_correction"  # "user_correction", "agent_discovery", "system"
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def effective_confidence(self) -> float:
        """Apply time-based confidence decay."""
        try:
            created = datetime.fromisoformat(self.ts)
            age_days = (datetime.now(timezone.utc) - created).days
            decay = (age_days / 30) * _DECAY_PER_30_DAYS
            return max(0.0, self.confidence - decay)
        except (ValueError, TypeError):
            return self.confidence


class LearningsStore:
    """Persistent JSONL-based learnings with search and confidence decay."""

    def __init__(self, path: str | None = None) -> None:
        resolved = path or os.environ.get(
            "DASH_LEARNINGS_PATH", "/app/data/learnings.jsonl"
        )
        self._path = Path(resolved)
        self._cache: list[Learning] | None = None

    def _ensure_dir(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> list[Learning]:
        """Load all learnings from disk, deduplicating by key+type."""
        if self._cache is not None:
            return self._cache

        entries: dict[str, Learning] = {}  # key:type → latest entry

        if self._path.exists():
            try:
                for line in self._path.read_text().splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        learning = Learning(**{
                            k: v for k, v in data.items()
                            if k in Learning.__dataclass_fields__
                        })
                        # Latest entry per key+type wins (dedup)
                        dedup_key = f"{learning.key}:{learning.type}"
                        entries[dedup_key] = learning
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.debug("Skipping malformed learning: %s", e)
            except OSError as e:
                logger.warning("Could not read learnings: %s", e)

        self._cache = list(entries.values())
        return self._cache

    def add(self, learning: Learning) -> None:
        """Append a new learning. Invalidates cache."""
        self._ensure_dir()
        try:
            with open(self._path, "a") as f:
                f.write(json.dumps(asdict(learning)) + "\n")
            self._cache = None  # Invalidate
            logger.info("Learning saved: [%s] %s", learning.key, learning.insight[:60])
        except OSError as e:
            logger.error("Failed to save learning: %s", e)

    def add_from_text(
        self,
        insight: str,
        *,
        skill: str = "general",
        learning_type: str = "pitfall",
        source: str = "user_correction",
    ) -> Learning:
        """Create and save a learning from a plain text insight.

        Generates a dedup key from the insight text.
        """
        # Generate key from insight: lowercase, strip punctuation, join with _
        words = re.findall(r"[a-z0-9]+", insight.lower())
        key = "_".join(words[:8])  # First 8 words as key

        learning = Learning(
            skill=skill,
            type=learning_type,
            key=key,
            insight=insight,
            source=source,
        )
        self.add(learning)
        return learning

    def search(self, query: str, top_k: int = 5) -> list[Learning]:
        """Search learnings by keyword, returning most relevant with decay applied.

        Args:
            query: Search terms (natural language).
            top_k: Max results.

        Returns:
            Learnings sorted by relevance, filtered by minimum confidence.
        """
        all_learnings = self._load()
        if not all_learnings:
            return []

        query_terms = set(re.findall(r"[a-z0-9]+", query.lower()))
        if not query_terms:
            # Return highest-confidence learnings if no query
            valid = [l for l in all_learnings if l.effective_confidence() >= _MIN_CONFIDENCE]
            valid.sort(key=lambda l: l.effective_confidence(), reverse=True)
            return valid[:top_k]

        scored: list[tuple[float, Learning]] = []
        for learning in all_learnings:
            conf = learning.effective_confidence()
            if conf < _MIN_CONFIDENCE:
                continue

            # Score by term overlap in key, insight, and skill
            searchable = f"{learning.key} {learning.insight} {learning.skill} {learning.type}".lower()
            matches = sum(1 for t in query_terms if t in searchable)
            if matches > 0:
                score = matches * conf
                scored.append((score, learning))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [l for _, l in scored[:top_k]]

    def to_context_block(self, query: str = "") -> str:
        """Format relevant learnings for system prompt injection.

        Args:
            query: The user's current message, for relevance filtering.

        Returns:
            Formatted text block, or empty string if no relevant learnings.
        """
        results = self.search(query, top_k=8)
        if not results:
            return ""

        lines = ["# Relevant Learnings (from past interactions)\n"]
        for learning in results:
            conf = learning.effective_confidence()
            conf_label = "HIGH" if conf >= 0.8 else "MED" if conf >= 0.5 else "LOW"
            lines.append(f"- [{conf_label}] {learning.insight}")

        return "\n".join(lines)

    def count(self) -> int:
        """Total number of deduplicated learnings."""
        return len(self._load())


# Module-level singleton
_store: LearningsStore | None = None


def get_learnings_store() -> LearningsStore:
    """Get or create the module-level LearningsStore singleton."""
    global _store
    if _store is None:
        _store = LearningsStore()
    return _store
