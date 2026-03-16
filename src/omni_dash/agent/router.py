"""Adaptive model routing for the Dash agent.

Routes simple queries to Haiku (fast, cheap, higher rate limits) and
complex dashboard-building requests to Sonnet (powerful, reliable
multi-tool orchestration).

Design:
    - Rule-based classification (~0ms, zero API cost)
    - Haiku: lookups, queries, listings, status checks
    - Sonnet: dashboard building, analysis, multi-step workflows
    - Default to Haiku — it covers ~70% of Slack messages
"""

from __future__ import annotations

import logging
import os
import re
from enum import Enum

logger = logging.getLogger(__name__)

# Model IDs
_HAIKU_MODEL = "claude-haiku-4-5-20251001"
_SONNET_MODEL = "claude-sonnet-4-5-20250929"


class ModelTier(Enum):
    """Which model tier to route a request to."""

    HAIKU = _HAIKU_MODEL
    SONNET = _SONNET_MODEL


# Patterns that signal complex work requiring Sonnet.
# Order matters — first match wins.
_SONNET_PATTERNS: list[re.Pattern[str]] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\b(?:build|create|generate|design|make)\b.*\b(?:dashboard|chart|report|viz)\b",
        r"\b(?:dashboard|chart|report|viz)\b.*\b(?:build|create|generate|design|make)\b",
        r"\b(?:analyze|analysis|insight|trends?|patterns?|investigate)\b",
        r"\bcompare\b.*\b(?:across|between|over\s+time)\b",
        r"\b(?:update|modify|change|edit)\b.*\b(?:multiple|several|all)\b.*\btile",
        r"\bwhy\b.*\b(?:drop|increase|spike|decline|change)\b",
        r"\b(?:complex|detailed|comprehensive|deep\s+dive)\b",
        r"\b(?:strategy|recommend|suggest|advise)\b",
        r"\badd\b.*\btile",
    ]
]

# Patterns that signal simple work suitable for Haiku.
_HAIKU_PATTERNS: list[re.Pattern[str]] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"^(?:list|show|get|fetch|what)\b.*\b(?:dashboard|topic|folder|field|table)",
        r"\b(?:ping|status|health|help|hello|hi|hey)\b",
        r"^(?:how\s+many|count)\b",
        r"\b(?:query|preview|sample)\b.*\b(?:data|table|rows)\b",
        r"\bprofile\b.*\b(?:data|table|field)\b",
        r"\b(?:filter|sort)\b.*\b(?:by|on)\b",
        r"\b(?:clone|delete|move|export|import)\b.*\bdashboard\b",
    ]
]


def classify_intent(message: str) -> ModelTier:
    """Classify a user message to determine which model to use.

    Returns:
        ModelTier.HAIKU for simple queries, ModelTier.SONNET for complex ones.
    """
    text = message.strip()

    # Very short messages are almost always simple
    word_count = len(text.split())
    if word_count <= 3:
        return ModelTier.HAIKU

    # Check for Sonnet signals first (complex wins over simple)
    for pattern in _SONNET_PATTERNS:
        if pattern.search(text):
            return ModelTier.SONNET

    # Check for Haiku signals
    for pattern in _HAIKU_PATTERNS:
        if pattern.search(text):
            return ModelTier.HAIKU

    # Long, ambiguous messages → Sonnet (more likely to need reasoning)
    if word_count > 25:
        return ModelTier.SONNET

    # Default to Haiku
    return ModelTier.HAIKU


def get_model_for_message(message: str) -> str:
    """Return the model ID to use for a given user message.

    Respects ``DASH_CLAUDE_MODEL`` env var as an override — if set,
    always uses that model regardless of classification.

    Returns:
        Model ID string (e.g. ``claude-haiku-4-5-20251001``).
    """
    # Env var override — bypasses routing entirely
    override = os.environ.get("DASH_CLAUDE_MODEL")
    if override:
        return override

    tier = classify_intent(message)
    logger.info("Routed to %s: %s", tier.name, message[:60])
    return tier.value
