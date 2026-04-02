"""Tests for agent.learnings — JSONL learnings store with confidence decay."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta

import pytest


@pytest.fixture()
def store(tmp_path):
    """Create a LearningsStore with a temp file."""
    from omni_dash.agent.learnings import LearningsStore

    return LearningsStore(path=str(tmp_path / "learnings.jsonl"))


def test_add_and_search(store):
    """Basic add and search."""
    from omni_dash.agent.learnings import Learning

    store.add(Learning(
        skill="dashboard_building",
        type="pitfall",
        key="customer_type_plg_slg",
        insight="customer_type values are PLG and SLG, not free/paid/trial",
    ))

    results = store.search("customer type")
    assert len(results) == 1
    assert "PLG and SLG" in results[0].insight


def test_add_from_text(store):
    """add_from_text generates a key and saves."""
    learning = store.add_from_text("Always sort time series by date ascending")
    assert learning.key  # Should have auto-generated key
    assert learning.insight == "Always sort time series by date ascending"

    results = store.search("sort date time series")
    assert len(results) == 1


def test_dedup_by_key_type(store):
    """Later entries with same key+type overwrite earlier ones."""
    from omni_dash.agent.learnings import Learning

    store.add(Learning(
        skill="general", type="pitfall", key="test_key",
        insight="old version",
    ))
    store.add(Learning(
        skill="general", type="pitfall", key="test_key",
        insight="new version",
    ))

    # Force reload
    store._cache = None
    results = store.search("test")
    assert len(results) == 1
    assert results[0].insight == "new version"


def test_confidence_decay():
    """Learnings lose confidence over time."""
    from omni_dash.agent.learnings import Learning

    # Fresh learning
    fresh = Learning(
        skill="general", type="pitfall", key="fresh",
        insight="fresh insight", confidence=1.0,
        ts=datetime.now(timezone.utc).isoformat(),
    )
    assert fresh.effective_confidence() >= 0.9

    # Old learning (90 days ago)
    old_ts = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    old = Learning(
        skill="general", type="pitfall", key="old",
        insight="old insight", confidence=1.0,
        ts=old_ts,
    )
    # 90 days = 3 * 30-day decay periods = 0.3 decay
    assert old.effective_confidence() == pytest.approx(0.7, abs=0.05)


def test_min_confidence_filter(store):
    """Low-confidence learnings are filtered out of search results."""
    from omni_dash.agent.learnings import Learning

    # Very old learning (300 days ago, confidence should be ~0)
    old_ts = (datetime.now(timezone.utc) - timedelta(days=300)).isoformat()
    store.add(Learning(
        skill="general", type="pitfall", key="ancient",
        insight="ancient insight about revenue", confidence=1.0,
        ts=old_ts,
    ))

    results = store.search("revenue")
    assert len(results) == 0  # Should be filtered by min confidence


def test_to_context_block(store):
    """Context block formats learnings for system prompt."""
    store.add_from_text("Always use USDCURRENCY_0 for revenue fields")
    store.add_from_text("customer_type is PLG or SLG, not free/paid")

    block = store.to_context_block(query="revenue customer")
    assert "# Relevant Learnings" in block
    assert "USDCURRENCY_0" in block


def test_to_context_block_empty(store):
    """Empty store returns empty string."""
    block = store.to_context_block(query="anything")
    assert block == ""


def test_count(store):
    """Count returns number of deduplicated learnings."""
    store.add_from_text("learning 1")
    store.add_from_text("learning 2")
    assert store.count() == 2


def test_persistence(tmp_path):
    """Learnings persist across store instances."""
    from omni_dash.agent.learnings import LearningsStore

    path = str(tmp_path / "learnings.jsonl")

    store1 = LearningsStore(path=path)
    store1.add_from_text("persisted insight")

    store2 = LearningsStore(path=path)
    results = store2.search("persisted")
    assert len(results) == 1
    assert results[0].insight == "persisted insight"
