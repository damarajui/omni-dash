"""Tests for memory.store — unified Convex + JSONL memory."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture()
def store(tmp_path, monkeypatch):
    """Create a MemoryStore with JSONL fallback and no Convex."""
    monkeypatch.delenv("CONVEX_URL", raising=False)
    monkeypatch.delenv("CONVEX_DEPLOY_KEY", raising=False)
    # Reset singleton
    import omni_dash.memory.store as mod
    mod._store = None
    import omni_dash.memory.convex_client as cmod
    cmod._client = None

    from omni_dash.memory.store import MemoryStore
    return MemoryStore(jsonl_path=str(tmp_path / "learnings.jsonl"))


def test_save_and_search_jsonl_fallback(store):
    """When Convex is offline, learnings work via JSONL."""
    assert not store.convex_available

    store.save_learning_from_text("customer_type is PLG or SLG, not free/paid")
    results = store.search_learnings("customer type")
    assert len(results) >= 1
    assert "PLG" in results[0].get("insight", "")


def test_context_block(store):
    """Context block formats learnings for prompt injection."""
    store.save_learning_from_text("Always use USDCURRENCY_0 for revenue")
    block = store.learnings_context_block(query="revenue")
    assert "USDCURRENCY_0" in block
    assert "# Relevant Learnings" in block


def test_context_block_empty(store):
    """Empty store returns empty string."""
    assert store.learnings_context_block(query="anything") == ""


def test_persistence_across_instances(tmp_path, monkeypatch):
    """Learnings persist across store instances via JSONL."""
    monkeypatch.delenv("CONVEX_URL", raising=False)
    monkeypatch.delenv("CONVEX_DEPLOY_KEY", raising=False)
    import omni_dash.memory.convex_client as cmod
    cmod._client = None

    from omni_dash.memory.store import MemoryStore

    path = str(tmp_path / "learnings.jsonl")
    s1 = MemoryStore(jsonl_path=path)
    s1.save_learning_from_text("test insight persists")

    s2 = MemoryStore(jsonl_path=path)
    results = s2.search_learnings("test insight")
    assert len(results) == 1


def test_dashboard_log_no_convex(store):
    """Dashboard log is a no-op without Convex (no crash)."""
    from omni_dash.memory.store import DashboardLog

    log = DashboardLog(
        prompt="build arr dashboard",
        status="success",
        model="claude-sonnet-4-5-20250929",
        tool_calls=5,
        duration_ms=12000,
    )
    store.log_dashboard_creation(log)  # Should not raise


def test_user_preferences_no_convex(store):
    """User preferences return None without Convex."""
    result = store.get_user_preferences("U12345")
    assert result is None


def test_feedback_no_convex(store):
    """Feedback save is a no-op without Convex (no crash)."""
    store.save_feedback("U12345", "this chart should be a line not bar")


def test_convex_client_configured(monkeypatch):
    """ConvexHTTPClient reports configured when env vars set."""
    monkeypatch.setenv("CONVEX_URL", "https://test-123.convex.cloud")
    monkeypatch.setenv("CONVEX_DEPLOY_KEY", "test-deploy-key")
    import omni_dash.memory.convex_client as cmod
    cmod._client = None

    from omni_dash.memory.convex_client import ConvexHTTPClient
    client = ConvexHTTPClient()
    assert client.configured


def test_convex_client_not_configured(monkeypatch):
    """ConvexHTTPClient reports not configured without env vars."""
    monkeypatch.delenv("CONVEX_URL", raising=False)
    monkeypatch.delenv("CONVEX_DEPLOY_KEY", raising=False)
    import omni_dash.memory.convex_client as cmod
    cmod._client = None

    from omni_dash.memory.convex_client import ConvexHTTPClient
    client = ConvexHTTPClient()
    assert not client.configured
    # Queries return None when not configured
    assert client.query("learnings:search", {"query": "test"}) is None
