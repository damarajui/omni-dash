"""Tests for executor Ralph Loop failure tracking."""

from __future__ import annotations

import json

import pytest


@pytest.fixture(autouse=True)
def _mock_env(monkeypatch):
    monkeypatch.setenv("OMNI_API_KEY", "test-key")
    monkeypatch.setenv("OMNI_BASE_URL", "https://test.omniapp.co")
    monkeypatch.setenv("OMNI_SHARED_MODEL_ID", "test-model")


def _make_executor():
    from omni_dash.agent.executor import ToolExecutor
    from omni_dash.agent.tool_registry import ToolRegistry

    reg = ToolRegistry()
    return ToolExecutor(reg)


def test_consecutive_failure_counter(monkeypatch):
    """Failure counter increments on create_dashboard errors."""
    executor = _make_executor()

    # Mock create_dashboard to return an error
    tool = executor._registry.get("create_dashboard")
    original = tool.callable
    monkeypatch.setattr(tool, "callable", lambda **kw: json.dumps({"error": "test error"}))

    executor.execute("create_dashboard", {"name": "test", "tiles": []})
    assert executor._consecutive_failures == 1

    executor.execute("create_dashboard", {"name": "test", "tiles": []})
    assert executor._consecutive_failures == 2


def test_success_resets_counter(monkeypatch):
    """Successful create_dashboard resets failure counter."""
    executor = _make_executor()
    executor._consecutive_failures = 2
    executor._last_failure_tool = "create_dashboard"

    tool = executor._registry.get("create_dashboard")
    monkeypatch.setattr(
        tool, "callable",
        lambda **kw: json.dumps({"url": "https://test.omniapp.co/dashboards/abc", "id": "abc"}),
    )

    executor.execute("create_dashboard", {"name": "test", "tiles": []})
    assert executor._consecutive_failures == 0


def test_circuit_break_after_3_failures(monkeypatch):
    """After 3 consecutive failures, result includes stop signal."""
    executor = _make_executor()

    tool = executor._registry.get("create_dashboard")
    monkeypatch.setattr(tool, "callable", lambda **kw: json.dumps({"error": "field not found"}))

    # Fail 3 times
    for _ in range(3):
        result, is_error = executor.execute("create_dashboard", {"name": "t", "tiles": []})

    assert is_error
    parsed = json.loads(result)
    assert "_ralph_loop" in parsed
    assert parsed["_ralph_loop"]["action"] == "STOP_RETRYING"
    assert "3" in parsed["_ralph_loop"]["message"]


def test_untracked_tools_no_counter(monkeypatch):
    """Non-dashboard tools don't affect failure counter."""
    executor = _make_executor()

    tool = executor._registry.get("list_topics")
    monkeypatch.setattr(tool, "callable", lambda **kw: json.dumps({"error": "API down"}))

    executor.execute("list_topics", {})
    assert executor._consecutive_failures == 0


def test_reset_failure_tracking():
    """reset_failure_tracking clears state."""
    executor = _make_executor()
    executor._consecutive_failures = 5
    executor._last_failure_tool = "create_dashboard"

    executor.reset_failure_tracking()
    assert executor._consecutive_failures == 0
    assert executor._last_failure_tool == ""
