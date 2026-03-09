"""Tests for agent.executor."""

from __future__ import annotations

import json

import pytest


@pytest.fixture(autouse=True)
def _mock_env(monkeypatch):
    monkeypatch.setenv("OMNI_API_KEY", "test-key")
    monkeypatch.setenv("OMNI_BASE_URL", "https://test.omniapp.co")
    monkeypatch.setenv("OMNI_SHARED_MODEL_ID", "test-model")


@pytest.fixture
def executor():
    from omni_dash.agent.executor import ToolExecutor
    from omni_dash.agent.tool_registry import ToolRegistry

    return ToolExecutor(ToolRegistry())


def test_unknown_tool_returns_error(executor):
    result, is_error = executor.execute("nonexistent_tool", {})
    assert is_error is True
    parsed = json.loads(result)
    assert "Unknown tool" in parsed["error"]


def test_execute_dispatches_to_tool(monkeypatch):
    """Verify executor dispatches and returns tool's result."""
    from omni_dash.agent.executor import ToolExecutor
    from omni_dash.agent.tool_registry import RegisteredTool

    class FakeRegistry:
        def get(self, name):
            if name == "test_tool":
                return RegisteredTool(
                    name="test_tool",
                    description="test",
                    input_schema={"type": "object", "properties": {}, "required": []},
                    callable=lambda: json.dumps({"status": "ok"}),
                )
            return None
        def get_definitions(self):
            return []
        @property
        def tool_count(self):
            return 1

    executor = ToolExecutor(FakeRegistry())
    result, is_error = executor.execute("test_tool", {})
    assert is_error is False
    assert json.loads(result) == {"status": "ok"}


def test_execute_handles_exception(monkeypatch):
    """Verify executor catches exceptions and returns error JSON."""
    from omni_dash.agent.executor import ToolExecutor
    from omni_dash.agent.tool_registry import RegisteredTool

    def _boom():
        raise RuntimeError("kaboom")

    class FakeRegistry:
        def get(self, name):
            if name == "boom":
                return RegisteredTool(
                    name="boom",
                    description="test",
                    input_schema={},
                    callable=_boom,
                )
            return None
        def get_definitions(self):
            return []

    executor = ToolExecutor(FakeRegistry())
    result, is_error = executor.execute("boom", {})
    assert is_error is True
    parsed = json.loads(result)
    assert "kaboom" in parsed["error"]


def test_execute_detects_error_in_result(monkeypatch):
    """When the tool itself returns an error JSON, is_error should be True."""
    from omni_dash.agent.executor import ToolExecutor
    from omni_dash.agent.tool_registry import RegisteredTool

    class FakeRegistry:
        def get(self, name):
            return RegisteredTool(
                name="err_tool",
                description="test",
                input_schema={},
                callable=lambda: json.dumps({"error": "something went wrong"}),
            )
        def get_definitions(self):
            return []

    executor = ToolExecutor(FakeRegistry())
    result, is_error = executor.execute("err_tool", {})
    assert is_error is True
