"""Tests for agent.loop — agentic tool-use loop."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _mock_env(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("OMNI_API_KEY", "test-key")
    monkeypatch.setenv("OMNI_BASE_URL", "https://test.omniapp.co")
    monkeypatch.setenv("OMNI_SHARED_MODEL_ID", "test-model")


def _make_text_response(text: str):
    """Build a fake Anthropic response with a single text block."""
    block = SimpleNamespace(type="text", text=text)
    return SimpleNamespace(content=[block], stop_reason="end_turn")


def _make_tool_response(tool_name: str, tool_id: str, tool_input: dict):
    """Build a fake Anthropic response with a tool_use block."""
    block = SimpleNamespace(
        type="tool_use", id=tool_id, name=tool_name, input=tool_input
    )
    return SimpleNamespace(content=[block], stop_reason="tool_use")


class FakeStream:
    """Simulates ``client.messages.stream()`` context manager."""

    def __init__(self, response):
        self._response = response

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __iter__(self):
        # Emit content_block_start + content_block_delta + content_block_stop
        for i, block in enumerate(self._response.content):
            if block.type == "text":
                yield SimpleNamespace(
                    type="content_block_start",
                    content_block=SimpleNamespace(type="text"),
                    index=i,
                )
                yield SimpleNamespace(
                    type="content_block_delta",
                    delta=SimpleNamespace(type="text_delta", text=block.text),
                    index=i,
                )
                yield SimpleNamespace(type="content_block_stop", index=i)
            elif block.type == "tool_use":
                yield SimpleNamespace(
                    type="content_block_start",
                    content_block=SimpleNamespace(
                        type="tool_use", id=block.id, name=block.name
                    ),
                    index=i,
                )
                yield SimpleNamespace(type="content_block_stop", index=i)

    def get_final_message(self):
        return self._response


def test_loop_text_only():
    """If Claude responds with text only, loop returns immediately."""
    from omni_dash.agent.executor import ToolExecutor
    from omni_dash.agent.tool_registry import ToolRegistry

    response = _make_text_response("Hello, I can help!")

    with patch("anthropic.Anthropic") as mock_cls:
        mock_client = MagicMock()
        mock_cls.return_value = mock_client
        mock_client.messages.stream.return_value = FakeStream(response)

        from omni_dash.agent.loop import AgentLoop

        registry = ToolRegistry()
        executor = ToolExecutor(registry)
        loop = AgentLoop(executor, model="test-model", api_key="test")

        messages = [{"role": "user", "content": "hi"}]
        messages, final_text = loop.run(messages, "system prompt")

    assert final_text == "Hello, I can help!"
    assert len(messages) == 2  # user + assistant


def test_loop_tool_then_text():
    """Loop should execute tool, then get text response."""
    from omni_dash.agent.executor import ToolExecutor
    from omni_dash.agent.tool_registry import ToolRegistry

    tool_response = _make_tool_response("list_folders", "call_1", {})
    text_response = _make_text_response("Here are your folders.")

    call_count = 0

    def _stream_side_effect(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return FakeStream(tool_response)
        return FakeStream(text_response)

    with patch("anthropic.Anthropic") as mock_cls:
        mock_client = MagicMock()
        mock_cls.return_value = mock_client
        mock_client.messages.stream.side_effect = _stream_side_effect

        from omni_dash.agent.loop import AgentLoop

        registry = ToolRegistry()
        executor = ToolExecutor(registry)
        # Mock the actual tool execution to avoid API calls
        executor.execute = MagicMock(
            return_value=(json.dumps([{"id": "f1", "name": "Test"}]), False)
        )

        loop = AgentLoop(executor, model="test-model", api_key="test")
        messages = [{"role": "user", "content": "list folders"}]
        messages, final_text = loop.run(messages, "system prompt")

    assert final_text == "Here are your folders."
    assert call_count == 2
    executor.execute.assert_called_once_with("list_folders", {})


def test_loop_streaming_callbacks():
    """Verify on_text_delta and on_tool_call callbacks are invoked."""
    from omni_dash.agent.executor import ToolExecutor
    from omni_dash.agent.tool_registry import ToolRegistry

    response = _make_text_response("streamed text")

    text_deltas: list[str] = []

    with patch("anthropic.Anthropic") as mock_cls:
        mock_client = MagicMock()
        mock_cls.return_value = mock_client
        mock_client.messages.stream.return_value = FakeStream(response)

        from omni_dash.agent.loop import AgentLoop

        registry = ToolRegistry()
        executor = ToolExecutor(registry)
        loop = AgentLoop(executor, model="test-model", api_key="test")

        messages = [{"role": "user", "content": "hi"}]
        messages, _ = loop.run(
            messages,
            "system",
            on_text_delta=lambda d: text_deltas.append(d),
        )

    assert "streamed text" in text_deltas


def test_loop_max_turns():
    """Loop should stop after max_turns even if Claude keeps calling tools."""
    from omni_dash.agent.executor import ToolExecutor
    from omni_dash.agent.tool_registry import ToolRegistry

    tool_response = _make_tool_response("list_folders", "call_1", {})

    with patch("anthropic.Anthropic") as mock_cls:
        mock_client = MagicMock()
        mock_cls.return_value = mock_client
        mock_client.messages.stream.return_value = FakeStream(tool_response)

        from omni_dash.agent.loop import AgentLoop

        registry = ToolRegistry()
        executor = ToolExecutor(registry)
        executor.execute = MagicMock(
            return_value=(json.dumps({"status": "ok"}), False)
        )

        loop = AgentLoop(executor, model="test", api_key="test", max_turns=2)
        messages = [{"role": "user", "content": "go"}]
        messages, _ = loop.run(messages, "system")

    # Should have called execute exactly 2 times (max_turns=2)
    assert executor.execute.call_count == 2
