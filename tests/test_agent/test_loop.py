"""Tests for agent loop retry and caching."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_stream_with_retry_succeeds_first_try():
    """Verify stream_with_retry returns stream on success."""
    from omni_dash.agent.loop import AgentLoop

    with patch("omni_dash.agent.loop.AgentLoop.__init__", return_value=None):
        loop = AgentLoop.__new__(AgentLoop)
        loop._client = MagicMock()
        loop._model = "test-model"

        mock_stream = MagicMock()
        loop._client.messages.stream.return_value = mock_stream

        result = loop._stream_with_retry(
            model="test-model",
            system=[{"type": "text", "text": "test"}],
            messages=[{"role": "user", "content": "hi"}],
            tool_defs=[],
        )

        assert result == mock_stream
        assert loop._client.messages.stream.call_count == 1


def test_stream_with_retry_retries_on_429():
    """Verify retry logic on rate limit error."""
    import anthropic
    from omni_dash.agent.loop import AgentLoop

    with patch("omni_dash.agent.loop.AgentLoop.__init__", return_value=None):
        loop = AgentLoop.__new__(AgentLoop)
        loop._client = MagicMock()
        loop._model = "test-model"

        mock_stream = MagicMock()
        rate_err = anthropic.RateLimitError(
            message="rate limited",
            response=MagicMock(status_code=429, headers={}),
            body={"error": {"message": "rate limited"}},
        )

        # Fail twice, succeed third time
        loop._client.messages.stream.side_effect = [rate_err, rate_err, mock_stream]

        with patch("omni_dash.agent.loop.time.sleep") as mock_sleep:
            result = loop._stream_with_retry(
                model="test-model",
                system=[{"type": "text", "text": "test"}],
                messages=[{"role": "user", "content": "hi"}],
                tool_defs=[],
            )

        assert result == mock_stream
        assert loop._client.messages.stream.call_count == 3
        assert mock_sleep.call_count == 2


def test_stream_with_retry_exhausts_retries():
    """Verify exception raised after max retries."""
    import anthropic
    import pytest
    from omni_dash.agent.loop import AgentLoop

    with patch("omni_dash.agent.loop.AgentLoop.__init__", return_value=None):
        loop = AgentLoop.__new__(AgentLoop)
        loop._client = MagicMock()
        loop._model = "test-model"

        rate_err = anthropic.RateLimitError(
            message="rate limited",
            response=MagicMock(status_code=429, headers={}),
            body={"error": {"message": "rate limited"}},
        )

        loop._client.messages.stream.side_effect = rate_err

        with patch("omni_dash.agent.loop.time.sleep"):
            with pytest.raises(anthropic.RateLimitError):
                loop._stream_with_retry(
                    model="test-model",
                    system=[{"type": "text", "text": "test"}],
                    messages=[{"role": "user", "content": "hi"}],
                    tool_defs=[],
                )

        # 1 initial + 3 retries = 4 attempts
        assert loop._client.messages.stream.call_count == 4


def test_system_prompt_uses_cache_control():
    """Verify system prompt is structured with cache_control for caching."""
    from omni_dash.agent.loop import AgentLoop

    with patch("omni_dash.agent.loop.AgentLoop.__init__", return_value=None):
        loop = AgentLoop.__new__(AgentLoop)
        loop._client = MagicMock()
        loop._model = "test-model"
        loop._max_turns = 1
        loop._executor = MagicMock()
        loop._executor.get_tool_definitions.return_value = []

        # Mock stream to return a simple text response
        mock_response = MagicMock()
        mock_response.content = [MagicMock(type="text", text="hello")]
        mock_response.usage = MagicMock(input_tokens=100, cache_read_input_tokens=0, cache_creation_input_tokens=50)

        mock_stream_ctx = MagicMock()
        mock_stream_ctx.__enter__ = MagicMock(return_value=mock_stream_ctx)
        mock_stream_ctx.__exit__ = MagicMock(return_value=False)
        mock_stream_ctx.__iter__ = MagicMock(return_value=iter([]))
        mock_stream_ctx.get_final_message.return_value = mock_response

        loop._client.messages.stream.return_value = mock_stream_ctx

        messages = [{"role": "user", "content": "hi"}]
        loop.run(messages, "test system prompt")

        # Check that system was passed as structured blocks with cache_control
        call_kwargs = loop._client.messages.stream.call_args
        system_arg = call_kwargs.kwargs.get("system") or call_kwargs[1].get("system")
        assert isinstance(system_arg, list)
        assert system_arg[0]["cache_control"] == {"type": "ephemeral"}
        assert system_arg[0]["text"] == "test system prompt"

        # Check cache_control was passed at top level
        cache_arg = call_kwargs.kwargs.get("cache_control") or call_kwargs[1].get("cache_control")
        assert cache_arg == {"type": "ephemeral"}


def test_run_uses_model_override():
    """Verify model override parameter is passed to _stream_with_retry."""
    from omni_dash.agent.loop import AgentLoop

    with patch("omni_dash.agent.loop.AgentLoop.__init__", return_value=None):
        loop = AgentLoop.__new__(AgentLoop)
        loop._client = MagicMock()
        loop._model = "default-model"
        loop._max_turns = 1
        loop._executor = MagicMock()
        loop._executor.get_tool_definitions.return_value = []

        mock_response = MagicMock()
        mock_response.content = [MagicMock(type="text", text="done")]
        mock_response.usage = MagicMock(input_tokens=50, cache_read_input_tokens=0, cache_creation_input_tokens=0)

        mock_stream_ctx = MagicMock()
        mock_stream_ctx.__enter__ = MagicMock(return_value=mock_stream_ctx)
        mock_stream_ctx.__exit__ = MagicMock(return_value=False)
        mock_stream_ctx.__iter__ = MagicMock(return_value=iter([]))
        mock_stream_ctx.get_final_message.return_value = mock_response

        loop._client.messages.stream.return_value = mock_stream_ctx

        messages = [{"role": "user", "content": "hi"}]
        loop.run(messages, "system", model="override-model")

        # Check model was passed correctly
        call_kwargs = loop._client.messages.stream.call_args
        assert call_kwargs.kwargs["model"] == "override-model"


def test_run_uses_default_model_when_no_override():
    """Verify default model is used when no override is specified."""
    from omni_dash.agent.loop import AgentLoop

    with patch("omni_dash.agent.loop.AgentLoop.__init__", return_value=None):
        loop = AgentLoop.__new__(AgentLoop)
        loop._client = MagicMock()
        loop._model = "default-model"
        loop._max_turns = 1
        loop._executor = MagicMock()
        loop._executor.get_tool_definitions.return_value = []

        mock_response = MagicMock()
        mock_response.content = [MagicMock(type="text", text="done")]
        mock_response.usage = MagicMock(input_tokens=50, cache_read_input_tokens=0, cache_creation_input_tokens=0)

        mock_stream_ctx = MagicMock()
        mock_stream_ctx.__enter__ = MagicMock(return_value=mock_stream_ctx)
        mock_stream_ctx.__exit__ = MagicMock(return_value=False)
        mock_stream_ctx.__iter__ = MagicMock(return_value=iter([]))
        mock_stream_ctx.get_final_message.return_value = mock_response

        loop._client.messages.stream.return_value = mock_stream_ctx

        messages = [{"role": "user", "content": "hi"}]
        loop.run(messages, "system")

        call_kwargs = loop._client.messages.stream.call_args
        assert call_kwargs.kwargs["model"] == "default-model"
