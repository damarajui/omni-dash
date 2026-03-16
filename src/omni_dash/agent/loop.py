"""Agentic loop for the Dash Slack bot.

Follows the proven pattern from ``ai/service.py`` but generalised for
any set of registered tools and with streaming support.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from typing import Any

from omni_dash.agent.executor import ToolExecutor

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = os.environ.get("DASH_CLAUDE_MODEL", "claude-sonnet-4-5-20250929")

# Retry config for rate limit errors (429)
_MAX_RETRIES = 3
_BASE_DELAY = 5.0  # seconds


class AgentLoop:
    """Run an agentic tool-use loop against the Anthropic API.

    Args:
        executor: Dispatches tool calls.
        model: Claude model to use (default, can be overridden per-run).
        max_turns: Safety limit on agentic turns.
        api_key: Anthropic API key (falls back to ``ANTHROPIC_API_KEY``).
    """

    def __init__(
        self,
        executor: ToolExecutor,
        *,
        model: str = _DEFAULT_MODEL,
        max_turns: int = 15,
        api_key: str | None = None,
    ) -> None:
        import anthropic

        resolved_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self._client = anthropic.Anthropic(api_key=resolved_key)
        self._executor = executor
        self._model = model
        self._max_turns = max_turns

    def _stream_with_retry(
        self,
        *,
        model: str,
        system: list[dict[str, Any]],
        messages: list[dict[str, Any]],
        tool_defs: list[dict[str, Any]],
    ):
        """Call messages.stream() with retry + exponential backoff on 429."""
        import anthropic

        last_err = None
        for attempt in range(_MAX_RETRIES + 1):
            try:
                return self._client.messages.stream(
                    model=model,
                    max_tokens=4096,
                    system=system,
                    messages=messages,
                    tools=tool_defs,
                    cache_control={"type": "ephemeral"},
                )
            except anthropic.RateLimitError as e:
                last_err = e
                if attempt < _MAX_RETRIES:
                    delay = _BASE_DELAY * (2 ** attempt)
                    logger.warning(
                        "Rate limited (429), retry %d/%d in %.0fs: %s",
                        attempt + 1, _MAX_RETRIES, delay, e,
                    )
                    time.sleep(delay)
                else:
                    logger.error("Rate limit retries exhausted after %d attempts", _MAX_RETRIES + 1)
                    raise
        raise last_err  # unreachable but satisfies type checker

    def run(
        self,
        messages: list[dict[str, Any]],
        system: str,
        *,
        model: str | None = None,
        on_text_delta: Callable[[str], None] | None = None,
        on_tool_call: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> tuple[list[dict[str, Any]], str]:
        """Run the agentic loop until Claude stops calling tools.

        Args:
            messages: Anthropic-format message list (mutated in place).
            system: System prompt.
            model: Override model for this run (e.g. from router).
                Falls back to the instance default if not provided.
            on_text_delta: Called with each text chunk during streaming.
            on_tool_call: Called with ``(tool_name, tool_input)`` before execution.

        Returns:
            ``(updated_messages, final_text)`` -- the final assistant text
            response (or empty string if the last turn was a tool call).
        """
        final_text = ""
        effective_model = model or self._model

        tool_defs = self._executor.get_tool_definitions()

        # Structure system prompt for caching -- cache_control on the
        # last block tells Anthropic to cache everything up to that point.
        system_blocks = [
            {
                "type": "text",
                "text": system,
                "cache_control": {"type": "ephemeral"},
            }
        ]

        logger.info(
            "Agent loop starting: model=%s, tools=%d, messages=%d",
            effective_model, len(tool_defs), len(messages),
        )

        for _turn in range(self._max_turns):
            # Stream the response
            text_parts: list[str] = []
            tool_use_blocks: list[dict[str, Any]] = []
            full_content: list[dict[str, Any]] = []

            logger.info("Turn %d: calling messages.stream()", _turn + 1)
            with self._stream_with_retry(
                model=effective_model,
                system=system_blocks,
                messages=messages,
                tool_defs=tool_defs,
            ) as stream:
                current_tool: dict[str, Any] | None = None

                for event in stream:
                    if event.type == "content_block_start":
                        block = event.content_block
                        if block.type == "text":
                            pass  # text deltas come via content_block_delta
                        elif block.type == "tool_use":
                            current_tool = {
                                "type": "tool_use",
                                "id": block.id,
                                "name": block.name,
                                "input": {},
                            }
                    elif event.type == "content_block_delta":
                        delta = event.delta
                        if delta.type == "text_delta":
                            text_parts.append(delta.text)
                            if on_text_delta:
                                on_text_delta(delta.text)
                        elif delta.type == "input_json_delta":
                            pass  # input assembled by SDK
                    elif event.type == "content_block_stop":
                        if current_tool is not None:
                            current_tool = None

                # Get the final assembled message
                response = stream.get_final_message()

            # Log cache usage if available
            usage = response.usage
            cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
            cache_create = getattr(usage, "cache_creation_input_tokens", 0) or 0
            if cache_read or cache_create:
                logger.info(
                    "Turn %d cache: read=%d, created=%d, input=%d",
                    _turn + 1, cache_read, cache_create, usage.input_tokens,
                )

            # Build full_content from the response
            for block in response.content:
                if block.type == "text":
                    full_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    tool_block = {
                        "type": "tool_use",
                        "id": block.id,
                        "name": block.name,
                        "input": block.input,
                    }
                    full_content.append(tool_block)
                    tool_use_blocks.append(tool_block)

            combined_text = "".join(text_parts)

            # If no tool calls, we're done
            if not tool_use_blocks:
                final_text = combined_text
                messages.append({"role": "assistant", "content": full_content})
                logger.info(
                    "Turn %d: text-only response (len=%d), loop done.",
                    _turn + 1, len(combined_text),
                )
                break

            # Execute tool calls
            tool_results: list[dict[str, Any]] = []
            for tb in tool_use_blocks:
                tool_name = tb["name"]
                tool_input = tb["input"]

                if on_tool_call:
                    on_tool_call(tool_name, tool_input)

                logger.info("Tool call: %s(%s)", tool_name, list(tool_input.keys()))
                result_str, is_error = self._executor.execute(tool_name, tool_input)
                logger.info(
                    "Tool result: %s -> %s (is_error=%s, len=%d)",
                    tool_name,
                    result_str[:120].replace(chr(10), " "),
                    is_error,
                    len(result_str),
                )

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tb["id"],
                    "content": result_str,
                    "is_error": is_error,
                })

            # Append to conversation
            messages.append({"role": "assistant", "content": full_content})
            messages.append({"role": "user", "content": tool_results})

            # Update final_text in case this was also a partial text response
            if combined_text:
                final_text = combined_text
        else:
            # Loop exhausted without Claude stopping naturally
            logger.warning(
                "Agent loop hit max_turns=%d. Last tools: %s",
                self._max_turns,
                [tb["name"] for tb in tool_use_blocks] if tool_use_blocks else "none",
            )
            if not final_text:
                final_text = (
                    "I ran out of processing steps before completing your request. "
                    "Please try breaking your question into smaller parts."
                )

        return messages, final_text
