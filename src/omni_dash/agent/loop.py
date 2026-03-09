"""Agentic loop for the Dash Slack bot.

Follows the proven pattern from ``ai/service.py`` but generalised for
any set of registered tools and with streaming support.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any

from omni_dash.agent.executor import ToolExecutor

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = os.environ.get("DASH_CLAUDE_MODEL", "claude-sonnet-4-5-20250929")


class AgentLoop:
    """Run an agentic tool-use loop against the Anthropic API.

    Args:
        executor: Dispatches tool calls.
        model: Claude model to use.
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

    def run(
        self,
        messages: list[dict[str, Any]],
        system: str,
        *,
        on_text_delta: Callable[[str], None] | None = None,
        on_tool_call: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> tuple[list[dict[str, Any]], str]:
        """Run the agentic loop until Claude stops calling tools.

        Args:
            messages: Anthropic-format message list (mutated in place).
            system: System prompt.
            on_text_delta: Called with each text chunk during streaming.
            on_tool_call: Called with ``(tool_name, tool_input)`` before execution.

        Returns:
            ``(updated_messages, final_text)`` — the final assistant text
            response (or empty string if the last turn was a tool call).
        """
        final_text = ""

        for _turn in range(self._max_turns):
            # Stream the response
            text_parts: list[str] = []
            tool_use_blocks: list[dict[str, Any]] = []
            full_content: list[dict[str, Any]] = []

            with self._client.messages.stream(
                model=self._model,
                max_tokens=4096,
                system=system,
                messages=messages,
                tools=self._executor.get_tool_definitions(),
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
                break

            # Execute tool calls
            tool_results: list[dict[str, Any]] = []
            for tb in tool_use_blocks:
                tool_name = tb["name"]
                tool_input = tb["input"]

                if on_tool_call:
                    on_tool_call(tool_name, tool_input)

                logger.info("Tool call: %s", tool_name)
                result_str, is_error = self._executor.execute(tool_name, tool_input)

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
