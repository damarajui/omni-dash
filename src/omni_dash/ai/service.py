"""Main AI service for natural language dashboard generation.

Provides the DashboardAI class that uses Claude with tool use to
explore dbt models and generate validated DashboardDefinition objects.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from omni_dash.ai.prompts import build_system_prompt
from omni_dash.ai.tools import ToolExecutor, get_tool_definitions
from omni_dash.config import get_settings
from omni_dash.dashboard.definition import DashboardDefinition
from omni_dash.dbt.model_registry import ModelRegistry
from omni_dash.exceptions import AIGenerationError, AINotAvailableError, ConfigurationError

logger = logging.getLogger(__name__)


@dataclass
class GenerateResult:
    """Result of AI dashboard generation."""

    definition: DashboardDefinition
    model_name: str | None = None
    tool_calls_made: int = 0
    reasoning: str = ""
    tool_call_log: list[dict[str, Any]] = field(default_factory=list)


class DashboardAI:
    """AI-powered dashboard generator using Claude with tool use.

    Uses an agentic loop where Claude explores the dbt data catalog
    via tools, then generates a validated DashboardDefinition.

    Usage:
        registry = ModelRegistry("/path/to/dbt")
        ai = DashboardAI(registry)
        result = ai.generate("Show me SEO traffic trends")
        print(result.definition.name)
    """

    def __init__(
        self,
        registry: ModelRegistry,
        *,
        model: str = "claude-sonnet-4-5-20250929",
        max_turns: int = 10,
        api_key: str | None = None,
    ):
        self._registry = registry
        self._model = model
        self._max_turns = max_turns
        self._api_key = api_key

    def generate(
        self,
        description: str,
        *,
        on_tool_call: Callable[[str, dict[str, Any], str], None] | None = None,
    ) -> GenerateResult:
        """Generate a dashboard definition from a natural language description.

        Args:
            description: Plain English description of the desired dashboard.
            on_tool_call: Optional callback called for each tool call with
                (tool_name, tool_input, result_str). Useful for verbose output.

        Returns:
            GenerateResult with the validated DashboardDefinition.

        Raises:
            AINotAvailableError: If the anthropic package is not installed.
            ConfigurationError: If ANTHROPIC_API_KEY is not set.
            AIGenerationError: If generation fails after max_turns.
        """
        # Lazy import — anthropic is an optional dependency
        try:
            import anthropic
        except ImportError:
            raise AINotAvailableError(
                "The 'anthropic' package is required for AI dashboard generation. "
                "Install it with: pip install omni-dash[ai]"
            )

        # Resolve API key
        api_key = self._api_key
        if not api_key:
            settings = get_settings()
            api_key = settings.anthropic_api_key
        if not api_key:
            raise ConfigurationError(
                "ANTHROPIC_API_KEY is not set. Set it in .env or as an environment variable."
            )

        client = anthropic.Anthropic(api_key=api_key)
        tools = get_tool_definitions()
        system_prompt = build_system_prompt()
        executor = ToolExecutor(self._registry)

        messages: list[dict[str, Any]] = [
            {"role": "user", "content": description},
        ]

        tool_calls_made = 0
        reasoning_parts: list[str] = []
        tool_call_log: list[dict[str, Any]] = []
        turn = 0

        for turn in range(self._max_turns):
            try:
                response = client.messages.create(
                    model=self._model,
                    max_tokens=4096,
                    system=system_prompt,
                    tools=tools,
                    messages=messages,
                )
            except anthropic.APIError as e:
                raise AIGenerationError(f"Anthropic API error: {e}") from e

            # Process response content blocks
            tool_use_blocks = []
            for block in response.content:
                if block.type == "text":
                    reasoning_parts.append(block.text)
                elif block.type == "tool_use":
                    tool_use_blocks.append(block)

            # If no tool calls, Claude is done
            if not tool_use_blocks:
                break

            # Execute each tool call
            assistant_content = response.content
            tool_results = []

            for tool_block in tool_use_blocks:
                tool_name = tool_block.name
                tool_input = tool_block.input
                tool_calls_made += 1

                logger.info("Tool call #%d: %s", tool_calls_made, tool_name)
                logger.debug("Tool input: %s", tool_input)

                result_str, is_error = executor.execute(tool_name, tool_input)

                tool_call_log.append({
                    "turn": turn + 1,
                    "tool": tool_name,
                    "input": tool_input,
                    "result_preview": result_str[:500],
                    "is_error": is_error,
                })

                if on_tool_call:
                    on_tool_call(tool_name, tool_input, result_str)

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_block.id,
                    "content": result_str,
                    "is_error": is_error,
                })

            # Append assistant message and tool results
            messages.append({"role": "assistant", "content": assistant_content})
            messages.append({"role": "user", "content": tool_results})

            # Check if we got a valid dashboard
            if executor.last_valid_definition is not None:
                # Check if the last tool call was create_dashboard (successful)
                last_tool = tool_use_blocks[-1]
                if last_tool.name == "create_dashboard" and not tool_results[-1].get("is_error"):
                    break

        # Extract result
        definition = executor.last_valid_definition
        if definition is None:
            raise AIGenerationError(
                f"Failed to generate a valid dashboard after {tool_calls_made} tool calls "
                f"and {turn + 1} turns. Claude may need a clearer description or the "
                "requested data may not be available in your dbt models."
            )

        # Infer which model was used
        model_name = None
        tables = definition.all_tables()
        if tables:
            model_name = next(iter(tables))

        return GenerateResult(
            definition=definition,
            model_name=model_name,
            tool_calls_made=tool_calls_made,
            reasoning="\n\n".join(reasoning_parts),
            tool_call_log=tool_call_log,
        )
