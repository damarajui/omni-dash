"""Thin dispatch layer for executing registered tools.

Follows the same ``(result_json, is_error)`` pattern as
``ai/tools.py:ToolExecutor``.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from omni_dash.agent.tool_registry import ToolRegistry

logger = logging.getLogger(__name__)


class ToolExecutor:
    """Execute tool calls against the :class:`ToolRegistry`."""

    def __init__(self, registry: ToolRegistry) -> None:
        self._registry = registry

    def execute(self, tool_name: str, tool_input: dict[str, Any]) -> tuple[str, bool]:
        """Execute a tool and return ``(result_json, is_error)``.

        The MCP tool functions already return JSON strings with built-in
        error handling, so we just call them and catch unexpected blowups.
        """
        tool = self._registry.get(tool_name)
        if tool is None:
            return json.dumps({"error": f"Unknown tool: {tool_name}"}), True

        try:
            result = tool.callable(**tool_input)
            # Check if the result itself reports an error
            is_error = False
            try:
                parsed = json.loads(result)
                if isinstance(parsed, dict) and "error" in parsed and len(parsed) <= 3:
                    is_error = True
            except (json.JSONDecodeError, TypeError):
                pass
            return result, is_error
        except Exception as e:
            logger.exception("Tool %s raised: %s", tool_name, e)
            return json.dumps({"error": f"Tool execution failed: {e}"}), True
