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

    def get_tool_definitions(self) -> list[dict[str, Any]]:
        """Return Anthropic-format tool definitions."""
        return self._registry.get_definitions()

    def execute(self, tool_name: str, tool_input: dict[str, Any]) -> tuple[str, bool]:
        """Execute a tool and return ``(result_json, is_error)``.

        The MCP tool functions already return JSON strings with built-in
        error handling, so we just call them and catch unexpected blowups.
        """
        import time as _time

        tool = self._registry.get(tool_name)
        if tool is None:
            logger.error("Unknown tool requested: %s", tool_name)
            return json.dumps({"error": f"Unknown tool: {tool_name}"}), True

        t0 = _time.monotonic()
        try:
            result = tool.callable(**tool_input)
            elapsed = _time.monotonic() - t0

            # Check if the result itself reports an error
            is_error = False
            error_detail = ""
            try:
                parsed = json.loads(result)
                if isinstance(parsed, dict) and "error" in parsed:
                    is_error = True
                    error_detail = str(parsed["error"])[:200]
            except (json.JSONDecodeError, TypeError):
                pass

            if is_error:
                logger.warning(
                    "Tool %s returned error in %.1fs: %s",
                    tool_name, elapsed, error_detail,
                )
            else:
                logger.info("Tool %s succeeded in %.1fs (result=%d bytes)", tool_name, elapsed, len(result))

            return result, is_error
        except TypeError as e:
            # Common: wrong kwargs passed to tool function
            elapsed = _time.monotonic() - t0
            logger.exception(
                "Tool %s signature mismatch in %.1fs (input keys: %s): %s",
                tool_name, elapsed, list(tool_input.keys()), e,
            )
            return json.dumps({"error": f"Tool parameter error: {e}"}), True
        except Exception as e:
            elapsed = _time.monotonic() - t0
            logger.exception("Tool %s raised in %.1fs: %s", tool_name, elapsed, e)
            return json.dumps({"error": f"Tool execution failed: {e}"}), True
