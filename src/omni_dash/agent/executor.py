"""Thin dispatch layer for executing registered tools.

Follows the same ``(result_json, is_error)`` pattern as
``ai/tools.py:ToolExecutor``.

Includes failure tracking for the Ralph Loop: consecutive
``create_dashboard`` failures trigger circuit-breaking and auto-learning.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from omni_dash.agent.tool_registry import ToolRegistry

logger = logging.getLogger(__name__)

# Tools that participate in Ralph Loop failure tracking
_TRACKED_TOOLS = {"create_dashboard", "update_dashboard", "add_tiles_to_dashboard"}
_MAX_CONSECUTIVE_FAILURES = 3


class ToolExecutor:
    """Execute tool calls against the :class:`ToolRegistry`.

    Tracks consecutive failures on dashboard-building tools. After
    ``_MAX_CONSECUTIVE_FAILURES`` consecutive errors, injects a warning
    into the result telling the agent to stop retrying.
    """

    def __init__(self, registry: ToolRegistry) -> None:
        self._registry = registry
        self._consecutive_failures: int = 0
        self._last_failure_tool: str = ""

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

            # Ralph Loop failure tracking
            if tool_name in _TRACKED_TOOLS:
                if is_error:
                    self._consecutive_failures += 1
                    self._last_failure_tool = tool_name
                    logger.warning(
                        "Ralph Loop: %s failure %d/%d: %s",
                        tool_name, self._consecutive_failures,
                        _MAX_CONSECUTIVE_FAILURES, error_detail,
                    )

                    if self._consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                        # Circuit break: inject stop signal into result
                        try:
                            enriched = json.loads(result)
                            enriched["_ralph_loop"] = {
                                "consecutive_failures": self._consecutive_failures,
                                "action": "STOP_RETRYING",
                                "message": (
                                    f"You have failed {self._consecutive_failures} consecutive times "
                                    f"on {tool_name}. STOP retrying. Report what went wrong, "
                                    "what you tried, and call save_learning with the failure pattern."
                                ),
                            }
                            result = json.dumps(enriched)
                        except (json.JSONDecodeError, TypeError):
                            pass
                else:
                    # Success after failures — log a learning
                    if self._consecutive_failures > 0:
                        logger.info(
                            "Ralph Loop: %s succeeded after %d failures",
                            tool_name, self._consecutive_failures,
                        )
                        self._auto_learn_recovery(tool_name)
                    self._consecutive_failures = 0
                    self._last_failure_tool = ""

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

    def _auto_learn_recovery(self, tool_name: str) -> None:
        """Automatically save a learning when a tool succeeds after failures."""
        try:
            from omni_dash.agent.learnings import get_learnings_store

            store = get_learnings_store()
            store.add_from_text(
                f"{tool_name} failed {self._consecutive_failures} times then succeeded. "
                f"Previous failure tool: {self._last_failure_tool}. "
                "The fix involved retrying with corrected parameters.",
                skill="dashboard_building",
                learning_type="pattern",
                source="agent_discovery",
            )
        except Exception as e:
            logger.debug("Auto-learn failed: %s", e)

    def reset_failure_tracking(self) -> None:
        """Reset failure counters (e.g., at start of new conversation)."""
        self._consecutive_failures = 0
        self._last_failure_tool = ""
