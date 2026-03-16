"""Context management for the Dash agent.

Handles two concerns:
1. **Tool result compression**: Shrink old tool results before sending
   to the API, keeping recent results intact for Claude to reference.
2. **Message preparation**: Compress old messages while preserving
   the conversation narrative arc (user intent → data found → built).
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Keep the last N tool results intact; older ones get compressed.
_KEEP_RECENT_TOOL_RESULTS = 3

# Max chars for a single tool result before compression kicks in.
_COMPRESS_THRESHOLD = 2000

# Tools whose results should never be compressed (always important).
_NEVER_COMPRESS = frozenset({"save_learning"})

# Tools whose results are ephemeral discovery data — compress aggressively.
_AGGRESSIVE_COMPRESS = frozenset({
    "list_topics",
    "get_topic_fields",
    "profile_data",
    "validate_dashboard",
    "suggest_chart",
    "list_dashboards",
    "list_folders",
    "get_dashboard_filters",
})


def _compress_single_result(content: str, tool_name: str) -> str:
    """Compress a single tool result string."""
    if len(content) <= _COMPRESS_THRESHOLD:
        return content

    try:
        data = json.loads(content)
    except (json.JSONDecodeError, TypeError):
        # Not JSON — just truncate
        return content[:500] + f"\n...[truncated from {len(content)} chars]"

    # Error results — keep as-is (Claude needs to know what went wrong)
    if isinstance(data, dict) and "error" in data:
        return content

    # query_data results — compress to summary
    if tool_name == "query_data" and isinstance(data, dict):
        rows = data.get("rows", data.get("data", []))
        fields = data.get("fields", [])
        return json.dumps({
            "_compressed": True,
            "tool": tool_name,
            "row_count": len(rows) if isinstance(rows, list) else "?",
            "fields": fields[:10] if isinstance(fields, list) else [],
            "sample": rows[:2] if isinstance(rows, list) else [],
        })

    # Dashboard creation/update results — keep URL and ID only
    if tool_name in ("create_dashboard", "update_dashboard", "add_tiles_to_dashboard"):
        if isinstance(data, dict):
            compressed = {"_compressed": True, "tool": tool_name}
            for key in ("url", "dashboard_url", "id", "dashboard_id", "new_id"):
                if key in data:
                    compressed[key] = data[key]
            return json.dumps(compressed)

    # List results — compress to count + sample
    if isinstance(data, list) and len(data) > 5:
        return json.dumps({
            "_compressed": True,
            "tool": tool_name,
            "count": len(data),
            "sample": data[:3],
        })

    # Aggressive compress for ephemeral tools
    if tool_name in _AGGRESSIVE_COMPRESS:
        if isinstance(data, dict):
            return json.dumps({
                "_compressed": True,
                "tool": tool_name,
                "keys": list(data.keys())[:10],
                "summary": str(data)[:300],
            })

    # Generic fallback — truncate
    if len(content) > _COMPRESS_THRESHOLD:
        return content[:_COMPRESS_THRESHOLD] + f"\n...[truncated from {len(content)} chars]"

    return content


def _is_tool_result_message(msg: dict[str, Any]) -> bool:
    """Check if a message is a tool_result user message."""
    if msg.get("role") != "user":
        return False
    content = msg.get("content")
    if not isinstance(content, list):
        return False
    return any(
        isinstance(block, dict) and block.get("type") == "tool_result"
        for block in content
    )


def _get_tool_name_for_result(
    tool_use_id: str,
    messages: list[dict[str, Any]],
    msg_idx: int,
) -> str:
    """Walk backwards from msg_idx to find the tool_use block matching this ID."""
    for i in range(msg_idx - 1, -1, -1):
        m = messages[i]
        if m.get("role") != "assistant":
            continue
        content = m.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if (
                isinstance(block, dict)
                and block.get("type") == "tool_use"
                and block.get("id") == tool_use_id
            ):
                return block.get("name", "unknown")
    return "unknown"


def compress_old_tool_results(
    messages: list[dict[str, Any]],
    keep_recent: int = _KEEP_RECENT_TOOL_RESULTS,
) -> list[dict[str, Any]]:
    """Compress old tool results in a message list.

    Keeps the last ``keep_recent`` tool result messages intact.
    Older ones get their content compressed to summaries.
    Returns a new list (does not mutate the input).
    """
    # Find indices of all tool_result messages
    tool_result_indices: list[int] = []
    for i, msg in enumerate(messages):
        if _is_tool_result_message(msg):
            tool_result_indices.append(i)

    if len(tool_result_indices) <= keep_recent:
        return messages  # Nothing to compress

    # Indices to compress (all except the last keep_recent)
    compress_indices = set(tool_result_indices[:-keep_recent])

    result = []
    for i, msg in enumerate(messages):
        if i not in compress_indices:
            result.append(msg)
            continue

        # Compress each tool_result block in this message
        content = msg["content"]
        compressed_content = []
        for block in content:
            if not isinstance(block, dict) or block.get("type") != "tool_result":
                compressed_content.append(block)
                continue

            tool_use_id = block.get("tool_use_id", "")
            tool_name = _get_tool_name_for_result(tool_use_id, messages, i)
            original = block.get("content", "")

            if tool_name in _NEVER_COMPRESS:
                compressed_content.append(block)
            elif isinstance(original, str) and len(original) > _COMPRESS_THRESHOLD:
                compressed_content.append({
                    **block,
                    "content": _compress_single_result(original, tool_name),
                })
            else:
                compressed_content.append(block)

        result.append({**msg, "content": compressed_content})

    return result


def prepare_messages_for_api(
    messages: list[dict[str, Any]],
    max_json_chars: int = 600_000,
    keep_recent: int = 10,
) -> list[dict[str, Any]]:
    """Prepare conversation messages for the Anthropic API.

    Two-phase compression:
    1. Compress old tool results (cheapest savings, preserves structure)
    2. Drop middle messages if still over budget (preserves first + last N)

    Returns a new list (does not mutate the input).
    """
    # Phase 1: Compress old tool results
    prepared = compress_old_tool_results(messages)

    serialized = json.dumps(prepared, default=str)
    if len(serialized) <= max_json_chars:
        return prepared

    # Phase 2: Drop middle messages (safety net)
    if len(prepared) <= keep_recent + 1:
        return prepared

    trimmed = [prepared[0]] + prepared[-keep_recent:]
    return trimmed
