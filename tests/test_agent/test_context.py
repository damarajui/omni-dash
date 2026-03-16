"""Tests for context management and tool result compression."""

from __future__ import annotations

import json


def _make_tool_use_msg(tool_id: str, tool_name: str, tool_input: dict | None = None):
    return {
        "role": "assistant",
        "content": [
            {
                "type": "tool_use",
                "id": tool_id,
                "name": tool_name,
                "input": tool_input or {},
            }
        ],
    }


def _make_tool_result_msg(tool_id: str, content: str, is_error: bool = False):
    return {
        "role": "user",
        "content": [
            {
                "type": "tool_result",
                "tool_use_id": tool_id,
                "content": content,
                "is_error": is_error,
            }
        ],
    }


def _big_query_result(n_rows: int = 50) -> str:
    """Create a query_data result larger than the compression threshold."""
    return json.dumps({
        "rows": [
            {"week_start": f"2025-W{i:02d}", "total_web_visits": i * 1000, "signups": i * 10, "extra_field": "x" * 50}
            for i in range(n_rows)
        ],
        "fields": ["week_start", "total_web_visits", "signups", "extra_field"],
    })


def test_compress_old_tool_results_keeps_recent():
    from omni_dash.agent.context import compress_old_tool_results

    messages = [
        {"role": "user", "content": "hello"},
    ]
    # Add 5 tool call/result pairs with big results
    for i in range(5):
        tid = f"tool_{i}"
        messages.append(_make_tool_use_msg(tid, "query_data"))
        messages.append(_make_tool_result_msg(tid, _big_query_result()))

    result = compress_old_tool_results(messages, keep_recent=3)

    # Should have same number of messages
    assert len(result) == len(messages)

    # Last 3 tool results should be intact (not compressed)
    for idx in [6, 8, 10]:
        content = result[idx]["content"][0]["content"]
        data = json.loads(content)
        assert "_compressed" not in data


def test_compress_query_data_result():
    from omni_dash.agent.context import _compress_single_result

    big_data = _big_query_result(50)
    assert len(big_data) > 2000  # Verify it's above threshold

    compressed = _compress_single_result(big_data, "query_data")
    parsed = json.loads(compressed)
    assert parsed["_compressed"] is True
    assert parsed["row_count"] == 50
    assert len(parsed["sample"]) == 2


def test_compress_dashboard_creation_result():
    from omni_dash.agent.context import _compress_single_result

    result = json.dumps({
        "url": "https://lindy.omniapp.co/dashboards/abc123",
        "dashboard_id": "abc123",
        "tiles_created": 5,
        "extra_metadata": "lots of stuff here " * 200,  # Make it large
    })
    assert len(result) > 2000

    compressed = _compress_single_result(result, "create_dashboard")
    parsed = json.loads(compressed)
    assert parsed["_compressed"] is True
    assert parsed["url"] == "https://lindy.omniapp.co/dashboards/abc123"
    assert parsed["dashboard_id"] == "abc123"
    assert "extra_metadata" not in parsed


def test_compress_list_result():
    from omni_dash.agent.context import _compress_single_result

    # Make list large enough to trigger compression
    result = json.dumps([{"name": f"table_{i}", "description": "x" * 100} for i in range(30)])
    assert len(result) > 2000

    compressed = _compress_single_result(result, "list_topics")
    parsed = json.loads(compressed)
    assert parsed["_compressed"] is True
    assert parsed["count"] == 30
    assert len(parsed["sample"]) == 3


def test_small_results_not_compressed():
    from omni_dash.agent.context import _compress_single_result

    small = json.dumps({"status": "ok"})
    assert _compress_single_result(small, "query_data") == small


def test_error_results_not_compressed():
    from omni_dash.agent.context import _compress_single_result

    error = json.dumps({"error": "something went wrong", "details": "x" * 3000})
    assert len(error) > 2000
    result = _compress_single_result(error, "query_data")
    assert result == error  # Errors are never compressed


def test_prepare_messages_for_api_under_budget():
    from omni_dash.agent.context import prepare_messages_for_api

    messages = [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": [{"type": "text", "text": "hi there"}]},
    ]

    result = prepare_messages_for_api(messages)
    assert result == messages


def test_prepare_messages_for_api_compresses_then_trims():
    from omni_dash.agent.context import prepare_messages_for_api

    # Create messages that exceed budget
    messages = [{"role": "user", "content": "first"}]
    for i in range(20):
        tid = f"tool_{i}"
        messages.append(_make_tool_use_msg(tid, "query_data"))
        messages.append(_make_tool_result_msg(tid, _big_query_result(100)))

    result = prepare_messages_for_api(messages, max_json_chars=5000, keep_recent=4)

    # Should be trimmed to first + last 4
    assert len(result) <= 5
    assert result[0]["content"] == "first"


def test_non_json_tool_results_truncated():
    from omni_dash.agent.context import _compress_single_result

    long_text = "x" * 5000
    result = _compress_single_result(long_text, "unknown_tool")
    assert len(result) < len(long_text)
    assert "truncated" in result


def test_compress_preserves_tool_result_structure():
    from omni_dash.agent.context import compress_old_tool_results

    messages = [
        {"role": "user", "content": "hello"},
        _make_tool_use_msg("t1", "query_data"),
        _make_tool_result_msg("t1", _big_query_result()),
        _make_tool_use_msg("t2", "list_topics"),
        _make_tool_result_msg("t2", json.dumps([{"name": f"t{i}", "desc": "x" * 100} for i in range(30)])),
        _make_tool_use_msg("t3", "query_data"),
        _make_tool_result_msg("t3", _big_query_result()),
    ]

    result = compress_old_tool_results(messages, keep_recent=1)

    # All messages should still be present
    assert len(result) == len(messages)

    # All tool result blocks should still have required keys
    for msg in result:
        if msg.get("role") == "user":
            content = msg.get("content")
            if isinstance(content, list):
                for block in content:
                    if block.get("type") == "tool_result":
                        assert "tool_use_id" in block
                        assert "content" in block
