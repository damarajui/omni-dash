"""Tests for agent.tool_registry."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _mock_env(monkeypatch):
    """Provide dummy env vars so lazy-init singletons don't blow up on import."""
    monkeypatch.setenv("OMNI_API_KEY", "test-key")
    monkeypatch.setenv("OMNI_BASE_URL", "https://test.omniapp.co")
    monkeypatch.setenv("OMNI_SHARED_MODEL_ID", "test-model")


def test_registry_registers_all_25_tools():
    from omni_dash.agent.tool_registry import ToolRegistry

    reg = ToolRegistry()
    assert reg.tool_count == 25


def test_registry_get_definitions_format():
    from omni_dash.agent.tool_registry import ToolRegistry

    reg = ToolRegistry()
    defs = reg.get_definitions()
    assert isinstance(defs, list)
    assert len(defs) == 25
    for d in defs:
        assert "name" in d
        assert "description" in d
        assert "input_schema" in d
        assert d["input_schema"]["type"] == "object"


def test_registry_get_returns_tool():
    from omni_dash.agent.tool_registry import ToolRegistry

    reg = ToolRegistry()
    tool = reg.get("list_topics")
    assert tool is not None
    assert tool.name == "list_topics"
    assert callable(tool.callable)


def test_registry_get_unknown_returns_none():
    from omni_dash.agent.tool_registry import ToolRegistry

    reg = ToolRegistry()
    assert reg.get("nonexistent_tool") is None


def test_all_tools_have_callable():
    from omni_dash.agent.tool_registry import ToolRegistry

    reg = ToolRegistry()
    for d in reg.get_definitions():
        tool = reg.get(d["name"])
        assert tool is not None, f"Tool {d['name']} not found"
        assert callable(tool.callable), f"Tool {d['name']} callable is not callable"


def test_tool_names_match_definitions():
    from omni_dash.agent.tool_registry import ToolRegistry

    reg = ToolRegistry()
    defs = reg.get_definitions()
    names = {d["name"] for d in defs}
    expected = {
        "list_dashboards", "get_dashboard", "create_dashboard",
        "update_dashboard", "add_tiles_to_dashboard", "update_tile",
        "delete_dashboard", "clone_dashboard", "move_dashboard",
        "import_dashboard", "export_dashboard",
        "list_topics", "get_topic_fields", "query_data", "list_folders",
        "suggest_chart", "validate_dashboard", "profile_data",
        "generate_dashboard",
        "ai_generate_query", "ai_pick_topic", "ai_analyze",
        "get_dashboard_filters", "update_dashboard_filters",
        "save_learning",
    }
    assert names == expected


def test_required_params_present():
    """Ensure required params are listed for key tools."""
    from omni_dash.agent.tool_registry import ToolRegistry

    reg = ToolRegistry()

    create = reg.get("create_dashboard")
    assert create is not None
    assert "name" in create.input_schema.get("required", [])
    assert "tiles" in create.input_schema.get("required", [])

    query = reg.get("query_data")
    assert query is not None
    assert "table" in query.input_schema.get("required", [])
    assert "fields" in query.input_schema.get("required", [])


def test_save_learning_tool_registered():
    """save_learning is the 25th tool and has correct schema."""
    from omni_dash.agent.tool_registry import ToolRegistry

    reg = ToolRegistry()
    tool = reg.get("save_learning")
    assert tool is not None
    assert "learning" in tool.input_schema.get("required", [])
    assert callable(tool.callable)


def test_save_learning_returns_json(monkeypatch):
    """save_learning callable returns valid JSON."""
    from omni_dash.agent.tool_registry import ToolRegistry

    reg = ToolRegistry()
    tool = reg.get("save_learning")

    # Mock add_learning to avoid hitting GitHub
    import scripts.github_utils as gu
    monkeypatch.setattr(gu, "add_learning", lambda _: True)

    import json
    result = tool.callable(learning="test rule")
    parsed = json.loads(result)
    assert parsed["status"] == "ok"


def test_save_learning_returns_error_on_failure(monkeypatch):
    """save_learning returns error JSON when add_learning fails."""
    from omni_dash.agent.tool_registry import ToolRegistry

    reg = ToolRegistry()
    tool = reg.get("save_learning")

    import scripts.github_utils as gu
    monkeypatch.setattr(gu, "add_learning", lambda _: False)

    import json
    result = tool.callable(learning="test rule")
    parsed = json.loads(result)
    assert "error" in parsed
