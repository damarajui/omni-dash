"""Tests for omni_dash.ai.tools — tool definitions and execution."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from omni_dash.ai.tools import ToolExecutor, get_tool_definitions
from omni_dash.dashboard.definition import ChartType
from omni_dash.dbt.manifest_reader import DbtColumnMetadata, DbtModelMetadata


@pytest.fixture
def mock_registry():
    registry = MagicMock()
    registry.list_models.return_value = [
        DbtModelMetadata(
            name="mart_seo_weekly_funnel",
            description="SEO funnel metrics",
            columns=[
                DbtColumnMetadata(name="week_start", description="Week start date"),
                DbtColumnMetadata(name="organic_visits", description="Total organic visits"),
            ],
            has_omni_grant=True,
            materialization="table",
        ),
        DbtModelMetadata(
            name="mart_paid_performance",
            description="Paid channel performance",
            columns=[
                DbtColumnMetadata(name="month_start", description="Month"),
                DbtColumnMetadata(name="spend", description="Ad spend"),
            ],
            has_omni_grant=True,
        ),
    ]
    registry.get_model.return_value = DbtModelMetadata(
        name="mart_seo_weekly_funnel",
        description="SEO funnel metrics by week",
        columns=[
            DbtColumnMetadata(name="week_start", description="Week start date", data_type="DATE"),
            DbtColumnMetadata(name="organic_visits", description="Total organic visits", data_type="NUMBER"),
            DbtColumnMetadata(name="conversions", description="Conversions", data_type="NUMBER"),
        ],
        has_omni_grant=True,
    )
    registry.search_models.return_value = [
        DbtModelMetadata(
            name="mart_seo_weekly_funnel",
            description="SEO funnel",
            columns=[],
        ),
    ]
    return registry


@pytest.fixture
def executor(mock_registry):
    return ToolExecutor(mock_registry)


class TestToolDefinitions:
    def test_returns_four_tools(self):
        tools = get_tool_definitions()
        assert len(tools) == 4

    def test_tool_names(self):
        tools = get_tool_definitions()
        names = {t["name"] for t in tools}
        assert names == {"list_models", "get_model_detail", "search_models", "create_dashboard"}

    def test_all_tools_have_required_fields(self):
        tools = get_tool_definitions()
        for tool in tools:
            assert "name" in tool
            assert "description" in tool
            assert "input_schema" in tool
            assert tool["input_schema"]["type"] == "object"

    def test_create_dashboard_schema_has_tiles(self):
        tools = get_tool_definitions()
        create_tool = next(t for t in tools if t["name"] == "create_dashboard")
        schema = create_tool["input_schema"]
        assert "tiles" in schema["properties"]
        assert "name" in schema["required"]

    def test_create_dashboard_schema_has_folder_id(self):
        tools = get_tool_definitions()
        create_tool = next(t for t in tools if t["name"] == "create_dashboard")
        schema = create_tool["input_schema"]
        assert "folder_id" in schema["properties"]
        assert schema["properties"]["folder_id"]["type"] == "string"

    def test_chart_types_in_schema(self):
        tools = get_tool_definitions()
        create_tool = next(t for t in tools if t["name"] == "create_dashboard")
        tile_props = create_tool["input_schema"]["properties"]["tiles"]["items"]["properties"]
        chart_type_enum = tile_props["chart_type"]["enum"]
        assert set(chart_type_enum) == {ct.value for ct in ChartType}


class TestToolExecutor:
    def test_list_models(self, executor, mock_registry):
        result, is_error = executor.execute("list_models", {})
        assert not is_error
        data = json.loads(result)
        assert len(data) == 2
        assert data[0]["name"] == "mart_seo_weekly_funnel"
        assert data[0]["has_omni_grant"] is True
        mock_registry.list_models.assert_called_once_with(layer=None)

    def test_list_models_with_layer(self, executor, mock_registry):
        executor.execute("list_models", {"layer": "staging"})
        mock_registry.list_models.assert_called_with(layer="staging")

    def test_get_model_detail(self, executor, mock_registry):
        result, is_error = executor.execute("get_model_detail", {"model_name": "mart_seo_weekly_funnel"})
        assert not is_error
        data = json.loads(result)
        assert data["name"] == "mart_seo_weekly_funnel"
        assert len(data["columns"]) == 3
        assert data["columns"][0]["name"] == "week_start"
        assert data["columns"][0]["data_type"] == "DATE"

    def test_search_models(self, executor, mock_registry):
        result, is_error = executor.execute("search_models", {"keyword": "seo"})
        assert not is_error
        data = json.loads(result)
        assert len(data) >= 1
        assert data[0]["name"] == "mart_seo_weekly_funnel"

    def test_create_dashboard_valid(self, executor):
        dashboard_input = {
            "name": "Test Dashboard",
            "tiles": [
                {
                    "name": "Visits Over Time",
                    "chart_type": "line",
                    "size": "half",
                    "query": {
                        "table": "mart_seo_weekly_funnel",
                        "fields": [
                            "mart_seo_weekly_funnel.week_start",
                            "mart_seo_weekly_funnel.organic_visits",
                        ],
                    },
                },
            ],
        }
        result, is_error = executor.execute("create_dashboard", dashboard_input)
        assert not is_error
        data = json.loads(result)
        assert data["status"] == "success"
        assert data["tile_count"] == 1
        assert executor.last_valid_definition is not None
        assert executor.last_valid_definition.name == "Test Dashboard"

    def test_create_dashboard_auto_positions(self, executor):
        dashboard_input = {
            "name": "Test",
            "tiles": [
                {
                    "name": "A",
                    "chart_type": "line",
                    "query": {"table": "t", "fields": ["t.a"]},
                },
                {
                    "name": "B",
                    "chart_type": "line",
                    "query": {"table": "t", "fields": ["t.b"]},
                },
            ],
        }
        executor.execute("create_dashboard", dashboard_input)
        defn = executor.last_valid_definition
        assert defn is not None
        # Auto-position should have set positions
        assert defn.tiles[0].position is not None
        assert defn.tiles[1].position is not None
        # Two half tiles should be side by side
        assert defn.tiles[0].position.x == 0
        assert defn.tiles[1].position.x == 6

    def test_create_dashboard_invalid_chart_type(self, executor):
        dashboard_input = {
            "name": "Bad",
            "tiles": [
                {
                    "name": "X",
                    "chart_type": "invalid_type",
                    "query": {"table": "t", "fields": ["t.a"]},
                },
            ],
        }
        result, is_error = executor.execute("create_dashboard", dashboard_input)
        assert is_error
        data = json.loads(result)
        assert data["status"] == "validation_error"
        assert "invalid_type" in data["errors"].lower() or "chart_type" in data["errors"].lower()

    def test_create_dashboard_no_fields(self, executor):
        dashboard_input = {
            "name": "Bad",
            "tiles": [
                {
                    "name": "X",
                    "chart_type": "line",
                    "query": {"table": "t", "fields": []},
                },
            ],
        }
        result, is_error = executor.execute("create_dashboard", dashboard_input)
        assert is_error
        data = json.loads(result)
        assert data["status"] == "validation_error"

    def test_unknown_tool(self, executor):
        result, is_error = executor.execute("nonexistent_tool", {})
        assert is_error
        data = json.loads(result)
        assert "Unknown tool" in data["error"]

    def test_last_valid_definition_initially_none(self, executor):
        assert executor.last_valid_definition is None

    def test_model_not_found_returns_error(self, executor, mock_registry):
        mock_registry.get_model.side_effect = Exception("not found")
        result, is_error = executor.execute("get_model_detail", {"model_name": "nope"})
        assert is_error
        data = json.loads(result)
        assert "not found" in data["error"]
