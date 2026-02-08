"""Tests for the dashboard serializer."""

import pytest

from omni_dash.dashboard.builder import DashboardBuilder
from omni_dash.dashboard.serializer import DashboardSerializer
from omni_dash.exceptions import DashboardDefinitionError


@pytest.fixture
def sample_definition():
    return (
        DashboardBuilder("SEO Weekly Funnel")
        .model("abc-123")
        .dbt_source("mart_seo_weekly_funnel")
        .add_line_chart(
            "Organic Visits",
            time_col="week_start",
            metric_cols=["organic_visits_total"],
        )
        .add_bar_chart(
            "Signups by Channel",
            dimension_col="channel",
            metric_cols=["signups"],
            stacked=True,
        )
        .add_number_tile("Current ARR", metric_col="running_plg_arr")
        .add_filter("week_start", filter_type="date_range", default="last 12 weeks")
        .auto_layout()
        .build()
    )


def test_to_omni_payload(sample_definition):
    payload = DashboardSerializer.to_omni_create_payload(sample_definition)

    assert payload["modelId"] == "abc-123"
    assert payload["name"] == "SEO Weekly Funnel"
    assert len(payload["queryPresentations"]) == 3

    # First tile
    qp0 = payload["queryPresentations"][0]
    assert qp0["name"] == "Organic Visits"
    assert qp0["chartType"] == "line"
    assert qp0["prefersChart"] is True
    assert qp0["query"]["modelId"] == "abc-123"
    assert "mart_seo_weekly_funnel.week_start" in qp0["query"]["fields"]

    # Number tile → mapped to "kpi" for Omni API
    qp2 = payload["queryPresentations"][2]
    assert qp2["chartType"] == "kpi"

    # Stacked bar → mapped to "barStacked" for Omni API
    qp1 = payload["queryPresentations"][1]
    assert qp1["chartType"] == "barStacked"


def test_chart_type_mapping_all():
    """Verify all internal chart types map to valid Omni API types."""
    from omni_dash.dashboard.serializer import _CHART_TYPE_TO_OMNI

    omni_valid = {
        "auto", "area", "areaStacked", "areaStackedPercentage",
        "bar", "barLine", "barGrouped", "barStacked", "barStackedPercentage",
        "boxplot", "code", "column", "columnGrouped", "columnStacked",
        "columnStackedPercentage", "heatmap", "kpi", "line", "lineColor",
        "map", "regionMap", "markdown", "omni-ai-summary-markdown", "pie",
        "sankey", "point", "pointColor", "pointSize", "pointSizeColor",
        "singleRecord", "omni-spreadsheet", "summaryValue", "table",
    }
    for internal, omni in _CHART_TYPE_TO_OMNI.items():
        assert omni in omni_valid, f"Internal '{internal}' maps to invalid Omni type '{omni}'"


def test_from_omni_export_reverse_maps_chart_types():
    """Verify Omni chart types are reverse-mapped when importing."""
    export_data = {
        "document": {"name": "Test", "modelId": "m1"},
        "dashboard": {
            "queryPresentations": [
                {
                    "name": "KPI",
                    "chartType": "kpi",
                    "query": {"table": "t", "fields": ["t.a"]},
                    "visualization": {"config": {}},
                },
                {
                    "name": "Stacked",
                    "chartType": "barStacked",
                    "query": {"table": "t", "fields": ["t.a", "t.b"]},
                    "visualization": {"config": {}},
                },
                {
                    "name": "Scatter",
                    "chartType": "point",
                    "query": {"table": "t", "fields": ["t.x", "t.y"]},
                    "visualization": {"config": {}},
                },
            ],
        },
    }
    defn = DashboardSerializer.from_omni_export(export_data)
    assert defn.tiles[0].chart_type == "number"
    assert defn.tiles[1].chart_type == "stacked_bar"
    assert defn.tiles[2].chart_type == "scatter"


def test_payload_requires_model_id():
    definition = (
        DashboardBuilder("No Model")
        .table("t")
        .add_line_chart("C", time_col="d", metric_cols=["v"])
        .build()
    )
    with pytest.raises(DashboardDefinitionError, match="model_id"):
        DashboardSerializer.to_omni_create_payload(definition)


def test_yaml_round_trip(sample_definition):
    yaml_str = DashboardSerializer.to_yaml(sample_definition)
    assert "SEO Weekly Funnel" in yaml_str
    assert "organic_visits_total" in yaml_str

    # Parse it back
    restored = DashboardSerializer.from_yaml(yaml_str)
    assert restored.name == sample_definition.name
    assert len(restored.tiles) == len(sample_definition.tiles)
    assert restored.tiles[0].chart_type == "line"
    assert restored.tiles[1].chart_type == "stacked_bar"


def test_yaml_preserves_filters(sample_definition):
    yaml_str = DashboardSerializer.to_yaml(sample_definition)
    restored = DashboardSerializer.from_yaml(yaml_str)
    assert len(restored.filters) == 1
    assert restored.filters[0].filter_type == "date_range"


def test_yaml_preserves_positions(sample_definition):
    yaml_str = DashboardSerializer.to_yaml(sample_definition)
    restored = DashboardSerializer.from_yaml(yaml_str)
    for orig, rest in zip(sample_definition.tiles, restored.tiles):
        if orig.position:
            assert rest.position is not None
            assert rest.position.x == orig.position.x
            assert rest.position.y == orig.position.y
            assert rest.position.w == orig.position.w


def test_from_omni_export():
    export_data = {
        "document": {
            "name": "Exported Dashboard",
            "modelId": "model-456",
        },
        "dashboard": {
            "queryPresentations": [
                {
                    "name": "Chart 1",
                    "chartType": "bar",
                    "query": {
                        "table": "my_table",
                        "fields": ["my_table.col1", "my_table.col2"],
                        "sorts": [{"columnName": "my_table.col1", "sortDescending": True}],
                        "limit": 100,
                    },
                    "visualization": {
                        "config": {
                            "xAxis": "my_table.col1",
                            "yAxis": ["my_table.col2"],
                            "stacked": True,
                        }
                    },
                }
            ]
        },
        "exportVersion": "0.1",
    }

    definition = DashboardSerializer.from_omni_export(export_data)
    assert definition.name == "Exported Dashboard"
    assert definition.model_id == "model-456"
    assert len(definition.tiles) == 1
    assert definition.tiles[0].chart_type == "bar"
    assert definition.tiles[0].vis_config.stacked is True


def test_from_yaml_invalid():
    with pytest.raises(DashboardDefinitionError):
        DashboardSerializer.from_yaml("")


def test_from_yaml_empty_dict():
    with pytest.raises(DashboardDefinitionError):
        DashboardSerializer.from_yaml("null")
