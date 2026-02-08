"""Tests for the dashboard builder."""

import pytest

from omni_dash.dashboard.builder import DashboardBuilder
from omni_dash.exceptions import DashboardDefinitionError


def test_basic_build():
    dashboard = (
        DashboardBuilder("Test Dashboard")
        .model("model-123")
        .dbt_source("mart_seo_weekly_funnel")
        .add_line_chart(
            "Visits",
            time_col="week_start",
            metric_cols=["organic_visits_total"],
        )
        .build()
    )
    assert dashboard.name == "Test Dashboard"
    assert dashboard.model_id == "model-123"
    assert dashboard.dbt_model == "mart_seo_weekly_funnel"
    assert len(dashboard.tiles) == 1
    assert dashboard.tiles[0].chart_type == "line"


def test_multiple_tile_types():
    dashboard = (
        DashboardBuilder("Multi-Tile")
        .model("m")
        .table("tbl")
        .add_line_chart("Lines", time_col="date", metric_cols=["m1"])
        .add_bar_chart("Bars", dimension_col="channel", metric_cols=["m1"])
        .add_table("Table", columns=["c1", "c2", "c3"])
        .add_number_tile("KPI", metric_col="total")
        .build()
    )
    assert len(dashboard.tiles) == 4
    assert dashboard.tiles[0].chart_type == "line"
    assert dashboard.tiles[1].chart_type == "bar"
    assert dashboard.tiles[2].chart_type == "table"
    assert dashboard.tiles[3].chart_type == "number"


def test_field_qualification():
    dashboard = (
        DashboardBuilder("Test")
        .model("m")
        .table("my_table")
        .add_line_chart("Test", time_col="date", metric_cols=["value"])
        .build()
    )
    fields = dashboard.tiles[0].query.fields
    assert "my_table.date" in fields
    assert "my_table.value" in fields


def test_already_qualified_fields():
    dashboard = (
        DashboardBuilder("Test")
        .model("m")
        .table("tbl")
        .add_line_chart("Test", time_col="other.date", metric_cols=["other.val"])
        .build()
    )
    fields = dashboard.tiles[0].query.fields
    assert "other.date" in fields
    assert "other.val" in fields


def test_auto_layout():
    dashboard = (
        DashboardBuilder("Layout Test")
        .model("m")
        .table("tbl")
        .add_line_chart("A", time_col="d", metric_cols=["m1"], size="half")
        .add_line_chart("B", time_col="d", metric_cols=["m2"], size="half")
        .add_table("C", columns=["c1"], size="full")
        .auto_layout()
        .build()
    )
    # After auto-layout, all tiles should have positions
    for tile in dashboard.tiles:
        assert tile.position is not None

    # First two half-width tiles should be on the same row
    assert dashboard.tiles[0].position.y == dashboard.tiles[1].position.y
    assert dashboard.tiles[0].position.x == 0
    assert dashboard.tiles[1].position.x == 6

    # Table should be below
    assert dashboard.tiles[2].position.y > dashboard.tiles[0].position.y


def test_filters():
    dashboard = (
        DashboardBuilder("Filter Test")
        .model("m")
        .table("tbl")
        .add_line_chart("Test", time_col="date", metric_cols=["val"])
        .add_filter("date", filter_type="date_range", default="last 12 weeks")
        .build()
    )
    assert len(dashboard.filters) == 1
    assert dashboard.filters[0].filter_type == "date_range"
    assert "tbl.date" in dashboard.filters[0].field


def test_empty_dashboard_raises():
    with pytest.raises(DashboardDefinitionError, match="at least one tile"):
        DashboardBuilder("Empty").model("m").build()


def test_stacked_bar():
    dashboard = (
        DashboardBuilder("Stacked")
        .model("m")
        .table("t")
        .add_bar_chart("Test", dimension_col="ch", metric_cols=["v"], stacked=True)
        .build()
    )
    assert dashboard.tiles[0].chart_type == "stacked_bar"


def test_area_chart():
    dashboard = (
        DashboardBuilder("Area")
        .model("m")
        .table("t")
        .add_area_chart("Test", time_col="d", metric_cols=["v1", "v2"], stacked=True)
        .build()
    )
    assert dashboard.tiles[0].chart_type == "stacked_area"


def test_pie_chart():
    dashboard = (
        DashboardBuilder("Pie")
        .model("m")
        .table("t")
        .add_pie_chart("Test", dimension_col="cat", metric_col="val")
        .build()
    )
    assert dashboard.tiles[0].chart_type == "pie"


def test_text_tile():
    dashboard = (
        DashboardBuilder("With Text")
        .model("m")
        .table("t")
        .add_line_chart("Chart", time_col="d", metric_cols=["v"])
        .add_text("# Dashboard Title\nSome description here.")
        .build()
    )
    assert len(dashboard.text_tiles) == 1
    assert "Dashboard Title" in dashboard.text_tiles[0].content


def test_labels_and_folder():
    dashboard = (
        DashboardBuilder("Test")
        .model("m")
        .table("t")
        .folder("folder-123")
        .label("seo")
        .label("weekly")
        .add_line_chart("C", time_col="d", metric_cols=["v"])
        .build()
    )
    assert dashboard.folder_id == "folder-123"
    assert "seo" in dashboard.labels
    assert "weekly" in dashboard.labels


def test_add_combo_chart():
    dashboard = (
        DashboardBuilder("Combo")
        .model("m")
        .table("t")
        .add_combo_chart(
            "Revenue & Count",
            time_col="date",
            bar_cols=["revenue"],
            line_cols=["count"],
            y_format="USDCURRENCY_0",
            y2_format="BIGNUMBER_0",
        )
        .build()
    )
    tile = dashboard.tiles[0]
    assert tile.chart_type == "combo"
    assert tile.vis_config.y2_axis is True
    assert tile.vis_config.y_axis_format == "USDCURRENCY_0"
    assert tile.vis_config.y2_axis_format == "BIGNUMBER_0"
    assert len(tile.vis_config.series_config) == 2
    assert tile.vis_config.series_config[0]["mark_type"] == "bar"
    assert tile.vis_config.series_config[1]["mark_type"] == "line"


def test_add_markdown_tile():
    dashboard = (
        DashboardBuilder("Markdown")
        .model("m")
        .table("t")
        .add_markdown_tile(
            "Header",
            template="<h1>Dashboard</h1><p>{{result._last.field.value}}</p>",
        )
        .build()
    )
    tile = dashboard.tiles[0]
    assert tile.chart_type == "text"
    assert tile.vis_config.markdown_template == "<h1>Dashboard</h1><p>{{result._last.field.value}}</p>"


def test_add_kpi_tile_with_sparkline():
    dashboard = (
        DashboardBuilder("KPI")
        .model("m")
        .table("t")
        .add_kpi_tile(
            "Revenue",
            metric_col="total_revenue",
            label="Total Revenue",
            value_format="USDCURRENCY_0",
            comparison_col="prev_revenue",
            comparison_type="number_percent",
            sparkline=True,
            sparkline_type="bar",
        )
        .build()
    )
    tile = dashboard.tiles[0]
    assert tile.chart_type == "number"
    assert tile.vis_config.kpi_label == "Total Revenue"
    assert tile.vis_config.value_format == "USDCURRENCY_0"
    assert tile.vis_config.kpi_comparison_field == "t.prev_revenue"
    assert tile.vis_config.kpi_comparison_type == "number_percent"
    assert tile.vis_config.kpi_sparkline is True
    assert tile.vis_config.kpi_sparkline_type == "bar"
    # comparison_col should be in fields
    assert "t.prev_revenue" in tile.query.fields


def test_line_chart_with_axis_format():
    dashboard = (
        DashboardBuilder("Formatted")
        .model("m")
        .table("t")
        .add_line_chart(
            "Revenue",
            time_col="date",
            metric_cols=["revenue"],
            axis_title_y="Revenue ($)",
            date_format="%-m/%-d/%-Y",
            label_rotation=270,
            value_format="USDCURRENCY_0",
            tooltip_fields=["date", "revenue"],
        )
        .build()
    )
    tile = dashboard.tiles[0]
    assert tile.vis_config.axis_label_y == "Revenue ($)"
    assert tile.vis_config.x_axis_format == "%-m/%-d/%-Y"
    assert tile.vis_config.x_axis_rotation == 270
    assert tile.vis_config.y_axis_format == "USDCURRENCY_0"
    assert "t.date" in tile.vis_config.tooltip_fields
