"""Tests for the dashboard serializer."""

import pytest

from omni_dash.dashboard.builder import DashboardBuilder
from omni_dash.dashboard.definition import (
    DashboardDefinition,
    SortSpec,
)
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

    # Sorts use snake_case (Omni queryJson format)
    assert qp0["query"]["sorts"][0]["column_name"] == "mart_seo_weekly_funnel.week_start"
    assert qp0["query"]["sorts"][0]["sort_descending"] is False
    assert qp0["query"]["sorts"][0]["null_sort"] == "DIALECT_DEFAULT"

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


def _make_omni_export_qp(name: str, chart_type: str, table: str, fields: list,
                          sorts=None, filters=None, vis_spec=None):
    """Helper to build a realistic Omni export queryPresentation structure."""
    query_json = {
        "table": table,
        "fields": fields,
        "sorts": sorts or [],
        "filters": filters or {},
        "limit": 200,
        "modelId": "model-123",
    }
    return {
        "queryPresentation": {
            "name": name,
            "description": "",
            "query": {
                "id": "q-1",
                "queryJson": query_json,
            },
            "visConfig": {
                "chartType": chart_type,
                "spec": vis_spec or {},
            },
        }
    }


def test_from_omni_export_reverse_maps_chart_types():
    """Verify Omni chart types are reverse-mapped when importing."""
    export_data = {
        "document": {"name": "Test", "modelId": "m1"},
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    _make_omni_export_qp("KPI", "kpi", "t", ["t.a"]),
                    _make_omni_export_qp("Stacked", "barStacked", "t", ["t.a", "t.b"]),
                    _make_omni_export_qp("Scatter", "point", "t", ["t.x", "t.y"]),
                ],
            },
            "metadata": {"layouts": {"lg": []}},
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
    """Test parsing a real Omni export structure with sorts, layout, and vis config."""
    export_data = {
        "document": {
            "name": "Exported Dashboard",
            "modelId": "model-456",
        },
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    _make_omni_export_qp(
                        "Chart 1", "bar", "my_table",
                        ["my_table.col1", "my_table.col2"],
                        sorts=[{"column_name": "my_table.col1", "sort_descending": True, "null_sort": "DIALECT_DEFAULT", "is_column_sort": False}],
                        vis_spec={"xAxis": "my_table.col1", "yAxis": ["my_table.col2"], "stacked": True},
                    ),
                ],
            },
            "metadata": {
                "layouts": {
                    "lg": [{"i": 1, "x": 0, "y": 0, "w": 24, "h": 8}],
                },
            },
        },
        "exportVersion": "0.1",
    }

    definition = DashboardSerializer.from_omni_export(export_data)
    assert definition.name == "Exported Dashboard"
    assert definition.model_id == "model-456"
    assert len(definition.tiles) == 1
    assert definition.tiles[0].chart_type == "bar"
    assert definition.tiles[0].vis_config.stacked is True
    # Verify sorts are parsed from camelCase
    assert len(definition.tiles[0].query.sorts) == 1
    assert definition.tiles[0].query.sorts[0].column_name == "my_table.col1"
    assert definition.tiles[0].query.sorts[0].sort_descending is True
    # Verify layout position (Omni 24-col → our 12-col: w=24//2=12)
    assert definition.tiles[0].position is not None
    assert definition.tiles[0].position.x == 0
    assert definition.tiles[0].position.w == 12


def test_kpi_tiles_have_no_sorts():
    """KPI tiles must NOT have sorts — Omni rejects sort fields not in the fields list."""
    definition = (
        DashboardBuilder("KPI Test")
        .model("m-1")
        .dbt_source("my_table")
        .add_number_tile("Metric A", metric_col="value_a")
        .build()
    )
    # Manually add a sort that references a column not in fields
    definition.tiles[0].query.sorts = [
        SortSpec(column_name="my_table.date_col", sort_descending=True)
    ]

    payload = DashboardSerializer.to_omni_create_payload(definition)
    kpi_qp = payload["queryPresentations"][0]

    # KPI tiles must have empty sorts
    assert kpi_qp["query"]["sorts"] == []
    assert kpi_qp["chartType"] == "kpi"
    # The sort column should NOT be added to fields for KPI tiles
    assert "my_table.date_col" not in kpi_qp["query"]["fields"]


def test_sort_columns_added_to_fields():
    """Sort columns not in the fields list should be automatically added."""
    from omni_dash.dashboard.definition import Tile, TileQuery, SortSpec as SS

    definition = DashboardDefinition(
        name="Sort Fix Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Chart",
                chart_type="line",
                query=TileQuery(
                    table="t",
                    fields=["t.metric"],
                    sorts=[SS(column_name="t.date_col", sort_descending=False)],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]

    # sort column should be added to fields automatically
    assert "t.date_col" in qp["query"]["fields"]
    assert "t.metric" in qp["query"]["fields"]
    assert qp["query"]["sorts"][0]["column_name"] == "t.date_col"


def test_kpi_limit_capped_at_1():
    """KPI tiles should have limit=1."""
    definition = (
        DashboardBuilder("Limit Test")
        .model("m-1")
        .dbt_source("t")
        .add_number_tile("KPI", metric_col="val")
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    assert payload["queryPresentations"][0]["query"]["limit"] == 1


def test_default_limit_upgraded_to_1000():
    """Default limit of 200 should be upgraded to Omni's standard 1000."""
    definition = (
        DashboardBuilder("Limit Test")
        .model("m-1")
        .dbt_source("t")
        .add_line_chart("Chart", time_col="d", metric_cols=["v"])
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    assert payload["queryPresentations"][0]["query"]["limit"] == 1000


def test_filter_format_omni():
    """Filters should be converted to Omni's {kind, type, values} format."""
    from omni_dash.dashboard.definition import Tile, TileQuery, FilterSpec as FS

    definition = DashboardDefinition(
        name="Filter Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Filtered",
                chart_type="bar",
                query=TileQuery(
                    table="t",
                    fields=["t.dim", "t.val"],
                    filters=[
                        FS(field="t.status", operator="is", value="active"),
                        FS(field="t.date", operator="before", value="1 weeks ago"),
                    ],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    filters = payload["queryPresentations"][0]["query"]["filters"]

    assert "t.status" in filters
    assert filters["t.status"]["kind"] == "EQUALS"
    assert filters["t.status"]["values"] == ["active"]

    assert "t.date" in filters
    assert filters["t.date"]["kind"] == "BEFORE"
    assert filters["t.date"]["type"] == "date"


def test_cartesian_spec_generated_for_line_chart():
    """Line chart with axis formatting should generate a cartesian spec."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Cartesian Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Revenue Trend",
                chart_type="line",
                query=TileQuery(table="t", fields=["t.date", "t.revenue"]),
                vis_config=TileVisConfig(
                    x_axis="t.date",
                    y_axis=["t.revenue"],
                    x_axis_format="%-m/%-d/%-Y",
                    x_axis_rotation=270,
                    y_axis_format="USDCURRENCY_0",
                    axis_label_y="Revenue ($)",
                    tooltip_fields=["t.date", "t.revenue"],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]

    assert vis["visType"] == "basic"
    assert vis["chartType"] == "line"

    spec = vis["spec"]
    assert spec["configType"] == "cartesian"
    assert spec["x"]["field"]["name"] == "t.date"
    assert spec["x"]["axis"]["label"]["format"]["format"] == "%-m/%-d/%-Y"
    assert spec["x"]["axis"]["label"]["format"]["angle"] == 270
    assert spec["y"]["axis"]["title"]["value"] == "Revenue ($)"
    assert spec["y"]["axis"]["label"]["format"]["format"] == "USDCURRENCY_0"
    assert spec["mark"]["type"] == "line"
    assert len(spec["tooltip"]) == 2


def test_kpi_vis_generates_markdown_config():
    """KPI tiles should generate omni-kpi visType with markdownConfig."""
    definition = (
        DashboardBuilder("KPI Vis Test")
        .model("m-1")
        .dbt_source("t")
        .add_number_tile("Total Revenue", metric_col="revenue", value_format="USDCURRENCY_0")
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]

    assert vis["visType"] == "omni-kpi"
    assert vis["chartType"] == "kpi"
    assert "markdownConfig" in vis["spec"]
    mc = vis["spec"]["markdownConfig"]
    assert len(mc) >= 1
    assert mc[0]["type"] == "number"
    assert mc[0]["config"]["field"]["field"]["name"] == "t.revenue"
    assert mc[0]["config"]["field"]["label"]["value"] == "Total Revenue"


def test_kpi_with_sparkline():
    """KPI tile with sparkline should include chart component in markdownConfig."""
    definition = (
        DashboardBuilder("Sparkline Test")
        .model("m-1")
        .dbt_source("t")
        .add_kpi_tile("Revenue", metric_col="rev", sparkline=True, sparkline_type="bar")
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    mc = payload["queryPresentations"][0]["visConfig"]["spec"]["markdownConfig"]

    assert len(mc) == 2
    assert mc[1]["type"] == "chart"
    assert mc[1]["config"]["type"] == "bar"


def test_markdown_tile_vis():
    """Markdown tiles should generate omni-markdown visType."""
    definition = (
        DashboardBuilder("Markdown Test")
        .model("m-1")
        .dbt_source("t")
        .add_markdown_tile("Header", template="<h1>Dashboard Title</h1>")
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]

    assert vis["visType"] == "omni-markdown"
    assert vis["chartType"] == "markdown"
    assert vis["spec"]["markdown"] == "<h1>Dashboard Title</h1>"


def test_table_vis_generates_spreadsheet_config():
    """Table tiles should generate omni-table visType with spreadsheet config."""
    definition = (
        DashboardBuilder("Table Test")
        .model("m-1")
        .dbt_source("t")
        .add_table("Data Table", columns=["col1", "col2"])
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]

    assert vis["visType"] == "omni-table"
    assert vis["chartType"] == "table"
    assert vis["spec"]["tableType"] == "spreadsheet"
    assert vis["spec"]["visColumnDisplay"] == "hide-view-name"


def test_dashboard_filters_applied_to_matching_tiles():
    """Dashboard-level filters should be propagated to matching tile queries."""
    definition = (
        DashboardBuilder("Filter Propagation")
        .model("m-1")
        .dbt_source("my_table")
        .add_line_chart("Chart", time_col="date", metric_cols=["visits"])
        .add_number_tile("KPI", metric_col="total")
        .add_filter("date", filter_type="date_range", default="last 12 weeks")
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)

    # Both tiles should have the dashboard filter applied
    for qp in payload["queryPresentations"]:
        filters = qp["query"].get("filters", {})
        assert "my_table.date" in filters
        assert filters["my_table.date"]["kind"] == "TIME_FOR_INTERVAL_DURATION"
        assert filters["my_table.date"]["type"] == "date"


def test_series_config_dual_axis():
    """Combo chart with series_config should generate dual-axis cartesian spec."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Dual Axis Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Revenue & Count",
                chart_type="combo",
                query=TileQuery(table="t", fields=["t.date", "t.revenue", "t.count"]),
                vis_config=TileVisConfig(
                    x_axis="t.date",
                    y2_axis=True,
                    y2_axis_format="BIGNUMBER_0",
                    series_config=[
                        {"field": "t.revenue", "mark_type": "bar", "y_axis": "y"},
                        {"field": "t.count", "mark_type": "line", "y_axis": "y2"},
                    ],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]
    spec = vis["spec"]

    assert vis["visType"] == "basic"
    assert "y2" in spec
    assert spec["y2"]["axis"]["label"]["format"]["format"] == "BIGNUMBER_0"
    assert len(spec["series"]) == 2
    assert spec["series"][0]["mark"]["type"] == "bar"
    assert spec["series"][0]["yAxis"] == "y"
    assert spec["series"][1]["mark"]["type"] == "line"
    assert spec["series"][1]["yAxis"] == "y2"


def test_value_format_in_kpi():
    """KPI value_format should propagate to markdownConfig."""
    definition = (
        DashboardBuilder("Format Test")
        .model("m-1")
        .dbt_source("t")
        .add_kpi_tile("Revenue", metric_col="rev", value_format="USDCURRENCY_0", label="Total Rev")
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    mc = payload["queryPresentations"][0]["visConfig"]["spec"]["markdownConfig"]
    assert mc[0]["config"]["field"]["format"] == "USDCURRENCY_0"
    assert mc[0]["config"]["field"]["label"]["value"] == "Total Rev"


def test_simple_chart_gets_cartesian_spec():
    """Line chart without advanced config should still get a full cartesian spec."""
    definition = (
        DashboardBuilder("Basic Test")
        .model("m-1")
        .dbt_source("t")
        .add_line_chart("Simple", time_col="d", metric_cols=["v"])
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]
    assert vis["visType"] == "basic"
    assert vis["chartType"] == "line"
    spec = vis["spec"]
    assert spec["configType"] == "cartesian"
    assert spec["x"]["field"]["name"] == "t.d"
    assert spec["mark"]["type"] == "line"
    # Auto-generated series for the metric field
    assert len(spec["series"]) == 1
    assert spec["series"][0]["field"]["name"] == "t.v"
    assert spec["series"][0]["yAxis"] == "y"


def test_area_chart_gets_cartesian_spec():
    """Area chart with only x_axis/y_axis should get a full cartesian spec, not basic config."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Area Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Weekly Spend",
                chart_type="area",
                query=TileQuery(table="t", fields=["t.week", "t.spend"]),
                vis_config=TileVisConfig(
                    x_axis="t.week",
                    y_axis=["t.spend"],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]
    assert vis["visType"] == "basic"
    assert vis["chartType"] == "area"
    spec = vis["spec"]
    assert spec["configType"] == "cartesian"
    assert spec["x"]["field"]["name"] == "t.week"
    assert spec["mark"]["type"] == "area"
    # Auto-generated series
    assert len(spec["series"]) == 1
    assert spec["series"][0]["field"]["name"] == "t.spend"
    assert spec["series"][0]["yAxis"] == "y"


def test_stacked_bar_auto_stacking():
    """Stacked bar chart should auto-set stackMultiMark and y-axis color stacking."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Stacked Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Revenue by Channel",
                chart_type="stacked_bar",
                query=TileQuery(table="t", fields=["t.month", "t.revenue", "t.channel"]),
                vis_config=TileVisConfig(
                    x_axis="t.month",
                    y_axis=["t.revenue"],
                    color_by="t.channel",
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]
    spec = vis["spec"]
    assert spec["behaviors"]["stackMultiMark"] is True
    assert spec["y"]["color"]["_stack"] == "stack"


def test_series_entry_default_yaxis():
    """Series entries with a field should default yAxis to 'y'."""
    from omni_dash.dashboard.serializer import _build_series_entry

    # No explicit y_axis → should default to "y"
    entry = _build_series_entry({"field": "t.revenue", "mark_type": "bar"})
    assert entry["yAxis"] == "y"

    # Explicit y_axis → should preserve
    entry2 = _build_series_entry({"field": "t.ctr", "mark_type": "line", "y_axis": "y2"})
    assert entry2["yAxis"] == "y2"


def test_auto_series_generation_multi_fields():
    """When no series_config, all non-x fields should get auto-generated series."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Multi Field Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Metrics",
                chart_type="line",
                query=TileQuery(table="t", fields=["t.date", "t.clicks", "t.impressions", "t.ctr"]),
                vis_config=TileVisConfig(
                    x_axis="t.date",
                    y_axis=["t.clicks", "t.impressions", "t.ctr"],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    spec = payload["queryPresentations"][0]["visConfig"]["spec"]
    # 3 auto-generated series (all fields except x_axis)
    assert len(spec["series"]) == 3
    field_names = [s["field"]["name"] for s in spec["series"]]
    assert field_names == ["t.clicks", "t.impressions", "t.ctr"]
    for s in spec["series"]:
        assert s["yAxis"] == "y"
        assert s["mark"]["type"] == "line"


def test_reference_lines_in_cartesian_spec():
    """Reference lines should appear in the Y axis spec."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Reference Line Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Revenue with Target",
                chart_type="line",
                query=TileQuery(table="t", fields=["t.date", "t.revenue"]),
                vis_config=TileVisConfig(
                    x_axis="t.date",
                    y_axis=["t.revenue"],
                    reference_lines=[{"value": 186, "label": "Goal", "dash": [8, 8]}],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]
    spec = vis["spec"]

    assert vis["visType"] == "basic"
    ref = spec["y"]["axis"]["referenceLine"]
    assert ref["enabled"] is True
    assert ref["value"] == 186
    assert ref["line"]["dash"] == [8, 8]
    assert ref["label"] == "Goal"


def test_heatmap_generates_heatmap_spec():
    """Heatmap tiles should generate heatmap configType spec."""
    definition = (
        DashboardBuilder("Heatmap Test")
        .model("m-1")
        .dbt_source("t")
        .add_heatmap(
            "NRR Cohort",
            x_col="weeks_since_cohort",
            y_col="cohort_label",
            color_col="cohort_nrr_percent",
            x_rotation=360,
        )
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]

    assert vis["visType"] == "basic"
    assert vis["chartType"] == "heatmap"
    spec = vis["spec"]
    assert spec["configType"] == "heatmap"
    assert spec["x"]["field"]["name"] == "t.weeks_since_cohort"
    assert spec["y"]["field"]["name"] == "t.cohort_label"
    assert spec["color"]["field"]["name"] == "t.cohort_nrr_percent"
    assert spec["x"]["axis"]["label"]["format"]["angle"] == 360
    assert spec["dataLabel"]["enabled"] is True


def test_vegalite_tile_generates_vegalite_vis():
    """Vega-Lite tiles should pass through the full spec."""
    vl_spec = {
        "height": 350,
        "layer": [
            {"mark": {"type": "bar"}, "encoding": {"x": {"field": "val"}}}
        ],
    }
    definition = (
        DashboardBuilder("VL Test")
        .model("m-1")
        .dbt_source("t")
        .add_vegalite_tile("Custom Funnel", spec=vl_spec, query_fields=["val", "label"])
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]

    assert vis["visType"] == "vegalite"
    assert vis["chartType"] == "code"
    spec = vis["spec"]
    assert spec["$schema"] == "https://vega.github.io/schema/vega-lite/v5.json"
    assert spec["width"] == "container"
    assert spec["background"] == "transparent"
    assert len(spec["layer"]) == 1
    # Fields array should include query fields for Omni
    assert "fields" in vis
    assert "t.val" in vis["fields"]
    assert "t.label" in vis["fields"]


def test_color_values_in_cartesian_spec():
    """Manual color mapping should be included in the cartesian spec."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Color Map Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="By Campaign",
                chart_type="line",
                query=TileQuery(table="t", fields=["t.date", "t.value", "t.type"]),
                vis_config=TileVisConfig(
                    x_axis="t.date",
                    color_by="t.type",
                    color_values={"Brand": "#FF8515", "Non-Brand": "#BE43C0"},
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    spec = payload["queryPresentations"][0]["visConfig"]["spec"]

    assert spec["color"]["field"]["name"] == "t.type"
    assert spec["color"]["manual"] is True
    assert spec["color"]["values"]["Brand"] == "#FF8515"


def test_composite_filters_in_payload():
    """Composite filters (AND/OR) should generate proper Omni format."""
    from omni_dash.dashboard.definition import (
        CalculatedField, CompositeFilter, FilterSpec as FS,
        Tile, TileQuery, TileVisConfig,
    )

    definition = DashboardDefinition(
        name="Composite Filter Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Filtered Chart",
                chart_type="line",
                query=TileQuery(
                    table="t",
                    fields=["t.date", "t.value"],
                    composite_filters=[
                        CompositeFilter(
                            conditions=[
                                FS(field="t.date", operator="date_range", value="2026-01-01"),
                                FS(field="t.date", operator="before", value="7 days ago"),
                            ],
                            conjunction="AND",
                        ),
                    ],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    filters = payload["queryPresentations"][0]["query"]["filters"]

    assert "t.date" in filters
    assert filters["t.date"]["type"] == "composite"
    assert filters["t.date"]["conjunction"] == "AND"
    assert len(filters["t.date"]["filters"]) == 2


def test_calculated_fields_in_payload():
    """Calculated fields should generate Omni AST format."""
    from omni_dash.dashboard.definition import CalculatedField, Tile, TileQuery

    definition = DashboardDefinition(
        name="Calc Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Activation Rate",
                chart_type="line",
                query=TileQuery(
                    table="t",
                    fields=["t.date", "t.activated", "t.signups"],
                    calculations=[
                        CalculatedField(
                            calc_name="calc_1",
                            label="Activation Rate",
                            formula="t.activated / t.signups",
                            format="PERCENT_1",
                        ),
                    ],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    calcs = payload["queryPresentations"][0]["query"]["calculations"]

    assert len(calcs) == 1
    assert calcs[0]["calc_name"] == "calc_1"
    assert calcs[0]["label"] == "Activation Rate"
    assert calcs[0]["format"] == "PERCENT_1"
    assert calcs[0]["sql_expression"]["operator"] == "Omni.OMNI_FX_SAFE_DIVIDE"
    assert calcs[0]["sql_expression"]["operands"][0]["field_name"] == "t.activated"


def test_field_metadata_in_payload():
    """Field metadata overrides should be included in the query."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="Metadata Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="DAU",
                chart_type="heatmap",
                query=TileQuery(
                    table="t",
                    fields=["t.date", "t.was_active_sum"],
                    metadata={"t.was_active_sum": {"label": "DAU"}},
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    meta = payload["queryPresentations"][0]["query"]["metadata"]

    assert meta["t.was_active_sum"]["label"] == "DAU"


def test_frozen_column_in_table_vis():
    """Table with frozen_column should include it in spec."""
    definition = (
        DashboardBuilder("Frozen Test")
        .model("m-1")
        .dbt_source("t")
        .add_table("Data", columns=["id", "name", "value"], frozen_column="id")
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    spec = payload["queryPresentations"][0]["visConfig"]["spec"]
    assert spec["frozenColumn"] == "t.id"


def test_data_labels_in_series_config():
    """Series with show_data_labels should generate proper dataLabel config."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Data Labels Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Labeled Chart",
                chart_type="line",
                query=TileQuery(table="t", fields=["t.date", "t.rate", "t.count"]),
                vis_config=TileVisConfig(
                    x_axis="t.date",
                    series_config=[
                        {
                            "field": "t.rate",
                            "mark_type": "line",
                            "color": "#FF6291",
                            "y_axis": "y",
                            "show_data_labels": True,
                            "data_label_format": "PERCENT_1",
                        },
                        {
                            "field": "t.count",
                            "mark_type": "bar",
                            "y_axis": "y2",
                            "show_data_labels": True,
                        },
                    ],
                    y2_axis=True,
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    series = payload["queryPresentations"][0]["visConfig"]["spec"]["series"]

    assert series[0]["dataLabel"]["enabled"] is True
    assert series[0]["dataLabel"]["format"] == "PERCENT_1"
    assert series[0]["mark"]["_mark_color"] == "#FF6291"
    assert series[1]["dataLabel"]["enabled"] is True


def test_kpi_comparison_swap_colors():
    """KPI with comparison should include comparisonType."""
    definition = (
        DashboardBuilder("KPI Comp")
        .model("m-1")
        .dbt_source("t")
        .add_kpi_tile(
            "CAC",
            metric_col="cac",
            comparison_col="prev_cac",
            comparison_type="percent",
        )
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    mc = payload["queryPresentations"][0]["visConfig"]["spec"]["markdownConfig"]

    assert len(mc) == 2
    assert mc[1]["type"] == "comparison"
    assert mc[1]["config"]["comparisonType"] == "percent"


def test_from_yaml_invalid():
    with pytest.raises(DashboardDefinitionError):
        DashboardSerializer.from_yaml("")


def test_from_yaml_empty_dict():
    with pytest.raises(DashboardDefinitionError):
        DashboardSerializer.from_yaml("null")


def test_markdown_fields_are_fully_qualified():
    """Markdown tile fields should be fully qualified (table.col), not stripped."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Markdown Fields Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="KPI Markdown",
                chart_type="text",
                query=TileQuery(
                    table="t",
                    fields=["t.day_start", "t.arr_sum"],
                ),
                vis_config=TileVisConfig(
                    markdown_template=(
                        '<single-value label="ARR">'
                        "{{result._last.t.arr_sum.value}}"
                        "</single-value>"
                    ),
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]

    assert vis["visType"] == "omni-markdown"
    assert vis["chartType"] == "markdown"
    # Fields must be fully qualified — NOT stripped of table prefix
    assert vis["fields"] == ["t.day_start", "t.arr_sum"]


def test_dashboard_filter_config_in_payload():
    """Dashboard filters should produce filterConfig and filterOrder in payload."""
    definition = (
        DashboardBuilder("Filter Config Test")
        .model("m-1")
        .dbt_source("t")
        .add_line_chart("Chart", time_col="date", metric_cols=["visits"])
        .add_filter("date", filter_type="date_range", label="Date Filter", default="last 12 weeks")
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)

    assert "filterConfig" in payload
    assert "filterOrder" in payload
    assert len(payload["filterOrder"]) == 1

    fid = payload["filterOrder"][0]
    fc = payload["filterConfig"][fid]
    assert fc["fieldName"] == "t.date"
    assert fc["label"] == "Date Filter"
    assert fc["kind"] == "TIME_FOR_INTERVAL_DURATION"
    assert fc["type"] == "date"


def test_vegalite_fields_included():
    """Vega-Lite tiles should include fields array from query."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="VL Fields Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="VL Chart",
                chart_type="vegalite",
                query=TileQuery(
                    table="t",
                    fields=["t.week", "t.category", "t.value"],
                ),
                vis_config=TileVisConfig(
                    vegalite_spec={
                        "mark": "bar",
                        "encoding": {
                            "x": {"field": "t\\.week", "type": "ordinal"},
                            "y": {"field": "t\\.value", "type": "quantitative"},
                        },
                    },
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]

    assert vis["fields"] == ["t.week", "t.category", "t.value"]
    assert vis["spec"]["mark"] == "bar"
