"""Tests for the dashboard serializer."""

import pytest

from omni_dash.dashboard.builder import DashboardBuilder
from omni_dash.dashboard.definition import (
    DashboardDefinition,
    SortSpec,
    TileVisConfig,
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

    # Query identifier map keys (1-indexed)
    for idx, qp in enumerate(payload["queryPresentations"], start=1):
        assert qp["queryIdentifierMapKey"] == str(idx), (
            f"QP {idx} missing queryIdentifierMapKey"
        )

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

    # Required top-level fields for Omni API
    assert payload["metadataVersion"] == 2
    assert isinstance(payload["filterConfig"], dict)
    assert isinstance(payload["filterOrder"], list)


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


# ── Wave 1: New feature tests ───────────────────────────────────────────────


def test_tile_subtitle_in_payload():
    """Tile subtitle should appear as subTitle in queryPresentation."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="Subtitle Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Revenue",
                subtitle="Last 90 days",
                chart_type="line",
                query=TileQuery(table="t", fields=["t.date", "t.revenue"]),
                vis_config=TileVisConfig(x_axis="t.date"),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    assert payload["queryPresentations"][0]["subTitle"] == "Last 90 days"


def test_tile_subtitle_omitted_when_empty():
    """Empty subtitle should not produce a subTitle key."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="No Subtitle",
        model_id="m-1",
        tiles=[
            Tile(
                name="Chart",
                chart_type="line",
                query=TileQuery(table="t", fields=["t.date", "t.v"]),
                vis_config=TileVisConfig(x_axis="t.date"),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    assert "subTitle" not in payload["queryPresentations"][0]


def test_hidden_tiles_in_metadata():
    """Hidden tiles should appear in payload metadata.hiddenTiles."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="Hidden Test",
        model_id="m-1",
        tiles=[
            Tile(name="Visible", chart_type="line", query=TileQuery(table="t", fields=["t.v"])),
            Tile(name="Secret", chart_type="line", query=TileQuery(table="t", fields=["t.v"]), hidden=True),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    assert "metadata" in payload
    assert payload["metadata"]["hiddenTiles"] == ["Secret"]


def test_sql_tile_in_payload():
    """SQL tile should produce isSql and userEditedSQL in the payload."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="SQL Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Custom SQL",
                chart_type="table",
                query=TileQuery(
                    table="t",
                    fields=["t.id"],
                    is_sql=True,
                    user_sql="SELECT * FROM my_table LIMIT 10",
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    assert qp["isSql"] is True
    assert qp["query"]["userEditedSQL"] == "SELECT * FROM my_table LIMIT 10"


def test_row_totals_in_payload():
    """Row totals should produce row_totals: {} in query."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="Totals Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Pivot",
                chart_type="table",
                query=TileQuery(table="t", fields=["t.a", "t.b"], row_totals=True),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    assert payload["queryPresentations"][0]["query"]["row_totals"] == {}


def test_column_totals_in_payload():
    """Column totals should produce column_totals: {} in query."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="Col Totals",
        model_id="m-1",
        tiles=[
            Tile(
                name="Pivot",
                chart_type="table",
                query=TileQuery(table="t", fields=["t.a", "t.b"], column_totals=True),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    assert payload["queryPresentations"][0]["query"]["column_totals"] == {}


def test_fill_fields_in_payload():
    """Fill fields should appear in query."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="Fill Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Chart",
                chart_type="line",
                query=TileQuery(
                    table="t",
                    fields=["t.date", "t.v"],
                    fill_fields=["t.date"],
                ),
                vis_config=TileVisConfig(x_axis="t.date"),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    assert payload["queryPresentations"][0]["query"]["fill_fields"] == ["t.date"]


def test_kpi_alignment_in_vis():
    """KPI alignment fields should flow through to the spec."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="KPI Alignment",
        model_id="m-1",
        tiles=[
            Tile(
                name="ARR",
                chart_type="number",
                query=TileQuery(table="t", fields=["t.arr"], limit=1),
                vis_config=TileVisConfig(
                    kpi_alignment="center",
                    kpi_vertical_alignment="center",
                    kpi_font_size="32px",
                    kpi_label_font_size="14px",
                    kpi_body_font_size="12px",
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    spec = payload["queryPresentations"][0]["visConfig"]["spec"]
    assert spec["alignment"] == "center"
    assert spec["verticalAlignment"] == "center"
    assert spec["fontKPISize"] == "32px"
    assert spec["fontLabelSize"] == "14px"
    assert spec["fontBodySize"] == "12px"


def test_table_row_banding_in_vis():
    """Table row banding should flow through to the spec."""
    definition = (
        DashboardBuilder("Banding Test")
        .model("m-1")
        .dbt_source("t")
        .add_table("Data", columns=["a", "b"])
        .build()
    )
    # Enable row banding on the tile
    definition.tiles[0].vis_config.table_row_banding = True
    definition.tiles[0].vis_config.table_hide_index = True
    definition.tiles[0].vis_config.table_truncate_headers = False
    definition.tiles[0].vis_config.table_show_descriptions = False

    payload = DashboardSerializer.to_omni_create_payload(definition)
    spec = payload["queryPresentations"][0]["visConfig"]["spec"]
    assert spec["rowBanding"]["enabled"] is True
    assert spec["hideIndexColumn"] is True
    assert spec["truncateHeaders"] is False
    assert spec["showDescriptions"] is False


def test_trendline_in_cartesian_spec():
    """Trendline should appear in the cartesian spec."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="Trendline Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Revenue Trend",
                chart_type="line",
                query=TileQuery(table="t", fields=["t.date", "t.revenue"]),
                vis_config=TileVisConfig(
                    x_axis="t.date",
                    y_axis=["t.revenue"],
                    show_trendline=True,
                    trendline_type="linear",
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    spec = payload["queryPresentations"][0]["visConfig"]["spec"]
    assert spec["trendline"]["enabled"] is True
    assert spec["trendline"]["type"] == "linear"


def test_trendline_moving_average():
    """Moving average trendline should include window."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="MA Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Smoothed",
                chart_type="line",
                query=TileQuery(table="t", fields=["t.date", "t.v"]),
                vis_config=TileVisConfig(
                    x_axis="t.date",
                    show_trendline=True,
                    trendline_type="moving_average",
                    moving_average_window=7,
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    spec = payload["queryPresentations"][0]["visConfig"]["spec"]
    assert spec["trendline"]["type"] == "moving_average"
    assert spec["trendline"]["window"] == 7


def test_theme_in_payload():
    """Dashboard theme should appear as dashboardCustomTheme."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    theme = {"primary_color": "#1A73E8", "font_family": "Inter"}
    definition = DashboardDefinition(
        name="Theme Test",
        model_id="m-1",
        tiles=[
            Tile(name="C", chart_type="line", query=TileQuery(table="t", fields=["t.v"])),
        ],
        theme=theme,
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    assert payload["dashboardCustomTheme"] == theme


def test_tile_filter_map_in_payload():
    """Tile filter map should appear in metadata."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    mapping = {"tile_1": {"filter_a": "value"}}
    definition = DashboardDefinition(
        name="TFM Test",
        model_id="m-1",
        tiles=[
            Tile(name="C", chart_type="line", query=TileQuery(table="t", fields=["t.v"])),
        ],
        tile_filter_map=mapping,
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    assert payload["metadata"]["tileFilterMap"] == mapping


def test_refresh_interval_in_payload():
    """Non-default refresh interval should appear in payload."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="Refresh Test",
        model_id="m-1",
        tiles=[
            Tile(name="C", chart_type="line", query=TileQuery(table="t", fields=["t.v"])),
        ],
        refresh_interval=300,
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    assert payload["refreshInterval"] == 300


def test_refresh_interval_default_omitted():
    """Default refresh interval (3600) should not be in payload."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="Default Refresh",
        model_id="m-1",
        tiles=[
            Tile(name="C", chart_type="line", query=TileQuery(table="t", fields=["t.v"])),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    assert "refreshInterval" not in payload


def test_builder_sql_tile():
    """Builder add_sql_tile should set is_sql and user_sql."""
    definition = (
        DashboardBuilder("SQL Builder")
        .model("m-1")
        .dbt_source("t")
        .add_sql_tile("Custom", sql="SELECT 1", fields=["id"])
        .build()
    )
    tile = definition.tiles[0]
    assert tile.query.is_sql is True
    assert tile.query.user_sql == "SELECT 1"

    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    assert qp["isSql"] is True


def test_builder_subtitle_line_chart():
    """Builder should pass subtitle through to the tile."""
    definition = (
        DashboardBuilder("Subtitle Builder")
        .model("m-1")
        .dbt_source("t")
        .add_line_chart("Revenue", time_col="date", metric_cols=["rev"], subtitle="YTD")
        .build()
    )
    assert definition.tiles[0].subtitle == "YTD"


def test_builder_trendline():
    """Builder should pass trendline params through."""
    definition = (
        DashboardBuilder("Trendline Builder")
        .model("m-1")
        .dbt_source("t")
        .add_line_chart("Trend", time_col="date", metric_cols=["v"], show_trendline=True, trendline_type="linear")
        .build()
    )
    assert definition.tiles[0].vis_config.show_trendline is True
    assert definition.tiles[0].vis_config.trendline_type == "linear"


def test_builder_theme():
    """Builder theme method should set the theme."""
    definition = (
        DashboardBuilder("Theme Builder")
        .model("m-1")
        .dbt_source("t")
        .add_line_chart("C", time_col="d", metric_cols=["v"])
        .theme({"bg": "#FFF"})
        .build()
    )
    assert definition.theme == {"bg": "#FFF"}


def test_date_filter_dict_value():
    """Date range filter with dict default_value should serialize correctly."""
    from omni_dash.dashboard.definition import DashboardFilter, Tile, TileQuery

    definition = DashboardDefinition(
        name="Dict Filter",
        model_id="m-1",
        tiles=[
            Tile(
                name="C",
                chart_type="line",
                query=TileQuery(table="t", fields=["t.date", "t.v"]),
                vis_config=TileVisConfig(x_axis="t.date"),
            ),
        ],
        filters=[
            DashboardFilter(
                field="t.date",
                filter_type="date_range",
                default_value={"left": "90 days ago", "right": "90 days"},
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    # Filter should be applied to the tile
    tile_filters = payload["queryPresentations"][0]["query"]["filters"]
    assert tile_filters["t.date"]["left_side"] == "90 days ago"
    assert tile_filters["t.date"]["right_side"] == "90 days"


# ---------------------------------------------------------------------------
# Bug 1: Auto-series excludes pivot/color fields
# ---------------------------------------------------------------------------


def test_auto_series_excludes_pivot_fields():
    """Pivot fields should not appear as y-axis series."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Pivot Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Stacked Bar",
                chart_type="stacked_bar",
                query=TileQuery(
                    table="t",
                    fields=["t.week", "t.source", "t.sessions"],
                    pivots=["t.source"],
                ),
                vis_config=TileVisConfig(x_axis="t.week"),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]
    series = vis["spec"]["series"]
    series_fields = [s["field"]["name"] for s in series]
    assert "t.source" not in series_fields
    assert "t.sessions" in series_fields


def test_auto_series_excludes_color_by_field():
    """color_by field should not appear as y-axis series."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Color Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Colored Line",
                chart_type="line",
                query=TileQuery(
                    table="t",
                    fields=["t.date", "t.channel", "t.revenue"],
                ),
                vis_config=TileVisConfig(x_axis="t.date", color_by="t.channel"),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]
    series = vis["spec"]["series"]
    series_fields = [s["field"]["name"] for s in series]
    assert "t.channel" not in series_fields
    assert "t.revenue" in series_fields


def test_auto_series_excludes_both_pivot_and_color():
    """Both pivot and color_by fields excluded from auto-series."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Pivot+Color Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Complex Chart",
                chart_type="stacked_bar",
                query=TileQuery(
                    table="t",
                    fields=["t.week", "t.source", "t.campaign", "t.clicks"],
                    pivots=["t.source"],
                ),
                vis_config=TileVisConfig(x_axis="t.week", color_by="t.campaign"),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]
    series = vis["spec"]["series"]
    series_fields = [s["field"]["name"] for s in series]
    assert "t.source" not in series_fields
    assert "t.campaign" not in series_fields
    assert "t.clicks" in series_fields
    assert len(series_fields) == 1


# ---------------------------------------------------------------------------
# Bug 2: KPI smart field selection
# ---------------------------------------------------------------------------


def test_kpi_explicit_kpi_field():
    """Explicit kpi_field should override default field selection."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="KPI Explicit",
        model_id="m-1",
        tiles=[
            Tile(
                name="Sessions",
                chart_type="number",
                query=TileQuery(
                    table="t",
                    fields=["t.week_start", "t.organic_sessions"],
                ),
                vis_config=TileVisConfig(kpi_field="t.organic_sessions"),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]
    kpi_field = vis["spec"]["markdownConfig"][0]["config"]["field"]["field"]["name"]
    assert kpi_field == "t.organic_sessions"


def test_kpi_heuristic_picks_measure_over_date():
    """Without explicit kpi_field, heuristic should pick measure, not date."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="KPI Heuristic",
        model_id="m-1",
        tiles=[
            Tile(
                name="Total Visits",
                chart_type="number",
                query=TileQuery(
                    table="t",
                    fields=["t.week_start", "t.total_visits_count"],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]
    kpi_field = vis["spec"]["markdownConfig"][0]["config"]["field"]["field"]["name"]
    assert kpi_field == "t.total_visits_count"


def test_kpi_fallback_to_first_field():
    """When no measure pattern matches, fall back to fields[0]."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="KPI Fallback",
        model_id="m-1",
        tiles=[
            Tile(
                name="Status",
                chart_type="number",
                query=TileQuery(
                    table="t",
                    fields=["t.some_val", "t.other_val"],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]
    kpi_field = vis["spec"]["markdownConfig"][0]["config"]["field"]["field"]["name"]
    assert kpi_field == "t.some_val"


# ---------------------------------------------------------------------------
# Bug 3: Cross-table filter propagation
# ---------------------------------------------------------------------------


def test_filter_same_table_direct_match():
    """Dashboard filter on same table applies directly."""
    from omni_dash.dashboard.definition import DashboardFilter, Tile, TileQuery

    definition = DashboardDefinition(
        name="Same Table Filter",
        model_id="m-1",
        tiles=[
            Tile(
                name="Chart",
                chart_type="line",
                query=TileQuery(table="funnel", fields=["funnel.week", "funnel.visits"]),
            ),
        ],
        filters=[
            DashboardFilter(
                field="funnel.week",
                filter_type="date_range",
                label="Date",
                default_value="last 12 weeks",
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    tile_filters = payload["queryPresentations"][0]["query"]["filters"]
    assert "funnel.week" in tile_filters


def test_filter_cross_table_remap():
    """Dashboard filter on table_a.col remaps to table_b.col when table_b has same column."""
    from omni_dash.dashboard.definition import DashboardFilter, Tile, TileQuery

    definition = DashboardDefinition(
        name="Cross Table Filter",
        model_id="m-1",
        tiles=[
            Tile(
                name="LLM Sessions",
                chart_type="bar",
                query=TileQuery(
                    table="llm", fields=["llm.week", "llm.session_count"]
                ),
            ),
        ],
        filters=[
            DashboardFilter(
                field="funnel.week",
                filter_type="date_range",
                label="Date",
                default_value="last 12 weeks",
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    tile_filters = payload["queryPresentations"][0]["query"]["filters"]
    # Should be remapped to llm.week, not funnel.week
    assert "llm.week" in tile_filters
    assert "funnel.week" not in tile_filters


def test_filter_cross_table_no_match_skips():
    """Dashboard filter on table_a.col should NOT apply to table_b if no matching column."""
    from omni_dash.dashboard.definition import DashboardFilter, Tile, TileQuery

    definition = DashboardDefinition(
        name="No Match Filter",
        model_id="m-1",
        tiles=[
            Tile(
                name="Pages",
                chart_type="table",
                query=TileQuery(
                    table="pages", fields=["pages.url", "pages.clicks"]
                ),
            ),
        ],
        filters=[
            DashboardFilter(
                field="funnel.week_start",
                filter_type="date_range",
                label="Date",
                default_value="last 12 weeks",
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    # No filters should be applied — pages has no week_start column
    assert "filters" not in qp["query"] or not qp["query"].get("filters")


# ---------------------------------------------------------------------------
# Bug 5: color alias for color_by
# ---------------------------------------------------------------------------


def test_color_alias_maps_to_color_by():
    """Passing 'color' in vis_config dict should map to color_by."""
    vc = TileVisConfig(**{"color": "t.channel", "x_axis": "t.date"})
    assert vc.color_by == "t.channel"


def test_color_by_takes_precedence_over_color():
    """Explicit color_by should take precedence over color alias."""
    vc = TileVisConfig(**{"color": "t.channel", "color_by": "t.source", "x_axis": "t.date"})
    assert vc.color_by == "t.source"


# ---------------------------------------------------------------------------
# Date filter normalization
# ---------------------------------------------------------------------------


class TestDateFilterNormalization:
    """_normalize_date_to_days converts freeform dates to Omni's N days format."""

    def test_days_ago_passthrough(self):
        from omni_dash.dashboard.serializer import _normalize_date_to_days

        left, right = _normalize_date_to_days("90 days ago")
        assert left == "90 days ago"
        assert right == "90 days"

    def test_weeks_converted_to_days(self):
        from omni_dash.dashboard.serializer import _normalize_date_to_days

        left, right = _normalize_date_to_days("last 12 weeks")
        assert left == "84 days ago"
        assert right == "84 days"

    def test_complete_weeks_converted(self):
        from omni_dash.dashboard.serializer import _normalize_date_to_days

        left, right = _normalize_date_to_days("12 complete weeks ago")
        assert left == "84 days ago"
        assert right == "84 days"

    def test_months_converted_to_days(self):
        from omni_dash.dashboard.serializer import _normalize_date_to_days

        left, right = _normalize_date_to_days("last 3 months")
        assert left == "90 days ago"
        assert right == "90 days"

    def test_bare_days_format(self):
        from omni_dash.dashboard.serializer import _normalize_date_to_days

        left, right = _normalize_date_to_days("30 days")
        assert left == "30 days ago"
        assert right == "30 days"

    def test_unparseable_defaults_to_90_days(self):
        from omni_dash.dashboard.serializer import _normalize_date_to_days

        left, right = _normalize_date_to_days("some random string")
        assert left == "90 days ago"
        assert right == "90 days"

    def test_dashboard_filter_integration(self):
        """Full round-trip: DashboardFilter with 'last 12 weeks' produces valid Omni filter."""
        from omni_dash.dashboard.definition import DashboardFilter
        from omni_dash.dashboard.serializer import _to_omni_filter_from_dashboard

        df = DashboardFilter(
            field="t.week_start",
            filter_type="date_range",
            label="Date Range",
            default_value="last 12 weeks",
        )
        result = _to_omni_filter_from_dashboard(df)
        assert result["left_side"] == "84 days ago"
        assert result["right_side"] == "84 days"
        assert result["kind"] == "TIME_FOR_INTERVAL_DURATION"


# ---------------------------------------------------------------------------
# Bug fix: is_not_null / is_null must send values=[] not null
# ---------------------------------------------------------------------------


def test_is_not_null_filter_has_empty_values_array():
    """is_not_null filter must include values=[] to avoid crashing Omni's Java backend.

    Omni's StringFilter requires a non-nullable `values` array.  Sending null
    causes: "instantiation of StringFilter value failed for JSON property values
    due to missing (therefore NULL) value for creator parameter values which is
    a non-nullable type".
    """
    from omni_dash.dashboard.definition import FilterSpec as FS, Tile, TileQuery

    definition = DashboardDefinition(
        name="IsNotNull Filter Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Non-null rows",
                chart_type="bar",
                query=TileQuery(
                    table="t",
                    fields=["t.status", "t.count"],
                    filters=[
                        FS(field="t.status", operator="is_not_null", value=None),
                    ],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    f = payload["queryPresentations"][0]["query"]["filters"]["t.status"]

    assert f["kind"] == "IS_NULL"
    assert f["is_negative"] is True
    assert f["values"] == []
    assert f["values"] is not None


def test_is_null_filter_has_empty_values_array():
    """is_null filter must include values=[] to avoid crashing Omni's Java backend.

    Same root cause as is_not_null: Omni's StringFilter requires a non-nullable
    `values` array even for null-check operators.
    """
    from omni_dash.dashboard.definition import FilterSpec as FS, Tile, TileQuery

    definition = DashboardDefinition(
        name="IsNull Filter Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Null rows",
                chart_type="bar",
                query=TileQuery(
                    table="t",
                    fields=["t.status", "t.count"],
                    filters=[
                        FS(field="t.status", operator="is_null", value=None),
                    ],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    f = payload["queryPresentations"][0]["query"]["filters"]["t.status"]

    assert f["kind"] == "IS_NULL"
    assert f["is_negative"] is False
    assert f["values"] == []
    assert f["values"] is not None


# ===========================================================================
# Phase 1: Missing Chart Types
# ===========================================================================


def test_pie_chart_serialization():
    """Pie chart should map to Omni 'pie' type with correct chartType."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Pie Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Revenue by Channel",
                chart_type="pie",
                query=TileQuery(table="t", fields=["t.channel", "t.revenue"]),
                vis_config=TileVisConfig(x_axis="t.channel", y_axis=["t.revenue"]),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    assert qp["chartType"] == "pie"
    # Pie is NOT in _CARTESIAN_CHART_TYPES, so it won't get a visConfig with spec
    # but the chartType should be set correctly
    assert qp["prefersChart"] is True
    assert "t.channel" in qp["query"]["fields"]
    assert "t.revenue" in qp["query"]["fields"]


def test_grouped_bar_chart_serialization():
    """Grouped bar chart should map to 'barGrouped' and generate a cartesian spec."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Grouped Bar Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Revenue vs Cost",
                chart_type="grouped_bar",
                query=TileQuery(table="t", fields=["t.category", "t.revenue", "t.cost"]),
                vis_config=TileVisConfig(
                    x_axis="t.category",
                    y_axis=["t.revenue", "t.cost"],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    assert qp["chartType"] == "barGrouped"
    assert qp["prefersChart"] is True

    # barGrouped IS in _CARTESIAN_CHART_TYPES so should get full cartesian spec
    vis = qp["visConfig"]
    assert vis["visType"] == "basic"
    spec = vis["spec"]
    assert spec["configType"] == "cartesian"
    assert spec["x"]["field"]["name"] == "t.category"
    assert spec["mark"]["type"] == "bar"
    # Auto-generated series: one per measure (revenue and cost)
    assert len(spec["series"]) == 2
    series_fields = [s["field"]["name"] for s in spec["series"]]
    assert "t.revenue" in series_fields
    assert "t.cost" in series_fields
    for s in spec["series"]:
        assert s["yAxis"] == "y"


def test_stacked_area_chart_serialization():
    """Stacked area chart should map to 'areaStacked' with stacking behavior."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Stacked Area Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Revenue by Source",
                chart_type="stacked_area",
                query=TileQuery(
                    table="t", fields=["t.date", "t.revenue", "t.source"]
                ),
                vis_config=TileVisConfig(
                    x_axis="t.date",
                    y_axis=["t.revenue"],
                    color_by="t.source",
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    assert qp["chartType"] == "areaStacked"

    # areaStacked IS in _CARTESIAN_CHART_TYPES
    vis = qp["visConfig"]
    assert vis["visType"] == "basic"
    spec = vis["spec"]
    assert spec["configType"] == "cartesian"
    assert spec["mark"]["type"] == "area"
    # Stacking behavior
    assert spec["behaviors"]["stackMultiMark"] is True
    assert spec["y"]["color"]["_stack"] == "stack"


def test_funnel_chart_serialization():
    """Funnel chart should fall back to 'bar' (documented limitation)."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Funnel Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Conversion Funnel",
                chart_type="funnel",
                query=TileQuery(table="t", fields=["t.stage", "t.count"]),
                vis_config=TileVisConfig(x_axis="t.stage", y_axis=["t.count"]),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    # funnel maps to "bar" in _CHART_TYPE_TO_OMNI
    assert qp["chartType"] == "bar"
    # "bar" IS in _CARTESIAN_CHART_TYPES so should get cartesian spec
    vis = qp["visConfig"]
    assert vis["visType"] == "basic"
    assert spec_mark_type(vis) == "bar"


def spec_mark_type(vis: dict) -> str:
    """Helper to extract mark type from a visConfig."""
    return vis["spec"]["mark"]["type"]


def test_donut_maps_to_pie():
    """Donut chart type should map to Omni 'pie'."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Donut Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Distribution",
                chart_type="donut",
                query=TileQuery(table="t", fields=["t.segment", "t.value"]),
                vis_config=TileVisConfig(x_axis="t.segment"),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    assert qp["chartType"] == "pie"
    assert qp["prefersChart"] is True


def test_pivot_table_serialization():
    """Pivot table with multiple dimensions and pivots should map to 'table'."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Pivot Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Revenue Pivot",
                chart_type="pivot_table",
                query=TileQuery(
                    table="t",
                    fields=["t.region", "t.quarter", "t.revenue"],
                    pivots=["t.quarter"],
                ),
                vis_config=TileVisConfig(x_axis="t.region"),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    assert qp["chartType"] == "table"
    # Pivots should be passed through to the query
    assert qp["query"]["pivots"] == ["t.quarter"]
    # table type → prefersChart is False
    assert qp["prefersChart"] is False


def test_from_export_grouped_bar():
    """Omni 'barGrouped' should reverse map to internal 'grouped_bar'."""
    export_data = {
        "document": {"name": "Test", "modelId": "m1"},
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    _make_omni_export_qp("Grouped", "barGrouped", "t", ["t.dim", "t.m1", "t.m2"]),
                ],
            },
            "metadata": {"layouts": {"lg": []}},
        },
    }
    defn = DashboardSerializer.from_omni_export(export_data)
    assert defn.tiles[0].chart_type == "grouped_bar"


def test_from_export_stacked_area():
    """Omni 'areaStacked' should reverse map to internal 'stacked_area'."""
    export_data = {
        "document": {"name": "Test", "modelId": "m1"},
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    _make_omni_export_qp("Stacked Area", "areaStacked", "t", ["t.date", "t.val"]),
                ],
            },
            "metadata": {"layouts": {"lg": []}},
        },
    }
    defn = DashboardSerializer.from_omni_export(export_data)
    assert defn.tiles[0].chart_type == "stacked_area"


def test_from_export_pie():
    """Omni 'pie' should reverse map to internal 'pie'."""
    export_data = {
        "document": {"name": "Test", "modelId": "m1"},
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    _make_omni_export_qp("Pie Chart", "pie", "t", ["t.category", "t.amount"]),
                ],
            },
            "metadata": {"layouts": {"lg": []}},
        },
    }
    defn = DashboardSerializer.from_omni_export(export_data)
    assert defn.tiles[0].chart_type == "pie"


# ===========================================================================
# Phase 3a: Serializer Edge Cases
# ===========================================================================


def test_kpi_with_only_date_fields():
    """KPI tile with only date fields should fall back to fields[0]."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="KPI Date Only",
        model_id="m-1",
        tiles=[
            Tile(
                name="Latest Date",
                chart_type="number",
                query=TileQuery(
                    table="t",
                    fields=["t.created_at", "t.updated_at"],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    vis = payload["queryPresentations"][0]["visConfig"]
    kpi_field = vis["spec"]["markdownConfig"][0]["config"]["field"]["field"]["name"]
    # Both fields are date-like — heuristic should still pick something (falls back to first)
    assert kpi_field == "t.created_at"


def test_empty_fields_list_raises_error():
    """Tile with empty fields list should raise validation error."""
    from omni_dash.dashboard.definition import Tile, TileQuery

    with pytest.raises(Exception):
        TileQuery(table="t", fields=[])


def test_extremely_long_field_names():
    """Fields with 200+ character names should serialize without truncation."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    long_name = "t." + "a" * 200
    definition = DashboardDefinition(
        name="Long Field Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Long Names",
                chart_type="line",
                query=TileQuery(table="t", fields=[long_name, "t.value"]),
                vis_config=TileVisConfig(x_axis=long_name, y_axis=["t.value"]),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    assert long_name in qp["query"]["fields"]
    # Cartesian spec should also have the long field name without truncation
    spec = qp["visConfig"]["spec"]
    assert spec["x"]["field"]["name"] == long_name
    assert len(spec["x"]["field"]["name"]) == 202  # "t." + 200 chars


def test_duplicate_fields_in_query():
    """Duplicate fields in query should be preserved (serializer does not deduplicate)."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Duplicate Fields Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Duplicated",
                chart_type="line",
                query=TileQuery(
                    table="t",
                    fields=["t.date", "t.revenue", "t.revenue"],
                ),
                vis_config=TileVisConfig(x_axis="t.date", y_axis=["t.revenue"]),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    # The fields list should be preserved as-is (duplicates included)
    assert qp["query"]["fields"].count("t.revenue") == 2
    # Series should be generated for each non-x field including duplicates
    series = qp["visConfig"]["spec"]["series"]
    series_fields = [s["field"]["name"] for s in series]
    assert series_fields.count("t.revenue") == 2


def test_unknown_chart_type_passthrough():
    """Chart type not in _CHART_TYPE_TO_OMNI should pass through as-is.

    Note: The Tile validator restricts chart_type to the ChartType enum,
    so truly unknown types won't pass validation. But the serializer's
    _CHART_TYPE_TO_OMNI.get(chart_type, chart_type) logic is what we test
    via the mapping itself.
    """
    from omni_dash.dashboard.serializer import _CHART_TYPE_TO_OMNI

    # Verify that all ChartType enum values are covered in the mapping
    from omni_dash.dashboard.definition import ChartType

    for ct in ChartType:
        assert ct.value in _CHART_TYPE_TO_OMNI, (
            f"ChartType.{ct.name} ('{ct.value}') is not in _CHART_TYPE_TO_OMNI"
        )


def test_filter_operator_fallback():
    """Unknown filter operator should fall back to EQUALS."""
    from omni_dash.dashboard.definition import FilterSpec as FS, Tile, TileQuery

    definition = DashboardDefinition(
        name="Unknown Op Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Filtered",
                chart_type="bar",
                query=TileQuery(
                    table="t",
                    fields=["t.dim", "t.val"],
                    filters=[
                        FS(field="t.status", operator="some_unknown_operator", value="test"),
                    ],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    f = payload["queryPresentations"][0]["query"]["filters"]["t.status"]
    # Unknown operator falls back to EQUALS
    assert f["kind"] == "EQUALS"
    assert f["values"] == ["test"]
    assert f["is_negative"] is False


# ===========================================================================
# Phase 3c: Filter Edge Cases
# ===========================================================================


def test_is_not_null_with_explicit_empty_values():
    """is_not_null with explicit values=[] should produce correct payload (regression)."""
    from omni_dash.dashboard.definition import FilterSpec as FS, Tile, TileQuery

    definition = DashboardDefinition(
        name="IsNotNull Empty Values",
        model_id="m-1",
        tiles=[
            Tile(
                name="Filtered",
                chart_type="bar",
                query=TileQuery(
                    table="t",
                    fields=["t.email", "t.count"],
                    filters=[
                        FS(field="t.email", operator="is_not_null", value=None),
                    ],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    f = payload["queryPresentations"][0]["query"]["filters"]["t.email"]
    assert f["kind"] == "IS_NULL"
    assert f["is_negative"] is True
    assert f["values"] == []
    # Must be an empty list, not None — Omni's Java backend crashes on null
    assert isinstance(f["values"], list)


def test_date_range_last_12_weeks_normalized():
    """'last 12 weeks' should be normalized to 84 days in a full serialization round-trip."""
    from omni_dash.dashboard.definition import DashboardFilter, Tile, TileQuery

    definition = DashboardDefinition(
        name="12 Weeks Normalized",
        model_id="m-1",
        tiles=[
            Tile(
                name="Trend",
                chart_type="line",
                query=TileQuery(
                    table="t", fields=["t.week_start", "t.visits"]
                ),
                vis_config=TileVisConfig(x_axis="t.week_start"),
            ),
        ],
        filters=[
            DashboardFilter(
                field="t.week_start",
                filter_type="date_range",
                label="Date",
                default_value="last 12 weeks",
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    tile_filter = payload["queryPresentations"][0]["query"]["filters"]["t.week_start"]
    assert tile_filter["left_side"] == "84 days ago"
    assert tile_filter["right_side"] == "84 days"
    assert tile_filter["kind"] == "TIME_FOR_INTERVAL_DURATION"

    # Also verify the filterConfig gets the same normalization
    fid = payload["filterOrder"][0]
    fc = payload["filterConfig"][fid]
    assert fc["kind"] == "TIME_FOR_INTERVAL_DURATION"


def test_dashboard_filter_with_none_default_skipped():
    """Dashboard filter with default_value=None should NOT be applied to tiles."""
    from omni_dash.dashboard.definition import DashboardFilter, Tile, TileQuery

    definition = DashboardDefinition(
        name="None Default Filter",
        model_id="m-1",
        tiles=[
            Tile(
                name="Chart",
                chart_type="line",
                query=TileQuery(
                    table="t", fields=["t.date", "t.revenue"]
                ),
                vis_config=TileVisConfig(x_axis="t.date"),
            ),
        ],
        filters=[
            DashboardFilter(
                field="t.date",
                filter_type="date_range",
                label="Date",
                default_value=None,
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    # With default_value=None, the filter should be skipped during tile propagation
    tile_filters = qp["query"].get("filters", {})
    assert "t.date" not in tile_filters


def test_composite_filter_with_three_conditions():
    """Composite filter with 3 AND conditions should produce correct nesting."""
    from omni_dash.dashboard.definition import (
        CompositeFilter, FilterSpec as FS, Tile, TileQuery,
    )

    definition = DashboardDefinition(
        name="Triple Composite",
        model_id="m-1",
        tiles=[
            Tile(
                name="Filtered",
                chart_type="bar",
                query=TileQuery(
                    table="t",
                    fields=["t.date", "t.value"],
                    composite_filters=[
                        CompositeFilter(
                            conditions=[
                                FS(field="t.date", operator="date_range", value="90 days ago"),
                                FS(field="t.date", operator="before", value="7 days ago"),
                                FS(field="t.date", operator="is_not_null", value=None),
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
    composite = filters["t.date"]
    assert composite["type"] == "composite"
    assert composite["conjunction"] == "AND"
    assert len(composite["filters"]) == 3
    # Verify each sub-filter kind
    kinds = [f["kind"] for f in composite["filters"]]
    assert kinds[0] == "TIME_FOR_INTERVAL_DURATION"
    assert kinds[1] == "BEFORE"
    assert kinds[2] == "IS_NULL"


# ===========================================================================
# Phase 4a: 16-Tile Stress Test
# ===========================================================================


def test_stress_16_tile_dashboard():
    """Build a 16-tile dashboard programmatically and verify all tiles serialize."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    tiles = []

    # 4 KPI tiles (quarter size)
    for i in range(4):
        tiles.append(
            Tile(
                name=f"KPI {i+1}",
                chart_type="number",
                query=TileQuery(table="t", fields=[f"t.metric_{i}"]),
                size="quarter",
            )
        )

    # 3 line charts (half size)
    for i in range(3):
        tiles.append(
            Tile(
                name=f"Line Chart {i+1}",
                chart_type="line",
                query=TileQuery(table="t", fields=["t.date", f"t.measure_{i}"]),
                vis_config=TileVisConfig(x_axis="t.date", y_axis=[f"t.measure_{i}"]),
                size="half",
            )
        )

    # 2 stacked bar charts (half size)
    for i in range(2):
        tiles.append(
            Tile(
                name=f"Stacked Bar {i+1}",
                chart_type="stacked_bar",
                query=TileQuery(
                    table="t", fields=["t.month", f"t.revenue_{i}", "t.channel"]
                ),
                vis_config=TileVisConfig(
                    x_axis="t.month",
                    y_axis=[f"t.revenue_{i}"],
                    color_by="t.channel",
                ),
                size="half",
            )
        )

    # 2 bar charts (third size)
    for i in range(2):
        tiles.append(
            Tile(
                name=f"Bar Chart {i+1}",
                chart_type="bar",
                query=TileQuery(table="t", fields=["t.category", f"t.count_{i}"]),
                vis_config=TileVisConfig(
                    x_axis="t.category", y_axis=[f"t.count_{i}"]
                ),
                size="third",
            )
        )

    # 1 scatter plot (half size)
    tiles.append(
        Tile(
            name="Scatter",
            chart_type="scatter",
            query=TileQuery(table="t", fields=["t.x_val", "t.y_val"]),
            vis_config=TileVisConfig(x_axis="t.x_val", y_axis=["t.y_val"]),
            size="half",
        )
    )

    # 1 heatmap (half size)
    tiles.append(
        Tile(
            name="Heatmap",
            chart_type="heatmap",
            query=TileQuery(table="t", fields=["t.day", "t.hour", "t.intensity"]),
            vis_config=TileVisConfig(
                x_axis="t.day", y_axis=["t.hour"], heatmap_color_field="t.intensity"
            ),
            size="half",
        )
    )

    # 1 combo chart (full size)
    tiles.append(
        Tile(
            name="Combo",
            chart_type="combo",
            query=TileQuery(table="t", fields=["t.date", "t.revenue", "t.growth"]),
            vis_config=TileVisConfig(
                x_axis="t.date",
                y2_axis=True,
                series_config=[
                    {"field": "t.revenue", "mark_type": "bar", "y_axis": "y"},
                    {"field": "t.growth", "mark_type": "line", "y_axis": "y2"},
                ],
            ),
            size="full",
        )
    )

    # 1 table (full size)
    tiles.append(
        Tile(
            name="Detail Table",
            chart_type="table",
            query=TileQuery(
                table="t", fields=["t.id", "t.name", "t.status", "t.value"]
            ),
            size="full",
        )
    )

    # 1 text/markdown tile (full size)
    tiles.append(
        Tile(
            name="Header",
            chart_type="text",
            query=TileQuery(table="t", fields=["t.id"]),
            vis_config=TileVisConfig(
                markdown_template="<h1>Dashboard Overview</h1>"
            ),
            size="full",
        )
    )

    assert len(tiles) == 16

    definition = DashboardDefinition(
        name="Stress Test Dashboard",
        model_id="m-1",
        tiles=tiles,
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)

    # All 16 tiles should serialize
    assert len(payload["queryPresentations"]) == 16

    # Verify chart types are correct
    chart_types = [qp["chartType"] for qp in payload["queryPresentations"]]
    assert chart_types.count("kpi") == 4
    assert chart_types.count("line") == 3
    assert chart_types.count("barStacked") == 2
    assert chart_types.count("bar") == 2
    assert chart_types.count("point") == 1
    assert chart_types.count("heatmap") == 1
    assert chart_types.count("barLine") == 1
    assert chart_types.count("table") == 1
    assert chart_types.count("markdown") == 1

    # Verify no crashes — all queryPresentations have required fields
    for qp in payload["queryPresentations"]:
        assert "name" in qp
        assert "chartType" in qp
        assert "query" in qp
        assert "fields" in qp["query"]
        assert "table" in qp["query"]
        assert "modelId" in qp["query"]


# ===========================================================================
# Phase 4b: Multi-Table Dashboard Filter Propagation
# ===========================================================================


def test_multi_table_dashboard_filter_propagation():
    """Dashboard filter on table_a.date should remap to table_b.date but skip table_c."""
    from omni_dash.dashboard.definition import DashboardFilter, Tile, TileQuery

    definition = DashboardDefinition(
        name="Multi-Table Filter",
        model_id="m-1",
        tiles=[
            # table_a: has "date" column — direct match
            Tile(
                name="Table A Chart",
                chart_type="line",
                query=TileQuery(
                    table="table_a", fields=["table_a.date", "table_a.visits"]
                ),
                vis_config=TileVisConfig(x_axis="table_a.date"),
            ),
            # table_b: has "date" column — cross-table remap
            Tile(
                name="Table B Chart",
                chart_type="bar",
                query=TileQuery(
                    table="table_b", fields=["table_b.date", "table_b.signups"]
                ),
                vis_config=TileVisConfig(x_axis="table_b.date"),
            ),
            # table_c: does NOT have "date" column — filter should NOT apply
            Tile(
                name="Table C Chart",
                chart_type="bar",
                query=TileQuery(
                    table="table_c", fields=["table_c.url", "table_c.clicks"]
                ),
                vis_config=TileVisConfig(x_axis="table_c.url"),
            ),
        ],
        filters=[
            DashboardFilter(
                field="table_a.date",
                filter_type="date_range",
                label="Date",
                default_value="last 12 weeks",
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)

    # table_a: direct match — filter applied as table_a.date
    filters_a = payload["queryPresentations"][0]["query"]["filters"]
    assert "table_a.date" in filters_a

    # table_b: cross-table remap — filter applied as table_b.date
    filters_b = payload["queryPresentations"][1]["query"]["filters"]
    assert "table_b.date" in filters_b
    assert "table_a.date" not in filters_b

    # table_c: no matching column — filter NOT applied
    qp_c = payload["queryPresentations"][2]
    filters_c = qp_c["query"].get("filters", {})
    assert "table_a.date" not in filters_c
    assert "table_c.date" not in filters_c
    assert len(filters_c) == 0


# ===========================================================================
# Phase 4c: Builder -> Serializer Round-Trip
# ===========================================================================


def test_builder_serializer_round_trip():
    """Build a complex dashboard with DashboardBuilder, serialize it, and verify structure."""
    definition = (
        DashboardBuilder("Round Trip Test")
        .model("model-rt-123")
        .dbt_source("mart_seo_weekly_funnel")
        .add_line_chart(
            "Organic Visits",
            time_col="week_start",
            metric_cols=["organic_visits_total", "organic_sessions"],
        )
        .add_bar_chart(
            "Signups by Channel",
            dimension_col="channel",
            metric_cols=["signups"],
        )
        .add_number_tile("Current ARR", metric_col="running_plg_arr", value_format="USDCURRENCY_0")
        .add_table("Detail Data", columns=["week_start", "channel", "signups", "organic_visits_total"])
        .add_filter("week_start", filter_type="date_range", label="Date Range", default="last 12 weeks")
        .auto_layout()
        .build()
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)

    # Top-level structure
    assert payload["modelId"] == "model-rt-123"
    assert payload["name"] == "Round Trip Test"
    assert len(payload["queryPresentations"]) == 4

    # All fields should be fully qualified (table.column)
    for qp in payload["queryPresentations"]:
        for field in qp["query"]["fields"]:
            assert "." in field, f"Field '{field}' is not qualified"
        # All modelIds should match
        assert qp["query"]["modelId"] == "model-rt-123"

    # Line chart
    qp_line = payload["queryPresentations"][0]
    assert qp_line["chartType"] == "line"
    assert qp_line["prefersChart"] is True
    assert "mart_seo_weekly_funnel.week_start" in qp_line["query"]["fields"]
    assert "mart_seo_weekly_funnel.organic_visits_total" in qp_line["query"]["fields"]
    # Line charts should have sorts on the time column
    if qp_line["query"]["sorts"]:
        assert qp_line["query"]["sorts"][0]["column_name"] == "mart_seo_weekly_funnel.week_start"
    # Should have a cartesian visConfig
    assert qp_line["visConfig"]["visType"] == "basic"
    assert qp_line["visConfig"]["spec"]["configType"] == "cartesian"

    # Bar chart
    qp_bar = payload["queryPresentations"][1]
    assert qp_bar["chartType"] == "bar"

    # KPI
    qp_kpi = payload["queryPresentations"][2]
    assert qp_kpi["chartType"] == "kpi"
    assert qp_kpi["query"]["limit"] == 1
    assert qp_kpi["visConfig"]["visType"] == "omni-kpi"

    # Table
    qp_table = payload["queryPresentations"][3]
    assert qp_table["chartType"] == "table"
    assert qp_table["prefersChart"] is False
    assert qp_table["visConfig"]["visType"] == "omni-table"

    # Filters should be propagated to all tiles with matching date column
    for qp in payload["queryPresentations"]:
        tile_table = qp["query"]["table"]
        tile_fields = qp["query"]["fields"]
        col_names = {f.split(".")[-1] for f in tile_fields}
        if "week_start" in col_names:
            tile_filters = qp["query"].get("filters", {})
            assert f"{tile_table}.week_start" in tile_filters

    # Filter config should be present at top level
    assert "filterConfig" in payload
    assert "filterOrder" in payload
    assert len(payload["filterOrder"]) == 1

    # Layout positions should be present (from auto_layout)
    for qp in payload["queryPresentations"]:
        assert "position" in qp
        pos = qp["position"]
        assert "x" in pos
        assert "y" in pos
        assert "w" in pos
        assert "h" in pos


# ===========================================================================
# Additional edge case: from_export for column variants
# ===========================================================================


def test_from_export_column_stacked_maps_to_stacked_bar():
    """Omni 'columnStacked' should reverse map to internal 'stacked_bar'."""
    export_data = {
        "document": {"name": "Test", "modelId": "m1"},
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    _make_omni_export_qp("Col Stacked", "columnStacked", "t", ["t.a", "t.b"]),
                ],
            },
            "metadata": {"layouts": {"lg": []}},
        },
    }
    defn = DashboardSerializer.from_omni_export(export_data)
    assert defn.tiles[0].chart_type == "stacked_bar"


def test_from_export_column_grouped_maps_to_grouped_bar():
    """Omni 'columnGrouped' should reverse map to internal 'grouped_bar'."""
    export_data = {
        "document": {"name": "Test", "modelId": "m1"},
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    _make_omni_export_qp("Col Grouped", "columnGrouped", "t", ["t.a", "t.b"]),
                ],
            },
            "metadata": {"layouts": {"lg": []}},
        },
    }
    defn = DashboardSerializer.from_omni_export(export_data)
    assert defn.tiles[0].chart_type == "grouped_bar"


def test_from_export_markdown_maps_to_text():
    """Omni 'markdown' should reverse map to internal 'text'."""
    export_data = {
        "document": {"name": "Test", "modelId": "m1"},
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    _make_omni_export_qp("Header", "markdown", "t", ["t.a"]),
                ],
            },
            "metadata": {"layouts": {"lg": []}},
        },
    }
    defn = DashboardSerializer.from_omni_export(export_data)
    assert defn.tiles[0].chart_type == "text"


# ===========================================================================
# Additional coverage: more edge cases
# ===========================================================================


def test_pie_chart_no_visconfig_spec():
    """Pie chart should NOT get a cartesian or heatmap visConfig (not in those sets).

    Pie is in _CHART_TYPE_TO_OMNI but not in _CARTESIAN_CHART_TYPES, so the
    serializer has no branch to generate a visConfig for it. Verify this is the case.
    """
    from omni_dash.dashboard.definition import Tile, TileQuery

    definition = DashboardDefinition(
        name="Pie No Spec",
        model_id="m-1",
        tiles=[
            Tile(
                name="Pie",
                chart_type="pie",
                query=TileQuery(table="t", fields=["t.label", "t.amount"]),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    assert qp["chartType"] == "pie"
    # Pie doesn't match any vis-building branch, so no visConfig should be set
    assert "visConfig" not in qp


def test_grouped_bar_not_stacked():
    """Grouped bar should NOT have stackMultiMark=True (unlike stacked bar)."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Grouped Not Stacked",
        model_id="m-1",
        tiles=[
            Tile(
                name="Grouped",
                chart_type="grouped_bar",
                query=TileQuery(table="t", fields=["t.dim", "t.m1", "t.m2"]),
                vis_config=TileVisConfig(x_axis="t.dim", y_axis=["t.m1", "t.m2"]),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    spec = payload["queryPresentations"][0]["visConfig"]["spec"]
    # Grouped bar is NOT stacked
    assert spec["behaviors"]["stackMultiMark"] is False
    # Y axis should NOT have color._stack
    y_config = spec.get("y", {})
    assert "color" not in y_config or "_stack" not in y_config.get("color", {})


def test_multiple_dashboard_filters():
    """Dashboard with multiple filters should produce correct filterConfig and filterOrder."""
    from omni_dash.dashboard.definition import DashboardFilter, Tile, TileQuery

    definition = DashboardDefinition(
        name="Multi Filter",
        model_id="m-1",
        tiles=[
            Tile(
                name="Chart",
                chart_type="line",
                query=TileQuery(
                    table="t", fields=["t.date", "t.channel", "t.revenue"]
                ),
                vis_config=TileVisConfig(x_axis="t.date"),
            ),
        ],
        filters=[
            DashboardFilter(
                field="t.date",
                filter_type="date_range",
                label="Date",
                default_value="last 12 weeks",
            ),
            DashboardFilter(
                field="t.channel",
                filter_type="select",
                label="Channel",
                default_value="organic",
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)

    assert len(payload["filterOrder"]) == 2
    assert len(payload["filterConfig"]) == 2

    # Both filters should be applied to the tile
    tile_filters = payload["queryPresentations"][0]["query"]["filters"]
    assert "t.date" in tile_filters
    assert "t.channel" in tile_filters
    assert tile_filters["t.date"]["kind"] == "TIME_FOR_INTERVAL_DURATION"
    assert tile_filters["t.channel"]["kind"] == "EQUALS"
    assert tile_filters["t.channel"]["values"] == ["organic"]


def test_scatter_chart_maps_to_point():
    """Scatter chart should map to Omni 'point' type with cartesian spec."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Scatter Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Correlation",
                chart_type="scatter",
                query=TileQuery(table="t", fields=["t.x_val", "t.y_val"]),
                vis_config=TileVisConfig(x_axis="t.x_val", y_axis=["t.y_val"]),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    assert qp["chartType"] == "point"
    # point IS in _CARTESIAN_CHART_TYPES
    vis = qp["visConfig"]
    assert vis["visType"] == "basic"
    assert vis["chartType"] == "point"
    spec = vis["spec"]
    assert spec["mark"]["type"] == "point"
    assert spec["x"]["field"]["name"] == "t.x_val"
    assert len(spec["series"]) == 1
    assert spec["series"][0]["field"]["name"] == "t.y_val"


def test_combo_chart_generates_dual_series_types():
    """Combo chart should map to 'barLine' with mixed mark types in series."""
    from omni_dash.dashboard.definition import Tile, TileQuery, TileVisConfig

    definition = DashboardDefinition(
        name="Combo Test",
        model_id="m-1",
        tiles=[
            Tile(
                name="Revenue & Growth",
                chart_type="combo",
                query=TileQuery(
                    table="t", fields=["t.month", "t.revenue", "t.growth_pct"]
                ),
                vis_config=TileVisConfig(
                    x_axis="t.month",
                    y2_axis=True,
                    series_config=[
                        {"field": "t.revenue", "mark_type": "bar", "y_axis": "y"},
                        {"field": "t.growth_pct", "mark_type": "line", "y_axis": "y2"},
                    ],
                ),
            ),
        ],
    )
    payload = DashboardSerializer.to_omni_create_payload(definition)
    qp = payload["queryPresentations"][0]
    assert qp["chartType"] == "barLine"

    vis = qp["visConfig"]
    spec = vis["spec"]
    assert spec["configType"] == "cartesian"
    assert "y2" in spec
    assert len(spec["series"]) == 2
    assert spec["series"][0]["mark"]["type"] == "bar"
    assert spec["series"][0]["yAxis"] == "y"
    assert spec["series"][1]["mark"]["type"] == "line"
    assert spec["series"][1]["yAxis"] == "y2"


# ===========================================================================
# Bug #3: SQL tile round-trip
# ===========================================================================


class TestSQLTileFromExport:
    """Test from_omni_export extracts SQL tile fields correctly."""

    def _make_sql_export(self, *, is_sql=True, user_sql="SELECT * FROM t",
                         sql_in_query_json=False):
        """Build a minimal Omni export with SQL tile fields."""
        query_json = {
            "table": "t",
            "fields": ["t.id"],
        }
        if sql_in_query_json:
            query_json["userEditedSQL"] = user_sql

        raw_query = {"queryJson": query_json}
        if not sql_in_query_json and user_sql:
            raw_query["userEditedSQL"] = user_sql

        return {
            "document": {"name": "Test", "modelId": "m"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "SQL Tile",
                                "isSql": is_sql,
                                "query": raw_query,
                                "visConfig": {
                                    "visType": "omni-table",
                                    "chartType": "table",
                                },
                            }
                        }
                    ]
                },
                "metadata": {
                    "layouts": {
                        "lg": [
                            {"i": 1, "x": 0, "y": 0, "w": 24, "h": 40}
                        ]
                    }
                },
            },
            "workbookModel": {},
        }

    def test_extracts_is_sql_from_export(self):
        """from_omni_export should extract isSql and userEditedSQL from the export."""
        export_data = self._make_sql_export(
            is_sql=True, user_sql="SELECT * FROM t"
        )
        defn = DashboardSerializer.from_omni_export(export_data)

        assert len(defn.tiles) == 1
        assert defn.tiles[0].query.is_sql is True
        assert defn.tiles[0].query.user_sql == "SELECT * FROM t"

    def test_non_sql_tile_defaults(self):
        """Export without isSql should default to is_sql=False and user_sql=None."""
        export_data = {
            "document": {"name": "Test", "modelId": "m"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Regular Tile",
                                "query": {
                                    "queryJson": {
                                        "table": "t",
                                        "fields": ["t.id"],
                                    },
                                },
                                "visConfig": {
                                    "visType": "omni-table",
                                    "chartType": "table",
                                },
                            }
                        }
                    ]
                },
                "metadata": {"layouts": {"lg": []}},
            },
        }
        defn = DashboardSerializer.from_omni_export(export_data)

        assert defn.tiles[0].query.is_sql is False
        assert defn.tiles[0].query.user_sql is None

    def test_user_sql_in_query_json(self):
        """userEditedSQL inside queryJson (not at query root) should still be extracted."""
        export_data = self._make_sql_export(
            is_sql=True, user_sql="SELECT 1 AS id", sql_in_query_json=True
        )
        defn = DashboardSerializer.from_omni_export(export_data)

        assert defn.tiles[0].query.is_sql is True
        assert defn.tiles[0].query.user_sql == "SELECT 1 AS id"


class TestSQLTileYAMLRoundTrip:
    """Test YAML serialization/deserialization preserves SQL tile fields."""

    def test_yaml_round_trip_preserves_sql(self):
        """SQL tile (is_sql=True, user_sql set) should survive a YAML round-trip."""
        from omni_dash.dashboard.definition import Tile, TileQuery

        original = DashboardDefinition(
            name="SQL YAML Test",
            model_id="m-1",
            tiles=[
                Tile(
                    name="Custom SQL",
                    chart_type="table",
                    query=TileQuery(
                        table="t",
                        fields=["t.id"],
                        is_sql=True,
                        user_sql="SELECT * FROM t",
                    ),
                ),
            ],
        )
        yaml_str = DashboardSerializer.to_yaml(original)
        restored = DashboardSerializer.from_yaml(yaml_str)

        assert restored.tiles[0].query.is_sql is True
        assert restored.tiles[0].query.user_sql == "SELECT * FROM t"

    def test_non_sql_tile_yaml_round_trip(self):
        """Regular tile (is_sql=False) should stay False/None after YAML round-trip."""
        from omni_dash.dashboard.definition import Tile, TileQuery

        original = DashboardDefinition(
            name="Non-SQL YAML Test",
            model_id="m-1",
            tiles=[
                Tile(
                    name="Regular",
                    chart_type="line",
                    query=TileQuery(
                        table="t",
                        fields=["t.date", "t.value"],
                    ),
                    vis_config=TileVisConfig(x_axis="t.date"),
                ),
            ],
        )
        yaml_str = DashboardSerializer.to_yaml(original)
        restored = DashboardSerializer.from_yaml(yaml_str)

        assert restored.tiles[0].query.is_sql is False
        assert restored.tiles[0].query.user_sql is None


class TestSQLTileSerializePayload:
    """Test to_omni_create_payload produces correct SQL fields."""

    def test_sql_tile_creates_omni_payload(self):
        """SQL tile should produce isSql and userEditedSQL in the Omni payload."""
        from omni_dash.dashboard.definition import Tile, TileQuery

        definition = DashboardDefinition(
            name="SQL Payload Test",
            model_id="m-1",
            tiles=[
                Tile(
                    name="Custom SQL",
                    chart_type="table",
                    query=TileQuery(
                        table="t",
                        fields=["t.id"],
                        is_sql=True,
                        user_sql="SELECT 1",
                    ),
                ),
            ],
        )
        payload = DashboardSerializer.to_omni_create_payload(definition)
        qp = payload["queryPresentations"][0]

        assert qp["isSql"] is True
        assert qp["query"]["userEditedSQL"] == "SELECT 1"
