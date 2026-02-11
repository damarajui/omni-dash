"""Serialize DashboardDefinition to/from Omni API payloads and YAML files."""

from __future__ import annotations

import hashlib
from typing import Any

import yaml

from omni_dash.dashboard.definition import (
    CalculatedField,
    CompositeFilter,
    DashboardDefinition,
    DashboardFilter,
    FilterSpec,
    SortSpec,
    TextTile,
    Tile,
    TilePosition,
    TileQuery,
    TileVisConfig,
)
from omni_dash.exceptions import DashboardDefinitionError


# Map internal chart type names to Omni API chart type names.
# Omni's valid types: auto, area, areaStacked, areaStackedPercentage, bar, barLine,
# barGrouped, barStacked, barStackedPercentage, boxplot, code, column, columnGrouped,
# columnStacked, columnStackedPercentage, heatmap, kpi, line, lineColor, map,
# regionMap, markdown, pie, sankey, point, pointColor, pointSize, pointSizeColor,
# singleRecord, summaryValue, table
_CHART_TYPE_TO_OMNI: dict[str, str] = {
    "line": "line",
    "bar": "bar",
    "area": "area",
    "scatter": "point",
    "pie": "pie",
    "donut": "pie",
    "table": "table",
    "number": "kpi",
    "funnel": "bar",
    "heatmap": "heatmap",
    "stacked_bar": "barStacked",
    "stacked_area": "areaStacked",
    "grouped_bar": "barGrouped",
    "combo": "barLine",
    "pivot_table": "table",
    "text": "markdown",
    "vegalite": "code",
}

# Reverse mapping: Omni API chart types → internal chart types.
_OMNI_TO_CHART_TYPE: dict[str, str] = {
    "line": "line",
    "lineColor": "line",
    "bar": "bar",
    "barStacked": "stacked_bar",
    "barStackedPercentage": "stacked_bar",
    "barGrouped": "grouped_bar",
    "barLine": "combo",
    "area": "area",
    "areaStacked": "stacked_area",
    "areaStackedPercentage": "stacked_area",
    "point": "scatter",
    "pointColor": "scatter",
    "pointSize": "scatter",
    "pointSizeColor": "scatter",
    "pie": "pie",
    "table": "table",
    "kpi": "number",
    "summaryValue": "number",
    "heatmap": "heatmap",
    "markdown": "text",
    "column": "bar",
    "columnStacked": "stacked_bar",
    "columnStackedPercentage": "stacked_bar",
    "columnGrouped": "grouped_bar",
    "sankey": "funnel",
    "singleRecord": "table",
    "code": "vegalite",
}


def _to_omni_filter(f: FilterSpec) -> dict[str, Any]:
    """Convert an internal FilterSpec to Omni's filter format.

    Omni filters use ``{kind, type, values/right_side, ...}`` instead of
    our simple ``{operator, value}``.  Maps common operators to Omni
    equivalents learned from real working dashboards.
    """
    op = f.operator.lower() if f.operator else "is"
    value = f.value

    # Map common operator names to Omni's kind/type system
    if op in ("date_range", "between", "date_between"):
        val_str = str(value) if value else "12 complete weeks ago"
        # Extract the interval portion: "12 complete weeks ago" → "12 complete weeks"
        interval = val_str.replace("ago", "").strip() if "ago" in val_str else val_str
        return {
            "kind": "TIME_FOR_INTERVAL_DURATION",
            "type": "date",
            "ui_type": "PAST",
            "isFiscal": False,
            "left_side": val_str,
            "right_side": interval,
            "is_negative": False,
        }
    elif op in ("before", "date_before"):
        return {
            "kind": "BEFORE",
            "type": "date",
            "ui_type": "BEFORE",
            "isFiscal": False,
            "right_side": str(value) if value else "1 weeks ago",
            "is_negative": False,
        }
    elif op in ("past", "last", "date_past"):
        return {
            "kind": "TIME_FOR_INTERVAL_DURATION",
            "type": "date",
            "ui_type": "PAST",
            "isFiscal": False,
            "left_side": str(value) if value else "12 complete weeks ago",
            "right_side": str(value).replace("ago", "").strip() if value else "12 weeks",
            "is_negative": False,
        }
    elif op in ("is", "equals", "="):
        if isinstance(value, list):
            return {
                "kind": "EQUALS",
                "type": "string",
                "values": [str(v) for v in value],
                "is_negative": False,
                "appliedLabels": {},
            }
        return {
            "kind": "EQUALS",
            "type": "string",
            "values": [str(value)] if value is not None else [],
            "is_negative": False,
            "appliedLabels": {},
        }
    elif op in ("is_not", "not_equals", "!="):
        if isinstance(value, list):
            return {
                "kind": "EQUALS",
                "type": "string",
                "values": [str(v) for v in value],
                "is_negative": True,
                "appliedLabels": {},
            }
        return {
            "kind": "EQUALS",
            "type": "string",
            "values": [str(value)] if value is not None else [],
            "is_negative": True,
            "appliedLabels": {},
        }
    elif op in ("gt", ">", "greater_than"):
        return {
            "kind": "GREATER_THAN",
            "type": "number",
            "right_side": str(value) if value is not None else "0",
            "is_negative": False,
        }
    elif op in ("lt", "<", "less_than"):
        return {
            "kind": "LESS_THAN",
            "type": "number",
            "right_side": str(value) if value is not None else "0",
            "is_negative": False,
        }
    elif op in ("contains", "like"):
        return {
            "kind": "CONTAINS",
            "type": "string",
            "right_side": str(value) if value is not None else "",
            "is_negative": False,
        }
    elif op in ("is_null", "null"):
        return {
            "kind": "IS_NULL",
            "type": "string",
            "is_negative": False,
        }
    elif op in ("is_not_null", "not_null"):
        return {
            "kind": "IS_NULL",
            "type": "string",
            "is_negative": True,
        }
    else:
        # Fallback: pass through as equals
        return {
            "kind": "EQUALS",
            "type": "string",
            "values": [str(value)] if value is not None else [],
            "is_negative": False,
        }


def _to_omni_filter_from_dashboard(dash_filter: DashboardFilter) -> dict[str, Any]:
    """Convert a DashboardFilter to Omni's per-tile query filter format.

    DashboardFilter uses ``filter_type`` (date_range, select, etc.) instead of
    the operator-based ``FilterSpec``.
    """
    ft = dash_filter.filter_type
    value = dash_filter.default_value

    if ft == "date_range":
        # Handle dict values with left/right keys (e.g., {"left": "90 days ago", "right": "90 days"})
        if isinstance(value, dict):
            left = value.get("left", "30 days ago")
            right = value.get("right", "30 days")
        else:
            left = str(value) if value else "30 days ago"
            right = left.replace("ago", "").strip() if "ago" in left else left
        return {
            "kind": "TIME_FOR_INTERVAL_DURATION",
            "type": "date",
            "ui_type": "PAST",
            "isFiscal": False,
            "left_side": left,
            "right_side": right,
            "is_negative": False,
        }
    elif ft == "select":
        return {
            "kind": "EQUALS",
            "type": "string",
            "values": [str(value)] if value else [],
            "is_negative": False,
            "appliedLabels": {},
        }
    elif ft == "multi_select":
        vals = value if isinstance(value, list) else ([str(value)] if value else [])
        return {
            "kind": "EQUALS",
            "type": "string",
            "values": [str(v) for v in vals],
            "is_negative": False,
            "appliedLabels": {},
        }
    elif ft == "number_range":
        return {
            "kind": "BETWEEN",
            "type": "number",
            "left_side": str(value) if value else "0",
            "right_side": "999999999",
            "is_negative": False,
        }
    elif ft == "text":
        return {
            "kind": "CONTAINS",
            "type": "string",
            "right_side": str(value) if value else "",
            "is_negative": False,
        }
    else:
        return {
            "kind": "EQUALS",
            "type": "string",
            "values": [str(value)] if value else [],
            "is_negative": False,
        }


# -- Cartesian chart types that use _build_cartesian_spec --
_CARTESIAN_CHART_TYPES = frozenset({
    "line", "lineColor", "bar", "barStacked", "barGrouped",
    "barLine", "area", "areaStacked", "point", "heatmap",
})

# Map Omni chart type names to mark types for Vega-Lite spec
_OMNI_TO_MARK: dict[str, str] = {
    "line": "line",
    "lineColor": "line",
    "bar": "bar",
    "barStacked": "bar",
    "barGrouped": "bar",
    "barLine": "bar",
    "area": "area",
    "areaStacked": "area",
    "point": "point",
    "heatmap": "rect",
}


def _build_series_entry(s: dict[str, Any]) -> dict[str, Any]:
    """Convert an internal series_config dict to an Omni spec series entry."""
    entry: dict[str, Any] = {}
    if "field" in s:
        entry["field"] = {"name": s["field"]}
    if "mark_type" in s:
        mark: dict[str, Any] = {"type": s["mark_type"]}
        if "color" in s:
            mark["_mark_color"] = s["color"]
        if "dash" in s:
            mark["line"] = {"dash": s["dash"], "width": s.get("line_width", 2)}
        if "point" in s:
            mark["line"] = mark.get("line", {})
            mark["line"]["point"] = s["point"]
        entry["mark"] = mark
    elif "color" in s:
        entry["mark"] = {"_mark_color": s["color"]}
    if "y_axis" in s:
        entry["yAxis"] = s["y_axis"]
    if "manual" in s:
        entry["manual"] = s["manual"]
    # Data labels
    if s.get("show_data_labels"):
        dl: dict[str, Any] = {"enabled": True}
        if "data_label_format" in s:
            dl["format"] = s["data_label_format"]
        if s.get("sparse_labels", True):
            dl["useSparseLabelAlgorithm"] = True
        entry["dataLabel"] = dl
    # Default yAxis to "y" if field is present but no yAxis specified
    if "field" in entry and "yAxis" not in entry:
        entry["yAxis"] = "y"
    return entry


def _build_cartesian_spec(tile: Tile, omni_chart_type: str, fields: list[str]) -> dict[str, Any]:
    """Build Omni cartesian visConfig.spec for series charts.

    Structure matches working Omni dashboards (WBR, User Activation, etc.):
    ``{version, configType, x, y, y2, mark, series, tooltip, _dependentAxis}``
    """
    vc = tile.vis_config
    spec: dict[str, Any] = {"version": 0, "configType": "cartesian"}

    # Derive stacking from chart type or explicit flag
    is_stacked = vc.stacked or omni_chart_type in ("barStacked", "areaStacked")

    # X axis
    if vc.x_axis:
        x_config: dict[str, Any] = {"field": {"name": vc.x_axis}}
        if vc.x_axis_format or vc.x_axis_rotation is not None:
            axis_label: dict[str, Any] = {"format": {}}
            if vc.x_axis_format:
                axis_label["format"]["format"] = vc.x_axis_format
            if vc.x_axis_rotation is not None:
                axis_label["format"]["angle"] = vc.x_axis_rotation
            x_config["axis"] = {"label": axis_label}
        if vc.axis_label_x:
            x_config.setdefault("axis", {})["title"] = {"value": vc.axis_label_x}
        spec["x"] = x_config

    # Y axis
    y_config: dict[str, Any] = {}
    if vc.axis_label_y:
        y_config["axis"] = {"title": {"value": vc.axis_label_y}}
    if vc.y_axis_format:
        y_config.setdefault("axis", {}).setdefault("label", {})["format"] = {
            "format": vc.y_axis_format,
        }
    # Reference lines on Y axis (Omni supports one referenceLine per axis)
    if vc.reference_lines:
        ref = vc.reference_lines[0]
        ref_config: dict[str, Any] = {
            "enabled": True,
            "type": "quantitative",
            "value": ref.get("value", 0),
        }
        if "dash" in ref:
            ref_config["line"] = {"dash": ref["dash"]}
        if "label" in ref:
            ref_config["label"] = ref["label"]
        if "color" in ref:
            ref_config["color"] = ref["color"]
        y_config.setdefault("axis", {})["referenceLine"] = ref_config
    # Color stacking on Y
    if is_stacked:
        y_config.setdefault("color", {})["_stack"] = "stack"
    if y_config:
        spec["y"] = y_config

    # Y2 axis (dual-axis for combo charts)
    if vc.y2_axis:
        y2_config: dict[str, Any] = {}
        if vc.y2_axis_format:
            y2_config["axis"] = {"label": {"format": {"format": vc.y2_axis_format}}}
        spec["y2"] = y2_config or {}

    # Mark type
    mark_type = _OMNI_TO_MARK.get(omni_chart_type, "line")
    spec["mark"] = {"type": mark_type}

    # Series config — use explicit series or auto-generate from fields
    if vc.series_config:
        spec["series"] = [_build_series_entry(s) for s in vc.series_config]
    elif fields:
        # Auto-generate series entries so Omni can assign fields to axes.
        # Exclude pivot and color_by fields — they are dimensions, not measures.
        x_field = vc.x_axis
        exclude: set[str] = set()
        if vc.color_by:
            exclude.add(vc.color_by)
        if tile.query.pivots:
            exclude.update(tile.query.pivots)
        y_fields = [f for f in fields if f != x_field and f not in exclude]
        if y_fields:
            spec["series"] = [
                {"field": {"name": f}, "yAxis": "y", "mark": {"type": mark_type}}
                for f in y_fields
            ]

    # Tooltips
    if vc.tooltip_fields:
        spec["tooltip"] = [{"field": {"name": f}} for f in vc.tooltip_fields]

    # Color
    if vc.color_by:
        color_config: dict[str, Any] = {"field": {"name": vc.color_by}}
        if vc.color_values:
            color_config["manual"] = True
            color_config["values"] = vc.color_values
        spec["color"] = color_config
    elif vc.color_values:
        spec["color"] = {"manual": True, "values": vc.color_values}

    # Trendlines
    if vc.show_trendline:
        trendline: dict[str, Any] = {"enabled": True, "type": vc.trendline_type or "linear"}
        if vc.moving_average_window:
            trendline["window"] = vc.moving_average_window
        spec["trendline"] = trendline

    # Behaviors
    spec["behaviors"] = {"stackMultiMark": is_stacked}
    spec["_dependentAxis"] = "y"
    return spec


def _build_heatmap_spec(tile: Tile) -> dict[str, Any]:
    """Build Omni heatmap visConfig.spec.

    Heatmaps use configType "heatmap" with x, y, and color fields.
    """
    vc = tile.vis_config
    spec: dict[str, Any] = {"version": 0, "configType": "heatmap"}

    if vc.x_axis:
        x_config: dict[str, Any] = {"field": {"name": vc.x_axis}, "manual": True}
        if vc.x_axis_rotation is not None:
            x_config["axis"] = {"label": {"format": {"angle": vc.x_axis_rotation}}}
        spec["x"] = x_config

    if vc.y_axis:
        spec["y"] = {"field": {"name": vc.y_axis[0]}, "manual": True}

    if vc.color_field:
        spec["color"] = {"field": {"name": vc.color_field}}

    if vc.show_data_labels:
        dl: dict[str, Any] = {"enabled": True}
        if vc.data_label_format:
            dl["format"] = vc.data_label_format
        spec["dataLabel"] = dl

    return spec


def _build_vegalite_vis(tile: Tile) -> dict[str, Any]:
    """Build Omni Vega-Lite visConfig for custom visualizations.

    Passes through a full Vega-Lite v5 spec with Omni-standard defaults.
    The spec goes under the ``spec`` key (matching Omni's stored format).
    Field names in VL encoding use escaped dots: ``table\\.field``.
    """
    import copy
    vl_spec = copy.deepcopy(tile.vis_config.vegalite_spec or {})
    # Ensure Omni-friendly defaults
    vl_spec.setdefault("$schema", "https://vega.github.io/schema/vega-lite/v5.json")
    vl_spec.setdefault("width", "container")
    vl_spec.setdefault("background", "transparent")
    vl_spec.setdefault("config", {}).setdefault("view", {})["stroke"] = None

    vis: dict[str, Any] = {
        "visType": "vegalite",
        "chartType": "code",
        "spec": vl_spec,
    }
    # Include query fields so Omni knows which columns to fetch
    if tile.query.fields:
        vis["fields"] = list(tile.query.fields)
    return vis


_MEASURE_PATTERNS = [
    "count", "sum", "avg", "total", "revenue", "rate", "percent",
    "amount", "cost", "spend", "sessions", "visits", "clicks",
    "impressions", "signups", "conversions", "orders", "users",
]


def _pick_kpi_field(fields: list[str], kpi_field: str | None) -> str:
    """Pick the best field for a KPI tile's displayed value.

    Priority: explicit ``kpi_field`` > heuristic measure detection > fields[0].
    """
    if kpi_field:
        return kpi_field
    for f in fields:
        col = f.split(".")[-1].lower()
        if any(p in col for p in _MEASURE_PATTERNS):
            return f
    return fields[0] if fields else ""


def _build_kpi_vis(tile: Tile) -> dict[str, Any]:
    """Build Omni KPI visConfig matching working PLG Sign-Ups pattern.

    Uses ``omni-kpi`` visType with ``markdownConfig`` for rich rendering.
    """
    vc = tile.vis_config
    field_name = _pick_kpi_field(tile.query.fields, vc.kpi_field)

    markdown_config: list[dict[str, Any]] = [{
        "type": "number",
        "config": {
            "field": {
                "row": "_first",
                "field": {"name": field_name, "manual": True, "pivotMap": {}},
                "label": {"value": vc.kpi_label or tile.name},
            },
            "descriptionBefore": tile.description or "",
        },
    }]

    # Optional comparison component
    if vc.kpi_comparison_field:
        comp: dict[str, Any] = {
            "type": "comparison",
            "config": {
                "field": {
                    "row": "_first",
                    "field": {"name": vc.kpi_comparison_field, "manual": True, "pivotMap": {}},
                },
            },
        }
        if vc.kpi_comparison_type:
            comp["config"]["comparisonType"] = vc.kpi_comparison_type
        if vc.kpi_comparison_format:
            comp["config"]["format"] = vc.kpi_comparison_format
        markdown_config.append(comp)

    # Optional sparkline
    if vc.kpi_sparkline:
        markdown_config.append({
            "type": "chart",
            "config": {"type": vc.kpi_sparkline_type or "bar"},
        })

    vis: dict[str, Any] = {
        "visType": "omni-kpi",
        "chartType": "kpi",
        "spec": {
            "alignment": vc.kpi_alignment or "left",
            "fontKPISize": vc.kpi_font_size or "",
            "fontBodySize": vc.kpi_body_font_size or "",
            "fontLabelSize": vc.kpi_label_font_size or "",
            "verticalAlignment": vc.kpi_vertical_alignment or "top",
            "markdownConfig": markdown_config,
        },
        "fields": [field_name],
    }

    if vc.value_format:
        vis["spec"]["markdownConfig"][0]["config"]["field"]["format"] = vc.value_format

    return vis


def _build_markdown_vis(tile: Tile) -> dict[str, Any]:
    """Build Omni markdown tile visConfig.

    Uses ``omni-markdown`` visType with raw markdown/HTML content.

    Field qualification rules (from real Omni dashboards):
    - ``fields`` array keeps fully qualified names: ``["table.col"]``
    - Mustache templates use full qualified names with dots:
      ``{{result.0.table.col.value}}``, ``{{result._last.table.col.raw}}``
    """
    vis: dict[str, Any] = {
        "visType": "omni-markdown",
        "chartType": "markdown",
        "spec": {
            "markdown": tile.vis_config.markdown_template or tile.description or "",
        },
    }
    # Fields stay fully qualified (Omni expects "table.col" not "col")
    if tile.query.fields:
        vis["fields"] = list(tile.query.fields)
    return vis


def _build_table_vis(tile: Tile) -> dict[str, Any]:
    """Build Omni table visConfig matching working spreadsheet tables.

    Supports per-column formatting via ``column_formats`` in vis_config:
    ``{"COL_NAME": {"align": "right", "width": 150}, ...}``
    """
    vc = tile.vis_config
    spec: dict[str, Any] = {
        "tableType": "spreadsheet",
        "rowBanding": {"enabled": vc.table_row_banding, "bandSize": 1},
        "hideIndexColumn": vc.table_hide_index,
        "truncateHeaders": vc.table_truncate_headers,
        "showDescriptions": vc.table_show_descriptions,
        "visColumnDisplay": "hide-view-name",
    }
    if tile.vis_config.column_formats:
        spec["columnFormats"] = tile.vis_config.column_formats
    if tile.vis_config.frozen_column:
        spec["frozenColumn"] = tile.vis_config.frozen_column

    return {
        "visType": "omni-table",
        "chartType": "table",
        "spec": spec,
    }


class DashboardSerializer:
    """Convert DashboardDefinition to/from various formats."""

    @staticmethod
    def to_omni_create_payload(definition: DashboardDefinition) -> dict[str, Any]:
        """Convert a DashboardDefinition to the Omni POST /api/v1/documents payload.

        This is the primary serialization target for creating dashboards via API.

        Applies Omni-specific rules learned from real working dashboards:
        - KPI tiles must NOT have sorts (they're single-value aggregations)
        - Sort column_names MUST appear in the fields list
        - Filters use Omni's ``{kind, type, values}`` format
        - Default limit is 1000 (Omni's standard)
        - KPI tiles use ``omni-kpi`` visType with ``markdownConfig``
        - Markdown tiles use ``omni-markdown`` visType
        - Table tiles use ``omni-table`` visType
        - Cartesian charts generate rich ``spec`` when advanced vis config is set
        - Dashboard-level filters are propagated to matching tile queries
        """
        if not definition.model_id:
            raise DashboardDefinitionError(
                "model_id is required for Omni API payload. "
                "Set it via DashboardBuilder.model() or in the definition."
            )

        query_presentations = []
        for tile in definition.tiles:
            omni_chart_type = _CHART_TYPE_TO_OMNI.get(tile.chart_type, tile.chart_type)
            is_kpi = omni_chart_type in ("kpi", "summaryValue")
            is_markdown = omni_chart_type == "markdown"
            is_table = omni_chart_type == "table"
            is_heatmap = tile.chart_type == "heatmap"
            is_vegalite = tile.chart_type == "vegalite"

            # Build fields list — ensure sort columns are included
            fields = list(tile.query.fields)

            # KPI tiles: no sorts (working KPIs in Omni always have sorts=[])
            # Other tiles: validate sort columns are in fields
            sorts: list[dict[str, Any]] = []
            if not is_kpi and tile.query.sorts:
                for s in tile.query.sorts:
                    if s.column_name not in fields:
                        fields.append(s.column_name)
                    sorts.append({
                        "column_name": s.column_name,
                        "sort_descending": s.sort_descending,
                        "null_sort": "DIALECT_DEFAULT",
                        "is_column_sort": False,
                    })

            # Determine limit — KPI tiles use 1, others default to 1000
            limit = tile.query.limit
            if is_kpi and limit > 1:
                limit = 1
            elif limit == 200:
                # Upgrade old default (200) to Omni's standard (1000)
                limit = 1000

            qp: dict[str, Any] = {
                "name": tile.name,
                "description": tile.description,
                "query": {
                    "table": tile.query.table,
                    "fields": fields,
                    "modelId": definition.model_id,
                    "limit": limit,
                    "sorts": sorts,
                },
            }

            # Subtitle
            if tile.subtitle:
                qp["subTitle"] = tile.subtitle

            # SQL mode
            if tile.query.is_sql:
                qp["isSql"] = True
                if tile.query.user_sql:
                    qp["query"]["userEditedSQL"] = tile.query.user_sql

            # Row/column totals
            if tile.query.row_totals:
                qp["query"]["row_totals"] = {}
            if tile.query.column_totals:
                qp["query"]["column_totals"] = {}

            # Fill fields (fill nulls in time series)
            if tile.query.fill_fields:
                qp["query"]["fill_fields"] = tile.query.fill_fields

            # Filters — convert to Omni's format
            omni_filters: dict[str, Any] = {}
            if tile.query.filters:
                for f in tile.query.filters:
                    omni_filters[f.field] = _to_omni_filter(f)

            # Dashboard-level filters → propagate to matching tile queries.
            # Cross-table: if filter is on table_a.col but tile queries table_b,
            # remap to table_b.col when the tile has a field with the same column name.
            if definition.filters:
                table = tile.query.table
                tile_cols = {f.split(".")[-1] for f in tile.query.fields}
                for dash_filter in definition.filters:
                    fld = dash_filter.field
                    if dash_filter.default_value is None:
                        continue
                    if fld.startswith(table + ".") or "." not in fld:
                        # Same table — direct match
                        omni_filters[fld] = _to_omni_filter_from_dashboard(dash_filter)
                    else:
                        # Cross-table — remap if tile has matching column name
                        col_name = fld.split(".")[-1]
                        if col_name in tile_cols:
                            remapped = f"{table}.{col_name}"
                            omni_filters[remapped] = _to_omni_filter_from_dashboard(dash_filter)

            if omni_filters:
                qp["query"]["filters"] = omni_filters

            # Composite filters (AND/OR combinations)
            if tile.query.composite_filters:
                for cf in tile.query.composite_filters:
                    if cf.conditions:
                        field_key = cf.conditions[0].field
                        composite_entry: dict[str, Any] = {
                            "type": "composite",
                            "conjunction": cf.conjunction,
                            "filters": [_to_omni_filter(c) for c in cf.conditions],
                        }
                        qp["query"].setdefault("filters", {})[field_key] = composite_entry

            # Calculated fields
            if tile.query.calculations:
                calcs = []
                for calc in tile.query.calculations:
                    calc_entry: dict[str, Any] = {
                        "calc_name": calc.calc_name,
                        "label": calc.label,
                        "swallow_errors": True,
                    }
                    if calc.format:
                        calc_entry["format"] = calc.format
                    if calc.sql_expression:
                        calc_entry["sql_expression"] = calc.sql_expression
                    elif calc.formula:
                        # Simple formula → Omni safe_divide AST
                        parts = calc.formula.split("/")
                        if len(parts) == 2:
                            calc_entry["sql_expression"] = {
                                "type": "call",
                                "operator": "Omni.OMNI_FX_SAFE_DIVIDE",
                                "operands": [
                                    {"type": "field", "field_name": parts[0].strip()},
                                    {"type": "field", "field_name": parts[1].strip()},
                                ],
                            }
                    calcs.append(calc_entry)
                qp["query"]["calculations"] = calcs

            # Field metadata overrides (custom labels)
            if tile.query.metadata:
                qp["query"]["metadata"] = tile.query.metadata

            # Pivots
            if tile.query.pivots:
                qp["query"]["pivots"] = tile.query.pivots

            qp["chartType"] = omni_chart_type
            qp["prefersChart"] = omni_chart_type not in ("table", "markdown")

            # Visualization config — use rich Omni visTypes when appropriate
            if is_vegalite:
                qp["visConfig"] = _build_vegalite_vis(tile)
            elif is_kpi:
                qp["visConfig"] = _build_kpi_vis(tile)
            elif is_markdown:
                qp["visConfig"] = _build_markdown_vis(tile)
            elif is_table:
                qp["visConfig"] = _build_table_vis(tile)
            elif is_heatmap:
                qp["visConfig"] = {
                    "visType": "basic",
                    "chartType": "heatmap",
                    "spec": _build_heatmap_spec(tile),
                }
            elif omni_chart_type in _CARTESIAN_CHART_TYPES:
                qp["visConfig"] = {
                    "visType": "basic",
                    "chartType": omni_chart_type,
                    "spec": _build_cartesian_spec(tile, omni_chart_type, fields),
                }

            # Position (layout)
            if tile.position:
                qp["position"] = {
                    "x": tile.position.x,
                    "y": tile.position.y,
                    "w": tile.position.w,
                    "h": tile.position.h,
                }

            query_presentations.append(qp)

        payload: dict[str, Any] = {
            "modelId": definition.model_id,
            "name": definition.name,
            "queryPresentations": query_presentations,
        }

        if definition.folder_id:
            payload["folderId"] = definition.folder_id

        # Refresh interval (only if non-default)
        if definition.refresh_interval != 3600:
            payload["refreshInterval"] = definition.refresh_interval

        # Custom theme
        if definition.theme:
            payload["dashboardCustomTheme"] = definition.theme

        # Hidden tiles
        hidden_tiles = [tile.name for tile in definition.tiles if tile.hidden]
        if hidden_tiles:
            payload.setdefault("metadata", {})["hiddenTiles"] = hidden_tiles

        # Tile filter map (per-tile filter targeting)
        if definition.tile_filter_map:
            payload.setdefault("metadata", {})["tileFilterMap"] = definition.tile_filter_map

        # Dashboard-level filters → filterConfig for Omni's filter UI controls
        if definition.filters:
            filter_config: dict[str, Any] = {}
            filter_order: list[str] = []
            for dash_filter in definition.filters:
                # Generate a short deterministic ID from the field name
                fid = hashlib.md5(dash_filter.field.encode()).hexdigest()[:8]
                omni_filter = _to_omni_filter_from_dashboard(dash_filter)
                omni_filter["fieldName"] = dash_filter.field
                omni_filter["label"] = dash_filter.label or dash_filter.field
                filter_config[fid] = omni_filter
                filter_order.append(fid)
            payload["filterConfig"] = filter_config
            payload["filterOrder"] = filter_order

        return payload

    @staticmethod
    def to_yaml(definition: DashboardDefinition) -> str:
        """Serialize a DashboardDefinition to YAML for version control."""
        data: dict[str, Any] = {
            "version": "1.0",
            "dashboard": {
                "name": definition.name,
                "description": definition.description,
                "model_id": definition.model_id,
                "refresh_interval": definition.refresh_interval,
            },
        }

        if definition.source_template:
            data["source_template"] = definition.source_template
        if definition.dbt_model:
            data["dbt_model"] = definition.dbt_model
        if definition.folder_id:
            data["dashboard"]["folder_id"] = definition.folder_id
        if definition.labels:
            data["dashboard"]["labels"] = definition.labels
        if definition.meta:
            data["meta"] = definition.meta

        # Filters
        if definition.filters:
            data["dashboard"]["filters"] = [
                {
                    "field": f.field,
                    "type": f.filter_type,
                    "label": f.label,
                    "default": f.default_value,
                    "required": f.required,
                    **({"options": f.options} if f.options else {}),
                }
                for f in definition.filters
            ]

        # Tiles
        data["dashboard"]["tiles"] = []
        for tile in definition.tiles:
            tile_data: dict[str, Any] = {
                "name": tile.name,
                "chart_type": tile.chart_type,
                "size": tile.size,
                "query": {
                    "table": tile.query.table,
                    "fields": tile.query.fields,
                },
            }

            if tile.description:
                tile_data["description"] = tile.description

            if tile.query.sorts:
                tile_data["query"]["sorts"] = [
                    {"column_name": s.column_name, "sort_descending": s.sort_descending}
                    for s in tile.query.sorts
                ]

            if tile.query.filters:
                tile_data["query"]["filters"] = [
                    {"field": f.field, "operator": f.operator, "value": f.value}
                    for f in tile.query.filters
                ]

            if tile.query.limit != 200:
                tile_data["query"]["limit"] = tile.query.limit

            if tile.query.pivots:
                tile_data["query"]["pivots"] = tile.query.pivots

            # Vis config (only non-default values)
            vis: dict[str, Any] = {}
            if tile.vis_config.x_axis:
                vis["x_axis"] = tile.vis_config.x_axis
            if tile.vis_config.y_axis:
                vis["y_axis"] = tile.vis_config.y_axis
            if tile.vis_config.color_by:
                vis["color_by"] = tile.vis_config.color_by
            if tile.vis_config.stacked:
                vis["stacked"] = True
            if not tile.vis_config.show_labels:
                vis["show_labels"] = False
            if not tile.vis_config.show_legend:
                vis["show_legend"] = False
            if not tile.vis_config.show_grid:
                vis["show_grid"] = False
            if tile.vis_config.show_values:
                vis["show_values"] = True
            if tile.vis_config.value_format:
                vis["value_format"] = tile.vis_config.value_format
            if tile.vis_config.axis_label_x:
                vis["axis_label_x"] = tile.vis_config.axis_label_x
            if tile.vis_config.axis_label_y:
                vis["axis_label_y"] = tile.vis_config.axis_label_y
            if tile.vis_config.series_colors:
                vis["series_colors"] = tile.vis_config.series_colors
            if tile.vis_config.custom:
                vis["custom"] = tile.vis_config.custom
            if vis:
                tile_data["vis_config"] = vis

            # Position
            if tile.position:
                tile_data["position"] = {
                    "x": tile.position.x,
                    "y": tile.position.y,
                    "w": tile.position.w,
                    "h": tile.position.h,
                }

            data["dashboard"]["tiles"].append(tile_data)

        # Text tiles
        if definition.text_tiles:
            data["dashboard"]["text_tiles"] = [
                {
                    "content": t.content,
                    **(
                        {
                            "position": {
                                "x": t.position.x,
                                "y": t.position.y,
                                "w": t.position.w,
                                "h": t.position.h,
                            }
                        }
                        if t.position
                        else {}
                    ),
                }
                for t in definition.text_tiles
            ]

        return yaml.dump(data, default_flow_style=False, sort_keys=False, width=120)

    @staticmethod
    def from_yaml(yaml_str: str) -> DashboardDefinition:
        """Deserialize a DashboardDefinition from YAML."""
        data = yaml.safe_load(yaml_str)
        if not data or not isinstance(data, dict):
            raise DashboardDefinitionError("Invalid YAML: empty or not a mapping")

        dash = data.get("dashboard", data)

        # Parse tiles
        tiles = []
        for tile_data in dash.get("tiles", []):
            query_data = tile_data.get("query", {})

            sorts = [
                SortSpec(
                    column_name=s.get("column_name", ""),
                    sort_descending=s.get("sort_descending", False),
                )
                for s in query_data.get("sorts", [])
            ]

            filters = [
                FilterSpec(
                    field=f.get("field", ""),
                    operator=f.get("operator", "is"),
                    value=f.get("value"),
                )
                for f in query_data.get("filters", [])
            ]

            vis_data = tile_data.get("vis_config", {})
            vis_config = TileVisConfig(
                x_axis=vis_data.get("x_axis"),
                y_axis=vis_data.get("y_axis", []),
                color_by=vis_data.get("color_by"),
                stacked=vis_data.get("stacked", False),
                show_labels=vis_data.get("show_labels", True),
                show_legend=vis_data.get("show_legend", True),
                show_grid=vis_data.get("show_grid", True),
                show_values=vis_data.get("show_values", False),
                value_format=vis_data.get("value_format"),
                axis_label_x=vis_data.get("axis_label_x"),
                axis_label_y=vis_data.get("axis_label_y"),
                series_colors=vis_data.get("series_colors", {}),
                custom=vis_data.get("custom", {}),
            )

            pos_data = tile_data.get("position")
            position = (
                TilePosition(
                    x=pos_data.get("x", 0),
                    y=pos_data.get("y", 0),
                    w=pos_data.get("w", 6),
                    h=pos_data.get("h", 4),
                )
                if pos_data
                else None
            )

            tiles.append(
                Tile(
                    name=tile_data.get("name", ""),
                    description=tile_data.get("description", ""),
                    query=TileQuery(
                        table=query_data.get("table", ""),
                        fields=query_data.get("fields", []),
                        sorts=sorts,
                        filters=filters,
                        limit=query_data.get("limit", 200),
                        pivots=query_data.get("pivots", []),
                    ),
                    chart_type=tile_data.get("chart_type", "line"),
                    vis_config=vis_config,
                    position=position,
                    size=tile_data.get("size", "half"),
                )
            )

        # Parse text tiles
        text_tiles = [
            TextTile(
                content=t.get("content", ""),
                position=TilePosition(**t["position"]) if t.get("position") else None,
            )
            for t in dash.get("text_tiles", [])
        ]

        # Parse filters
        dashboard_filters = [
            DashboardFilter(
                field=f.get("field", ""),
                filter_type=f.get("type", "date_range"),
                label=f.get("label", ""),
                default_value=f.get("default"),
                required=f.get("required", False),
                options=f.get("options", []),
            )
            for f in dash.get("filters", [])
        ]

        return DashboardDefinition(
            name=dash.get("name", ""),
            model_id=dash.get("model_id", ""),
            description=dash.get("description", ""),
            tiles=tiles,
            text_tiles=text_tiles,
            filters=dashboard_filters,
            refresh_interval=dash.get("refresh_interval", 3600),
            source_template=data.get("source_template"),
            dbt_model=data.get("dbt_model"),
            folder_id=dash.get("folder_id"),
            labels=dash.get("labels", []),
            meta=data.get("meta", {}),
        )

    @staticmethod
    def from_omni_export(export_data: dict[str, Any]) -> DashboardDefinition:
        """Parse an Omni dashboard export into a DashboardDefinition.

        Handles the JSON structure returned by GET /api/unstable/documents/:id/export.

        Real Omni export structure:
          dashboard.queryPresentationCollection.queryPresentationCollectionMemberships[].queryPresentation
          queryPresentation.query.queryJson  (query fields, sorts, filters, etc.)
          queryPresentation.visConfig.chartType
          dashboard.metadata.layouts.lg[]  (tile positions)
        """
        doc = export_data.get("document", {})
        dash = export_data.get("dashboard", {})

        # Extract query presentations from the real nested structure
        qpc = dash.get("queryPresentationCollection", {})
        memberships = qpc.get("queryPresentationCollectionMemberships", [])

        # Build layout map: 1-indexed position → {x, y, w, h}
        metadata = dash.get("metadata", {})
        layout_items = metadata.get("layouts", {}).get("lg", [])
        layout_map: dict[int, dict[str, int]] = {}
        for item in layout_items:
            idx = item.get("i")
            if idx is not None:
                layout_map[int(idx)] = {
                    "x": item.get("x", 0),
                    "y": item.get("y", 0),
                    "w": item.get("w", 12),
                    "h": item.get("h", 6),
                }

        tiles = []
        for i, membership in enumerate(memberships):
            qp = membership.get("queryPresentation", {})

            # Query data is nested under query.queryJson
            raw_query = qp.get("query", {})
            query = raw_query.get("queryJson", raw_query)

            # Chart type is in visConfig.chartType
            vis_config_data = qp.get("visConfig", {})
            omni_chart_type = vis_config_data.get("chartType", "line")
            vis_spec = vis_config_data.get("spec", {})

            sorts = [
                SortSpec(
                    column_name=s.get("column_name", s.get("columnName", "")),
                    sort_descending=s.get("sort_descending", s.get("sortDescending", False)),
                )
                for s in query.get("sorts", [])
            ]

            raw_filters = query.get("filters", {})
            filters = []
            if isinstance(raw_filters, dict):
                for field, fdata in raw_filters.items():
                    if isinstance(fdata, dict):
                        filters.append(
                            FilterSpec(
                                field=field,
                                operator=fdata.get("operator", "is"),
                                value=fdata.get("value"),
                            )
                        )

            # Layout position — Omni uses a 24-col grid, we use 12-col.
            # Scale x and w by half; height is in Omni's own units so
            # clamp to a sensible range.
            layout = layout_map.get(i + 1)
            position = None
            if layout:
                scaled_x = max(0, min(11, layout["x"] // 2))
                scaled_w = max(1, min(12, layout["w"] // 2))
                if scaled_x + scaled_w > 12:
                    scaled_w = 12 - scaled_x
                # Omni heights are ~10x ours; scale down, minimum 2
                scaled_h = max(2, layout["h"] // 10)
                position = TilePosition(
                    x=scaled_x,
                    y=layout["y"],
                    w=scaled_w,
                    h=scaled_h,
                )

            tiles.append(
                Tile(
                    name=qp.get("name", ""),
                    description=qp.get("description", ""),
                    query=TileQuery(
                        table=query.get("table", ""),
                        fields=query.get("fields", []),
                        sorts=sorts,
                        filters=filters,
                        limit=query.get("limit", 200),
                        pivots=query.get("pivots", []),
                    ),
                    chart_type=_OMNI_TO_CHART_TYPE.get(
                        omni_chart_type, omni_chart_type
                    ),
                    vis_config=TileVisConfig(
                        x_axis=vis_spec.get("xAxis"),
                        y_axis=vis_spec.get("yAxis", []),
                        color_by=vis_spec.get("colorBy"),
                        stacked=vis_spec.get("stacked", False),
                        show_values=vis_spec.get("showValues", False),
                        series_colors=vis_spec.get("seriesColors", {}),
                    ),
                    position=position,
                )
            )

        return DashboardDefinition(
            name=doc.get("name", ""),
            model_id=doc.get("modelId", ""),
            description=doc.get("description", ""),
            tiles=tiles,
        )
