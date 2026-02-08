"""Tool definitions for Claude's function calling in dashboard generation.

Defines 4 tools that Claude can use to explore the dbt data catalog
and create validated dashboard definitions:
- list_models: Discover available dbt models
- get_model_detail: Get column metadata for a specific model
- search_models: Search models by keyword
- create_dashboard: Validate and build a DashboardDefinition
"""

from __future__ import annotations

import json
import logging
from typing import Any

from omni_dash.dashboard.definition import ChartType, DashboardDefinition, TileSize
from omni_dash.dashboard.layout import LayoutManager
from omni_dash.dbt.model_registry import ModelRegistry

logger = logging.getLogger(__name__)


def get_tool_definitions() -> list[dict[str, Any]]:
    """Return Anthropic tool definitions for the dashboard generation agent."""
    return [
        {
            "name": "list_models",
            "description": (
                "List available dbt models in the data warehouse. "
                "Returns model names, descriptions, column counts, and whether "
                "they are Omni-ready (have OMNATA_SYNC_ENGINE grants). "
                "Call this first to understand what data is available."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "layer": {
                        "type": "string",
                        "description": (
                            "Filter by dbt layer. Use 'mart' for analytics-ready models "
                            "(recommended for dashboards), or omit for all models."
                        ),
                        "enum": ["mart", "staging", "intermediate"],
                    },
                },
                "required": [],
            },
        },
        {
            "name": "get_model_detail",
            "description": (
                "Get detailed metadata for a specific dbt model, including all columns "
                "with their names, descriptions, and data types. Use this to understand "
                "what fields are available for building dashboard tiles."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "description": "The dbt model name (e.g., 'mart_seo_weekly_funnel').",
                    },
                },
                "required": ["model_name"],
            },
        },
        {
            "name": "search_models",
            "description": (
                "Search dbt models by keyword across names, descriptions, and column names. "
                "Use this when the user's description mentions a topic and you need to find "
                "the relevant model."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "keyword": {
                        "type": "string",
                        "description": "Search keyword (e.g., 'seo', 'paid', 'retention').",
                    },
                },
                "required": ["keyword"],
            },
        },
        {
            "name": "create_dashboard",
            "description": (
                "Create a validated dashboard definition. The input must be a complete "
                "dashboard specification with tiles, queries, and chart configurations. "
                "The system will validate the definition and auto-position tiles on a "
                "12-column grid. If validation fails, you'll receive error details — "
                "fix them and call this tool again."
            ),
            "input_schema": _build_dashboard_schema(),
        },
    ]


def _build_dashboard_schema() -> dict[str, Any]:
    """Build the JSON schema for the create_dashboard tool input.

    Uses DashboardDefinition's Pydantic schema with additional guidance
    for Claude in the descriptions.
    """
    valid_chart_types = [ct.value for ct in ChartType]
    valid_sizes = [ts.value for ts in TileSize]

    return {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Dashboard display name (e.g., 'SEO Weekly Funnel').",
            },
            "description": {
                "type": "string",
                "description": "Brief description of the dashboard's purpose.",
                "default": "",
            },
            "tiles": {
                "type": "array",
                "description": "List of dashboard tiles (charts, tables, numbers).",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Tile display name (e.g., 'Organic Visits Over Time').",
                        },
                        "description": {
                            "type": "string",
                            "description": "Brief description of what this tile shows.",
                            "default": "",
                        },
                        "chart_type": {
                            "type": "string",
                            "description": f"Chart type. Must be one of: {valid_chart_types}",
                            "enum": valid_chart_types,
                        },
                        "size": {
                            "type": "string",
                            "description": (
                                f"Tile size for auto-layout. Options: {valid_sizes}. "
                                "full=12cols, half=6cols, third=4cols, quarter=3cols, two_thirds=8cols."
                            ),
                            "enum": valid_sizes,
                            "default": "half",
                        },
                        "query": {
                            "type": "object",
                            "description": "Data query for this tile.",
                            "properties": {
                                "table": {
                                    "type": "string",
                                    "description": "The dbt model/table name (e.g., 'mart_seo_weekly_funnel').",
                                },
                                "fields": {
                                    "type": "array",
                                    "description": (
                                        "List of field references. MUST be qualified as 'table_name.column_name' "
                                        "(e.g., 'mart_seo_weekly_funnel.week_start')."
                                    ),
                                    "items": {"type": "string"},
                                },
                                "sorts": {
                                    "type": "array",
                                    "description": "Sort specifications.",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "column_name": {
                                                "type": "string",
                                                "description": "Qualified field name to sort by.",
                                            },
                                            "sort_descending": {
                                                "type": "boolean",
                                                "default": False,
                                            },
                                        },
                                        "required": ["column_name"],
                                    },
                                    "default": [],
                                },
                                "filters": {
                                    "type": "array",
                                    "description": "Query-level filters.",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "field": {"type": "string"},
                                            "operator": {
                                                "type": "string",
                                                "default": "is",
                                            },
                                            "value": {},
                                        },
                                        "required": ["field"],
                                    },
                                    "default": [],
                                },
                                "limit": {
                                    "type": "integer",
                                    "description": (
                                        "Max rows. Default 200 (auto-upgraded to 1000 for Omni). "
                                        "Use 1 for KPI/number tiles."
                                    ),
                                    "default": 200,
                                },
                                "pivots": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "default": [],
                                },
                                "calculations": {
                                    "type": "array",
                                    "description": (
                                        "Calculated fields. Each: {calc_name, label, "
                                        "formula ('field_a / field_b' for safe divide), format}."
                                    ),
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "calc_name": {"type": "string"},
                                            "label": {"type": "string"},
                                            "formula": {"type": "string"},
                                            "format": {"type": "string"},
                                        },
                                        "required": ["calc_name", "label"],
                                    },
                                    "default": [],
                                },
                                "metadata": {
                                    "type": "object",
                                    "description": (
                                        "Per-field metadata overrides. "
                                        "E.g., {\"t.field\": {\"label\": \"Custom Name\"}}."
                                    ),
                                    "additionalProperties": {
                                        "type": "object",
                                        "properties": {
                                            "label": {"type": "string"},
                                        },
                                    },
                                },
                                "composite_filters": {
                                    "type": "array",
                                    "description": (
                                        "Composite filters combining conditions with AND/OR. "
                                        "Each: {conditions: [{field, operator, value}], conjunction: 'AND'|'OR'}."
                                    ),
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "conditions": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "field": {"type": "string"},
                                                        "operator": {"type": "string"},
                                                        "value": {},
                                                    },
                                                    "required": ["field"],
                                                },
                                            },
                                            "conjunction": {
                                                "type": "string",
                                                "enum": ["AND", "OR"],
                                                "default": "AND",
                                            },
                                        },
                                    },
                                    "default": [],
                                },
                            },
                            "required": ["table", "fields"],
                        },
                        "vis_config": {
                            "type": "object",
                            "description": "Visualization configuration.",
                            "properties": {
                                "x_axis": {
                                    "type": "string",
                                    "description": "Qualified field for x-axis (e.g., time column for line charts).",
                                },
                                "y_axis": {
                                    "type": "array",
                                    "description": "Qualified field(s) for y-axis (metric columns).",
                                    "items": {"type": "string"},
                                },
                                "color_by": {
                                    "type": "string",
                                    "description": "Qualified field to color/group by (for breakdowns).",
                                },
                                "stacked": {
                                    "type": "boolean",
                                    "default": False,
                                },
                                "show_labels": {
                                    "type": "boolean",
                                    "default": True,
                                },
                                "show_legend": {
                                    "type": "boolean",
                                    "default": True,
                                },
                                "show_values": {
                                    "type": "boolean",
                                    "default": False,
                                },
                                "value_format": {
                                    "type": "string",
                                    "description": (
                                        "Omni number format code. Common values: "
                                        "USDCURRENCY_0 ($1,235), USDCURRENCY_2 ($1,234.50), "
                                        "BIGNUMBER_2 (5.60M), PERCENT_0 (24%), PERCENT_2 (24.40%), "
                                        "number_0 (1,235), big_0 (5.6M)."
                                    ),
                                },
                                "x_axis_format": {
                                    "type": "string",
                                    "description": (
                                        "Date format for x-axis labels. "
                                        "Examples: '%-m/%-d/%-Y', 'MMM-DD-YY'."
                                    ),
                                },
                                "x_axis_rotation": {
                                    "type": "integer",
                                    "description": "Label rotation angle for x-axis (0, 45, 270).",
                                },
                                "y_axis_format": {
                                    "type": "string",
                                    "description": "Number format for y-axis labels (e.g., 'USDCURRENCY_0').",
                                },
                                "y2_axis": {
                                    "type": "boolean",
                                    "description": "Enable secondary Y axis (for combo/dual-axis charts).",
                                    "default": False,
                                },
                                "y2_axis_format": {
                                    "type": "string",
                                    "description": "Number format for secondary y-axis.",
                                },
                                "axis_label_x": {
                                    "type": "string",
                                    "description": "Custom x-axis title.",
                                },
                                "axis_label_y": {
                                    "type": "string",
                                    "description": "Custom y-axis title.",
                                },
                                "series_config": {
                                    "type": "array",
                                    "description": (
                                        "Per-series customization for multi-series/combo charts. "
                                        "Each entry: {field, mark_type (line|bar), color (#hex), "
                                        "y_axis (y|y2), show_data_labels (bool), data_label_format}."
                                    ),
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "field": {"type": "string"},
                                            "mark_type": {"type": "string", "enum": ["line", "bar", "area", "point"]},
                                            "color": {"type": "string"},
                                            "y_axis": {"type": "string", "enum": ["y", "y2"]},
                                            "show_data_labels": {"type": "boolean"},
                                            "data_label_format": {"type": "string"},
                                        },
                                    },
                                },
                                "tooltip_fields": {
                                    "type": "array",
                                    "description": "Qualified fields to show in chart tooltips.",
                                    "items": {"type": "string"},
                                },
                                "kpi_label": {
                                    "type": "string",
                                    "description": "Override display label for KPI tiles.",
                                },
                                "kpi_sparkline": {
                                    "type": "boolean",
                                    "description": "Show sparkline in KPI tile.",
                                    "default": False,
                                },
                                "kpi_sparkline_type": {
                                    "type": "string",
                                    "description": "Sparkline type: 'bar' or 'line'.",
                                    "enum": ["bar", "line"],
                                },
                                "kpi_comparison_field": {
                                    "type": "string",
                                    "description": "Qualified field for KPI comparison value.",
                                },
                                "kpi_comparison_type": {
                                    "type": "string",
                                    "description": "Comparison display type.",
                                    "enum": ["number_percent", "number", "percent"],
                                },
                                "markdown_template": {
                                    "type": "string",
                                    "description": (
                                        "Raw markdown/HTML template for text tiles. "
                                        "Supports Mustache syntax: {{result.0.field.value}} (formatted), "
                                        "{{result.0.field.raw}} (raw for CSS), "
                                        "{{result._last.field.value}} (last row)."
                                    ),
                                },
                                "reference_lines": {
                                    "type": "array",
                                    "description": (
                                        "Reference/target lines on charts. "
                                        "Each entry: {value (number), label (string), "
                                        "dash ([8,8] for dashed), color (#hex)}."
                                    ),
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "value": {"type": "number"},
                                            "label": {"type": "string"},
                                            "dash": {"type": "array", "items": {"type": "integer"}},
                                            "color": {"type": "string"},
                                        },
                                        "required": ["value"],
                                    },
                                },
                                "color_field": {
                                    "type": "string",
                                    "description": "Field for heatmap color intensity (qualified name).",
                                },
                                "color_values": {
                                    "type": "object",
                                    "description": (
                                        "Manual color mapping: category name → hex color. "
                                        "E.g., {\"Brand\": \"#FF8515\", \"Non-Brand\": \"#BE43C0\"}."
                                    ),
                                    "additionalProperties": {"type": "string"},
                                },
                                "show_data_labels": {
                                    "type": "boolean",
                                    "description": "Show data labels on chart marks.",
                                    "default": False,
                                },
                                "data_label_format": {
                                    "type": "string",
                                    "description": "Format for data labels (e.g., 'PERCENT_1', 'BIGNUMBER_2').",
                                },
                                "frozen_column": {
                                    "type": "string",
                                    "description": "Pin a table column for horizontal scrolling (qualified field name).",
                                },
                                "column_formats": {
                                    "type": "object",
                                    "description": (
                                        "Per-column formatting for tables. "
                                        "E.g., {\"t.revenue\": {\"align\": \"right\", \"width\": 150}}."
                                    ),
                                    "additionalProperties": {
                                        "type": "object",
                                        "properties": {
                                            "align": {"type": "string", "enum": ["left", "right", "center"]},
                                            "width": {"type": "integer"},
                                        },
                                    },
                                },
                                "vegalite_spec": {
                                    "type": "object",
                                    "description": (
                                        "Full Vega-Lite v5 spec for custom visualizations. "
                                        "Use chart_type='vegalite'. Omni wraps with container width."
                                    ),
                                },
                            },
                        },
                    },
                    "required": ["name", "chart_type", "query"],
                },
            },
            "folder_id": {
                "type": "string",
                "description": (
                    "Optional Omni folder ID to place the dashboard in. "
                    "If omitted, the dashboard will be created in the user's default location."
                ),
            },
            "filters": {
                "type": "array",
                "description": "Dashboard-level filter controls.",
                "items": {
                    "type": "object",
                    "properties": {
                        "field": {
                            "type": "string",
                            "description": "Qualified field name for the filter.",
                        },
                        "filter_type": {
                            "type": "string",
                            "description": "Filter UI type.",
                            "enum": ["date_range", "select", "multi_select", "text", "number_range"],
                            "default": "date_range",
                        },
                        "label": {
                            "type": "string",
                            "description": "Display label for the filter.",
                        },
                        "default_value": {
                            "description": "Default filter value.",
                        },
                        "required": {
                            "type": "boolean",
                            "default": False,
                        },
                    },
                    "required": ["field"],
                },
                "default": [],
            },
        },
        "required": ["name", "tiles"],
    }


class ToolExecutor:
    """Executes tool calls against the ModelRegistry and validates dashboard definitions."""

    def __init__(self, registry: ModelRegistry):
        self._registry = registry
        self._last_valid_definition: DashboardDefinition | None = None

    @property
    def last_valid_definition(self) -> DashboardDefinition | None:
        return self._last_valid_definition

    def execute(self, tool_name: str, tool_input: dict[str, Any]) -> tuple[str, bool]:
        """Execute a tool and return (result_json, is_error).

        Returns:
            Tuple of (result string, is_error flag).
        """
        try:
            if tool_name == "list_models":
                return self._list_models(tool_input), False
            elif tool_name == "get_model_detail":
                return self._get_model_detail(tool_input), False
            elif tool_name == "search_models":
                return self._search_models(tool_input), False
            elif tool_name == "create_dashboard":
                return self._create_dashboard(tool_input)
            else:
                return json.dumps({"error": f"Unknown tool: {tool_name}"}), True
        except Exception as e:
            logger.warning("Tool %s failed: %s", tool_name, e)
            return json.dumps({"error": str(e)}), True

    def _list_models(self, tool_input: dict[str, Any]) -> str:
        layer = tool_input.get("layer")
        models = self._registry.list_models(layer=layer)
        result = [
            {
                "name": m.name,
                "description": m.description[:200] if m.description else "",
                "column_count": len(m.columns),
                "has_omni_grant": m.has_omni_grant,
                "materialization": m.materialization,
            }
            for m in models
        ]
        return json.dumps(result)

    def _get_model_detail(self, tool_input: dict[str, Any]) -> str:
        model_name = tool_input["model_name"]
        model = self._registry.get_model(model_name)
        result = {
            "name": model.name,
            "description": model.description,
            "materialization": model.materialization,
            "has_omni_grant": model.has_omni_grant,
            "columns": [
                {
                    "name": c.name,
                    "description": c.description,
                    "data_type": c.data_type,
                }
                for c in model.columns
            ],
        }
        return json.dumps(result)

    def _search_models(self, tool_input: dict[str, Any]) -> str:
        keyword = tool_input["keyword"]
        models = self._registry.search_models(keyword)
        result = [
            {
                "name": m.name,
                "description": m.description[:200] if m.description else "",
                "column_count": len(m.columns),
            }
            for m in models[:10]
        ]
        return json.dumps(result)

    def _create_dashboard(self, tool_input: dict[str, Any]) -> tuple[str, bool]:
        """Validate and create a DashboardDefinition from tool input.

        Returns (result_json, is_error). On validation failure, returns
        the error details so Claude can self-correct.
        """
        try:
            definition = DashboardDefinition(**tool_input)
            # Auto-position tiles
            definition.tiles = LayoutManager.auto_position(definition.tiles)
            self._last_valid_definition = definition
            return json.dumps({
                "status": "success",
                "tile_count": definition.tile_count,
                "tables_used": list(definition.all_tables()),
                "fields_used": list(definition.all_fields()),
            }), False
        except Exception as e:
            error_msg = str(e)
            logger.info("Dashboard validation failed: %s", error_msg)
            return json.dumps({
                "status": "validation_error",
                "errors": error_msg,
                "hint": "Fix the errors and call create_dashboard again.",
            }), True
