"""Core data models for dashboard definitions.

These Pydantic models represent the "dashboard-as-code" artifact — the
central object that flows between templates, builders, serializers,
and the Omni API.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class ChartType(str, Enum):
    """Supported chart types in Omni."""

    LINE = "line"
    BAR = "bar"
    AREA = "area"
    SCATTER = "scatter"
    PIE = "pie"
    DONUT = "donut"
    TABLE = "table"
    NUMBER = "number"
    FUNNEL = "funnel"
    HEATMAP = "heatmap"
    STACKED_BAR = "stacked_bar"
    STACKED_AREA = "stacked_area"
    GROUPED_BAR = "grouped_bar"
    COMBO = "combo"
    PIVOT_TABLE = "pivot_table"
    TEXT = "text"


class TileSize(str, Enum):
    """Predefined tile sizes for auto-layout."""

    FULL = "full"  # 12 cols
    HALF = "half"  # 6 cols
    THIRD = "third"  # 4 cols
    QUARTER = "quarter"  # 3 cols
    TWO_THIRDS = "two_thirds"  # 8 cols


TILE_SIZE_WIDTHS: dict[TileSize, int] = {
    TileSize.FULL: 12,
    TileSize.HALF: 6,
    TileSize.THIRD: 4,
    TileSize.QUARTER: 3,
    TileSize.TWO_THIRDS: 8,
}

# Default tile dimensions by chart type (width_cols, height_rows)
CHART_TYPE_DEFAULTS: dict[str, tuple[int, int]] = {
    "line": (6, 4),
    "bar": (6, 4),
    "area": (6, 4),
    "stacked_bar": (6, 4),
    "stacked_area": (6, 4),
    "grouped_bar": (6, 4),
    "scatter": (6, 4),
    "combo": (6, 4),
    "pie": (4, 4),
    "donut": (4, 4),
    "table": (12, 6),
    "pivot_table": (12, 6),
    "number": (3, 2),
    "funnel": (6, 5),
    "heatmap": (12, 6),
    "text": (12, 2),
}


class TilePosition(BaseModel):
    """Grid position of a tile on the dashboard canvas."""

    x: int = 0  # Column position (0-11)
    y: int = 0  # Row position
    w: int = 6  # Width in columns (1-12)
    h: int = 4  # Height in rows

    @field_validator("x")
    @classmethod
    def validate_x(cls, v: int) -> int:
        if not 0 <= v <= 11:
            raise ValueError(f"x must be 0-11, got {v}")
        return v

    @field_validator("w")
    @classmethod
    def validate_w(cls, v: int) -> int:
        if not 1 <= v <= 12:
            raise ValueError(f"w must be 1-12, got {v}")
        return v

    @field_validator("h")
    @classmethod
    def validate_h(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"h must be >= 1, got {v}")
        return v

    @model_validator(mode="after")
    def validate_bounds(self) -> TilePosition:
        if self.x + self.w > 12:
            raise ValueError(
                f"Tile extends beyond grid: x({self.x}) + w({self.w}) = {self.x + self.w} > 12"
            )
        return self


class SortSpec(BaseModel):
    """Sort specification for a query."""

    column_name: str
    sort_descending: bool = False


class FilterSpec(BaseModel):
    """Filter specification for a query."""

    field: str
    operator: str = "is"
    value: Any = None


class TileQuery(BaseModel):
    """Query definition for a dashboard tile."""

    table: str
    fields: list[str]
    sorts: list[SortSpec] = Field(default_factory=list)
    filters: list[FilterSpec] = Field(default_factory=list)
    limit: int = 200
    pivots: list[str] = Field(default_factory=list)

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("At least one field is required")
        return v


class TileVisConfig(BaseModel):
    """Visualization configuration for a tile."""

    # Core axis & display
    x_axis: str | None = None
    y_axis: list[str] = Field(default_factory=list)
    color_by: str | None = None
    series_colors: dict[str, str] = Field(default_factory=dict)
    show_labels: bool = True
    show_legend: bool = True
    show_grid: bool = True
    stacked: bool = False
    show_values: bool = False
    value_format: str | None = None
    axis_label_x: str | None = None
    axis_label_y: str | None = None
    custom: dict[str, Any] = Field(default_factory=dict)

    # Axis formatting
    x_axis_format: str | None = None  # Date format: "%-m/%-d/%-Y", "MMM-DD-YY"
    x_axis_rotation: int | None = None  # Label angle: 0, 45, 270
    y_axis_format: str | None = None  # Number format: "USDCURRENCY_0", "PERCENT_0"
    y2_axis: bool = False  # Enable secondary Y axis (combo charts)
    y2_axis_format: str | None = None  # Format for secondary axis

    # Series customization (for multi-series or combo charts)
    # Each entry: {"field": "table.col", "mark_type": "line"|"bar",
    #   "color": "#hex", "y_axis": "y"|"y2", "dash": [8,8],
    #   "data_label_format": "BIGNUMBER_2", "show_data_labels": True}
    series_config: list[dict[str, Any]] = Field(default_factory=list)

    # Trendlines & analytics
    show_trendline: bool = False
    trendline_type: str | None = None  # "linear", "moving_average"
    moving_average_window: int | None = None

    # Tooltips
    tooltip_fields: list[str] = Field(default_factory=list)

    # KPI-specific
    kpi_comparison_field: str | None = None  # Field for comparison (e.g., previous period)
    kpi_comparison_type: str | None = None  # "number_percent", "number", "percent"
    kpi_comparison_format: str | None = None  # "USDCURRENCY_0"
    kpi_label: str | None = None  # Override display label
    kpi_sparkline: bool = False  # Show sparkline in KPI tile
    kpi_sparkline_type: str | None = None  # "bar" or "line"

    # Markdown tile content
    markdown_template: str | None = None  # Raw markdown/HTML with Mustache syntax


class Tile(BaseModel):
    """A single tile (chart/table/number) on a dashboard."""

    name: str
    description: str = ""
    query: TileQuery
    chart_type: str = "line"
    vis_config: TileVisConfig = Field(default_factory=TileVisConfig)
    position: TilePosition | None = None
    size: str = "half"  # TileSize value for auto-layout

    @field_validator("chart_type")
    @classmethod
    def validate_chart_type(cls, v: str) -> str:
        valid = {ct.value for ct in ChartType}
        if v not in valid:
            raise ValueError(f"Invalid chart_type '{v}'. Must be one of: {sorted(valid)}")
        return v


class DashboardFilter(BaseModel):
    """A dashboard-level filter control."""

    field: str
    filter_type: str = "date_range"  # date_range, select, multi_select, text, number_range
    label: str = ""
    default_value: Any = None
    required: bool = False
    options: list[str] = Field(default_factory=list)


class TextTile(BaseModel):
    """A text/markdown tile on the dashboard."""

    content: str
    position: TilePosition | None = None
    size: str = "full"


class DashboardDefinition(BaseModel):
    """Complete dashboard-as-code definition.

    This is the central artifact that flows through the entire system:
    templates render into it, builders construct it, serializers convert
    it to/from Omni API payloads and YAML files.
    """

    name: str
    model_id: str = ""
    description: str = ""
    tiles: list[Tile] = Field(default_factory=list)
    text_tiles: list[TextTile] = Field(default_factory=list)
    filters: list[DashboardFilter] = Field(default_factory=list)
    refresh_interval: int = 3600  # seconds
    source_template: str | None = None
    dbt_model: str | None = None
    folder_id: str | None = None
    labels: list[str] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)

    @property
    def tile_count(self) -> int:
        return len(self.tiles) + len(self.text_tiles)

    def get_tile(self, name: str) -> Tile | None:
        """Find a tile by name."""
        for tile in self.tiles:
            if tile.name == name:
                return tile
        return None

    def all_fields(self) -> set[str]:
        """Get all unique field references across all tiles."""
        fields: set[str] = set()
        for tile in self.tiles:
            fields.update(tile.query.fields)
        return fields

    def all_tables(self) -> set[str]:
        """Get all unique table references."""
        return {tile.query.table for tile in self.tiles}
