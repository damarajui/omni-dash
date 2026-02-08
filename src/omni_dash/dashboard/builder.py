"""Fluent builder API for composing dashboard definitions."""

from __future__ import annotations

from typing import Any

from omni_dash.dashboard.definition import (
    DashboardDefinition,
    DashboardFilter,
    FilterSpec,
    SortSpec,
    TextTile,
    Tile,
    TileQuery,
    TileVisConfig,
)
from omni_dash.dashboard.layout import LayoutManager
from omni_dash.exceptions import DashboardDefinitionError


class DashboardBuilder:
    """Fluent API for building dashboard definitions step by step.

    Usage:
        dashboard = (
            DashboardBuilder("SEO Weekly Funnel")
            .model("abc-123")
            .dbt_source("mart_seo_weekly_funnel")
            .add_line_chart(
                "Organic Visits",
                time_col="week_start",
                metric_cols=["organic_visits_total", "total_web_visits"],
            )
            .add_number_tile("Current ARR", metric_col="running_organic_plg_arr")
            .add_filter("week_start", filter_type="date_range", default="last 12 weeks")
            .auto_layout()
            .build()
        )
    """

    def __init__(self, name: str):
        self._name = name
        self._model_id = ""
        self._description = ""
        self._dbt_model: str | None = None
        self._table: str | None = None
        self._tiles: list[Tile] = []
        self._text_tiles: list[TextTile] = []
        self._filters: list[DashboardFilter] = []
        self._refresh_interval = 3600
        self._source_template: str | None = None
        self._folder_id: str | None = None
        self._labels: list[str] = []
        self._meta: dict[str, Any] = {}

    def model(self, model_id: str) -> DashboardBuilder:
        """Set the Omni model ID."""
        self._model_id = model_id
        return self

    def description(self, desc: str) -> DashboardBuilder:
        """Set the dashboard description."""
        self._description = desc
        return self

    def dbt_source(self, model_name: str) -> DashboardBuilder:
        """Set the source dbt model name (also used as default table)."""
        self._dbt_model = model_name
        if not self._table:
            self._table = model_name
        return self

    def table(self, table_name: str) -> DashboardBuilder:
        """Set the Omni table/view name for queries."""
        self._table = table_name
        return self

    def folder(self, folder_id: str) -> DashboardBuilder:
        self._folder_id = folder_id
        return self

    def label(self, label: str) -> DashboardBuilder:
        self._labels.append(label)
        return self

    def refresh_interval(self, seconds: int) -> DashboardBuilder:
        self._refresh_interval = seconds
        return self

    def template(self, template_name: str) -> DashboardBuilder:
        self._source_template = template_name
        return self

    def _qualify_field(self, field: str) -> str:
        """Qualify a bare field name with the table name."""
        if "." in field:
            return field
        if self._table:
            return f"{self._table}.{field}"
        return field

    def _qualify_fields(self, fields: list[str]) -> list[str]:
        return [self._qualify_field(f) for f in fields]

    def add_tile(self, tile: Tile) -> DashboardBuilder:
        """Add a pre-built tile."""
        self._tiles.append(tile)
        return self

    def add_line_chart(
        self,
        name: str,
        *,
        time_col: str,
        metric_cols: list[str],
        sort_asc: bool = True,
        limit: int = 200,
        size: str = "half",
        description: str = "",
        show_labels: bool = True,
        stacked: bool = False,
        axis_title_y: str | None = None,
        date_format: str | None = None,
        label_rotation: int | None = None,
        value_format: str | None = None,
        series_config: list[dict[str, Any]] | None = None,
        tooltip_fields: list[str] | None = None,
    ) -> DashboardBuilder:
        """Add a line chart tile."""
        fields = [time_col] + metric_cols
        self._tiles.append(
            Tile(
                name=name,
                description=description,
                query=TileQuery(
                    table=self._table or "",
                    fields=self._qualify_fields(fields),
                    sorts=[SortSpec(column_name=self._qualify_field(time_col), sort_descending=not sort_asc)],
                    limit=limit,
                ),
                chart_type="line",
                vis_config=TileVisConfig(
                    x_axis=self._qualify_field(time_col),
                    y_axis=self._qualify_fields(metric_cols),
                    show_labels=show_labels,
                    stacked=stacked,
                    axis_label_y=axis_title_y,
                    x_axis_format=date_format,
                    x_axis_rotation=label_rotation,
                    y_axis_format=value_format,
                    series_config=series_config or [],
                    tooltip_fields=self._qualify_fields(tooltip_fields) if tooltip_fields else [],
                ),
                size=size,
            )
        )
        return self

    def add_area_chart(
        self,
        name: str,
        *,
        time_col: str,
        metric_cols: list[str],
        stacked: bool = True,
        size: str = "half",
        description: str = "",
        axis_title_y: str | None = None,
        date_format: str | None = None,
        value_format: str | None = None,
    ) -> DashboardBuilder:
        """Add an area chart tile."""
        fields = [time_col] + metric_cols
        self._tiles.append(
            Tile(
                name=name,
                description=description,
                query=TileQuery(
                    table=self._table or "",
                    fields=self._qualify_fields(fields),
                    sorts=[SortSpec(column_name=self._qualify_field(time_col), sort_descending=False)],
                ),
                chart_type="stacked_area" if stacked else "area",
                vis_config=TileVisConfig(
                    x_axis=self._qualify_field(time_col),
                    y_axis=self._qualify_fields(metric_cols),
                    stacked=stacked,
                    axis_label_y=axis_title_y,
                    x_axis_format=date_format,
                    y_axis_format=value_format,
                ),
                size=size,
            )
        )
        return self

    def add_bar_chart(
        self,
        name: str,
        *,
        dimension_col: str,
        metric_cols: list[str],
        sort_by: str | None = None,
        sort_desc: bool = True,
        stacked: bool = False,
        grouped: bool = False,
        limit: int = 50,
        size: str = "half",
        description: str = "",
        axis_title_y: str | None = None,
        value_format: str | None = None,
        label_rotation: int | None = None,
    ) -> DashboardBuilder:
        """Add a bar chart tile."""
        fields = [dimension_col] + metric_cols
        sort_col = sort_by or metric_cols[0]
        chart_type = "stacked_bar" if stacked else ("grouped_bar" if grouped else "bar")

        self._tiles.append(
            Tile(
                name=name,
                description=description,
                query=TileQuery(
                    table=self._table or "",
                    fields=self._qualify_fields(fields),
                    sorts=[SortSpec(column_name=self._qualify_field(sort_col), sort_descending=sort_desc)],
                    limit=limit,
                ),
                chart_type=chart_type,
                vis_config=TileVisConfig(
                    x_axis=self._qualify_field(dimension_col),
                    y_axis=self._qualify_fields(metric_cols),
                    stacked=stacked,
                    axis_label_y=axis_title_y,
                    y_axis_format=value_format,
                    x_axis_rotation=label_rotation,
                ),
                size=size,
            )
        )
        return self

    def add_table(
        self,
        name: str,
        *,
        columns: list[str],
        sort_by: str | None = None,
        sort_desc: bool = True,
        limit: int = 100,
        size: str = "full",
        description: str = "",
    ) -> DashboardBuilder:
        """Add a data table tile."""
        sorts = []
        if sort_by:
            sorts.append(SortSpec(column_name=self._qualify_field(sort_by), sort_descending=sort_desc))

        self._tiles.append(
            Tile(
                name=name,
                description=description,
                query=TileQuery(
                    table=self._table or "",
                    fields=self._qualify_fields(columns),
                    sorts=sorts,
                    limit=limit,
                ),
                chart_type="table",
                size=size,
            )
        )
        return self

    def add_number_tile(
        self,
        name: str,
        *,
        metric_col: str,
        filters: list[FilterSpec] | None = None,
        value_format: str | None = None,
        size: str = "quarter",
        description: str = "",
    ) -> DashboardBuilder:
        """Add a single-number KPI tile."""
        self._tiles.append(
            Tile(
                name=name,
                description=description,
                query=TileQuery(
                    table=self._table or "",
                    fields=[self._qualify_field(metric_col)],
                    filters=filters or [],
                    limit=1,
                ),
                chart_type="number",
                vis_config=TileVisConfig(
                    value_format=value_format,
                ),
                size=size,
            )
        )
        return self

    def add_pie_chart(
        self,
        name: str,
        *,
        dimension_col: str,
        metric_col: str,
        limit: int = 10,
        size: str = "third",
        description: str = "",
    ) -> DashboardBuilder:
        """Add a pie/donut chart tile."""
        self._tiles.append(
            Tile(
                name=name,
                description=description,
                query=TileQuery(
                    table=self._table or "",
                    fields=self._qualify_fields([dimension_col, metric_col]),
                    sorts=[SortSpec(column_name=self._qualify_field(metric_col), sort_descending=True)],
                    limit=limit,
                ),
                chart_type="pie",
                vis_config=TileVisConfig(
                    color_by=self._qualify_field(dimension_col),
                ),
                size=size,
            )
        )
        return self

    def add_scatter(
        self,
        name: str,
        *,
        x_col: str,
        y_col: str,
        color_by: str | None = None,
        size: str = "half",
        description: str = "",
    ) -> DashboardBuilder:
        """Add a scatter plot tile."""
        fields = [x_col, y_col]
        if color_by:
            fields.append(color_by)

        self._tiles.append(
            Tile(
                name=name,
                description=description,
                query=TileQuery(
                    table=self._table or "",
                    fields=self._qualify_fields(fields),
                ),
                chart_type="scatter",
                vis_config=TileVisConfig(
                    x_axis=self._qualify_field(x_col),
                    y_axis=[self._qualify_field(y_col)],
                    color_by=self._qualify_field(color_by) if color_by else None,
                ),
                size=size,
            )
        )
        return self

    def add_combo_chart(
        self,
        name: str,
        *,
        time_col: str,
        bar_cols: list[str],
        line_cols: list[str],
        sort_asc: bool = True,
        limit: int = 200,
        size: str = "half",
        description: str = "",
        y_format: str | None = None,
        y2_format: str | None = None,
        axis_title_y: str | None = None,
        tooltip_fields: list[str] | None = None,
    ) -> DashboardBuilder:
        """Add a dual-axis combo chart (bar + line on same tile)."""
        fields = [time_col] + bar_cols + line_cols

        # Build series config: bars on y, lines on y2
        series_cfg: list[dict[str, Any]] = []
        for col in bar_cols:
            series_cfg.append({
                "field": self._qualify_field(col),
                "mark_type": "bar",
                "y_axis": "y",
            })
        for col in line_cols:
            series_cfg.append({
                "field": self._qualify_field(col),
                "mark_type": "line",
                "y_axis": "y2",
            })

        self._tiles.append(
            Tile(
                name=name,
                description=description,
                query=TileQuery(
                    table=self._table or "",
                    fields=self._qualify_fields(fields),
                    sorts=[SortSpec(column_name=self._qualify_field(time_col), sort_descending=not sort_asc)],
                    limit=limit,
                ),
                chart_type="combo",
                vis_config=TileVisConfig(
                    x_axis=self._qualify_field(time_col),
                    y_axis=self._qualify_fields(bar_cols + line_cols),
                    axis_label_y=axis_title_y,
                    y_axis_format=y_format,
                    y2_axis=True,
                    y2_axis_format=y2_format,
                    series_config=series_cfg,
                    tooltip_fields=self._qualify_fields(tooltip_fields) if tooltip_fields else [],
                ),
                size=size,
            )
        )
        return self

    def add_markdown_tile(
        self,
        name: str,
        *,
        template: str,
        query_table: str | None = None,
        query_fields: list[str] | None = None,
        size: str = "full",
        description: str = "",
    ) -> DashboardBuilder:
        """Add a markdown/HTML tile with Mustache data binding.

        Use ``{{result._last.field.value}}`` syntax in the template for dynamic data.
        """
        table = query_table or self._table or ""
        fields = self._qualify_fields(query_fields) if query_fields else [f"{table}.id"]

        self._tiles.append(
            Tile(
                name=name,
                description=description,
                query=TileQuery(
                    table=table,
                    fields=fields,
                    limit=1,
                ),
                chart_type="text",
                vis_config=TileVisConfig(
                    markdown_template=template,
                ),
                size=size,
            )
        )
        return self

    def add_kpi_tile(
        self,
        name: str,
        *,
        metric_col: str,
        filters: list[FilterSpec] | None = None,
        value_format: str | None = None,
        label: str | None = None,
        comparison_col: str | None = None,
        comparison_type: str | None = None,
        comparison_format: str | None = None,
        sparkline: bool = False,
        sparkline_type: str | None = None,
        size: str = "quarter",
        description: str = "",
    ) -> DashboardBuilder:
        """Add a rich KPI tile with optional comparison and sparkline."""
        fields = [self._qualify_field(metric_col)]
        if comparison_col:
            fields.append(self._qualify_field(comparison_col))

        self._tiles.append(
            Tile(
                name=name,
                description=description,
                query=TileQuery(
                    table=self._table or "",
                    fields=fields,
                    filters=filters or [],
                    limit=1,
                ),
                chart_type="number",
                vis_config=TileVisConfig(
                    value_format=value_format,
                    kpi_label=label,
                    kpi_comparison_field=self._qualify_field(comparison_col) if comparison_col else None,
                    kpi_comparison_type=comparison_type,
                    kpi_comparison_format=comparison_format,
                    kpi_sparkline=sparkline,
                    kpi_sparkline_type=sparkline_type,
                ),
                size=size,
            )
        )
        return self

    def add_text(
        self,
        content: str,
        *,
        size: str = "full",
    ) -> DashboardBuilder:
        """Add a text/markdown tile."""
        self._text_tiles.append(TextTile(content=content, size=size))
        return self

    def add_filter(
        self,
        field: str,
        *,
        filter_type: str = "date_range",
        label: str = "",
        default: Any = None,
        required: bool = False,
        options: list[str] | None = None,
    ) -> DashboardBuilder:
        """Add a dashboard-level filter."""
        self._filters.append(
            DashboardFilter(
                field=self._qualify_field(field),
                filter_type=filter_type,
                label=label or field.split(".")[-1].replace("_", " ").title(),
                default_value=default,
                required=required,
                options=options or [],
            )
        )
        return self

    def auto_layout(self) -> DashboardBuilder:
        """Auto-position all tiles using the grid layout manager."""
        self._tiles = LayoutManager.auto_position(self._tiles)
        return self

    def build(self) -> DashboardDefinition:
        """Build and validate the final dashboard definition."""
        if not self._tiles and not self._text_tiles:
            raise DashboardDefinitionError("Dashboard must have at least one tile")

        # Ensure all tiles have table set
        for tile in self._tiles:
            if not tile.query.table and self._table:
                tile.query.table = self._table

        return DashboardDefinition(
            name=self._name,
            model_id=self._model_id,
            description=self._description,
            tiles=self._tiles,
            text_tiles=self._text_tiles,
            filters=self._filters,
            refresh_interval=self._refresh_interval,
            source_template=self._source_template,
            dbt_model=self._dbt_model,
            folder_id=self._folder_id,
            labels=self._labels,
            meta=self._meta,
        )
