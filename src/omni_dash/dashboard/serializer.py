"""Serialize DashboardDefinition to/from Omni API payloads and YAML files."""

from __future__ import annotations

from typing import Any

import yaml

from omni_dash.dashboard.definition import (
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


class DashboardSerializer:
    """Convert DashboardDefinition to/from various formats."""

    @staticmethod
    def to_omni_create_payload(definition: DashboardDefinition) -> dict[str, Any]:
        """Convert a DashboardDefinition to the Omni POST /api/v1/documents payload.

        This is the primary serialization target for creating dashboards via API.
        """
        if not definition.model_id:
            raise DashboardDefinitionError(
                "model_id is required for Omni API payload. "
                "Set it via DashboardBuilder.model() or in the definition."
            )

        query_presentations = []
        for tile in definition.tiles:
            qp: dict[str, Any] = {
                "name": tile.name,
                "description": tile.description,
                "query": {
                    "table": tile.query.table,
                    "fields": tile.query.fields,
                    "modelId": definition.model_id,
                    "limit": tile.query.limit,
                },
            }

            # Sorts
            if tile.query.sorts:
                qp["query"]["sorts"] = [
                    {
                        "columnName": s.column_name,
                        "sortDescending": s.sort_descending,
                    }
                    for s in tile.query.sorts
                ]

            # Filters
            if tile.query.filters:
                qp["query"]["filters"] = {
                    f.field: {"operator": f.operator, "value": f.value}
                    for f in tile.query.filters
                }

            # Pivots
            if tile.query.pivots:
                qp["query"]["pivots"] = tile.query.pivots

            # Chart type
            qp["chartType"] = tile.chart_type
            qp["prefersChart"] = tile.chart_type != "table"

            # Visualization config
            vis: dict[str, Any] = {"visType": "basic", "config": {}}
            if tile.vis_config.x_axis:
                vis["config"]["xAxis"] = tile.vis_config.x_axis
            if tile.vis_config.y_axis:
                vis["config"]["yAxis"] = tile.vis_config.y_axis
            if tile.vis_config.color_by:
                vis["config"]["colorBy"] = tile.vis_config.color_by
            if tile.vis_config.stacked:
                vis["config"]["stacked"] = True
            if not tile.vis_config.show_labels:
                vis["config"]["showLabels"] = False
            if not tile.vis_config.show_legend:
                vis["config"]["showLegend"] = False
            if not tile.vis_config.show_grid:
                vis["config"]["showGrid"] = False
            if tile.vis_config.show_values:
                vis["config"]["showValues"] = True
            if tile.vis_config.value_format:
                vis["config"]["valueFormat"] = tile.vis_config.value_format
            if tile.vis_config.axis_label_x:
                vis["config"]["axisLabelX"] = tile.vis_config.axis_label_x
            if tile.vis_config.axis_label_y:
                vis["config"]["axisLabelY"] = tile.vis_config.axis_label_y
            if tile.vis_config.series_colors:
                vis["config"]["seriesColors"] = tile.vis_config.series_colors
            if tile.vis_config.custom:
                vis["config"].update(tile.vis_config.custom)

            qp["visualization"] = vis

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
        """
        doc = export_data.get("document", {})
        dash = export_data.get("dashboard", {})

        tiles = []
        for qp in dash.get("queryPresentations", []):
            query = qp.get("query", {})
            vis = qp.get("visualization", {}).get("config", {})

            sorts = [
                SortSpec(
                    column_name=s.get("columnName", ""),
                    sort_descending=s.get("sortDescending", False),
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
                    chart_type=qp.get("chartType", "line"),
                    vis_config=TileVisConfig(
                        x_axis=vis.get("xAxis"),
                        y_axis=vis.get("yAxis", []),
                        color_by=vis.get("colorBy"),
                        stacked=vis.get("stacked", False),
                        show_values=vis.get("showValues", False),
                        series_colors=vis.get("seriesColors", {}),
                    ),
                )
            )

        return DashboardDefinition(
            name=doc.get("name", ""),
            model_id=doc.get("modelId", ""),
            description=doc.get("description", ""),
            tiles=tiles,
        )
