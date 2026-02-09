"""MCP server exposing omni-dash tools for Claude Code.

Tools:
  - list_dashboards: List dashboards in the Omni org
  - get_dashboard: Get dashboard details by ID
  - create_dashboard: Create a new dashboard from a spec
  - delete_dashboard: Delete a dashboard by ID
  - export_dashboard: Export a dashboard's full definition
  - list_topics: List available Omni model topics (data tables)
  - get_topic_fields: Get fields/columns for a topic
  - query_data: Run a query against Omni and return results
  - list_folders: List folders in the Omni org

Run:
    uv run python -m omni_dash.mcp
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Load .env from the omni-dash project root (handles MCP subprocess CWD issues)
_project_root = Path(__file__).resolve().parents[3]
load_dotenv(_project_root / ".env", override=False)

from omni_dash.api.client import OmniClient
from omni_dash.api.documents import DocumentService
from omni_dash.api.models import ModelService
from omni_dash.api.queries import QueryBuilder, QueryRunner
from omni_dash.config import get_settings
from omni_dash.dashboard.builder import DashboardBuilder
from omni_dash.dashboard.definition import DashboardDefinition
from omni_dash.dashboard.layout import LayoutManager
from omni_dash.dashboard.serializer import DashboardSerializer
from omni_dash.exceptions import OmniDashError

logger = logging.getLogger(__name__)

mcp = FastMCP(
    "omni-dash",
    instructions=(
        "Omni BI dashboard SDK. Use these tools to explore data, "
        "create dashboards, and manage them in Omni. "
        "Start with list_topics to discover available data, then "
        "get_topic_fields to see columns, optionally query_data to "
        "preview, and finally create_dashboard to build."
    ),
)

# ---------------------------------------------------------------------------
# Lazy-initialized shared clients
# ---------------------------------------------------------------------------
_client: OmniClient | None = None
_doc_svc: DocumentService | None = None
_model_svc: ModelService | None = None
_query_runner: QueryRunner | None = None


def _get_client() -> OmniClient:
    global _client
    if _client is None:
        _client = OmniClient()
    return _client


def _get_doc_svc() -> DocumentService:
    global _doc_svc
    if _doc_svc is None:
        _doc_svc = DocumentService(_get_client())
    return _doc_svc


def _get_model_svc() -> ModelService:
    global _model_svc
    if _model_svc is None:
        _model_svc = ModelService(_get_client())
    return _model_svc


def _get_query_runner() -> QueryRunner:
    global _query_runner
    if _query_runner is None:
        _query_runner = QueryRunner(_get_client())
    return _query_runner


def _get_shared_model_id() -> str:
    """Get the shared model ID from settings or discover it."""
    settings = get_settings()
    model_id = getattr(settings, "omni_shared_model_id", "")
    if model_id:
        return model_id
    # Fall back to finding a shared model
    models = _get_model_svc().list_models()
    for m in models:
        if m.model_kind == "shared":
            return m.id
    if models:
        return models[0].id
    return ""


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def list_dashboards(folder_id: str | None = None) -> str:
    """List dashboards in the Omni org.

    Args:
        folder_id: Optional folder ID to filter by.

    Returns:
        JSON array of dashboards with id, name, document_type, folder_id.
    """
    try:
        docs = _get_doc_svc().list_dashboards(folder_id=folder_id)
        return json.dumps(
            [
                {
                    "id": d.id,
                    "name": d.name,
                    "type": d.document_type,
                    "folder_id": d.folder_id,
                    "updated_at": d.updated_at,
                }
                for d in docs
            ],
            indent=2,
        )
    except OmniDashError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_dashboard(dashboard_id: str) -> str:
    """Get a dashboard's details by ID.

    Args:
        dashboard_id: The dashboard identifier (short hex ID from URL).

    Returns:
        JSON with dashboard metadata including tiles, layouts.
    """
    try:
        dash = _get_doc_svc().get_dashboard(dashboard_id)
        return json.dumps(
            {
                "document_id": dash.document_id,
                "name": dash.name,
                "model_id": dash.model_id,
                "tile_count": len(dash.query_presentations),
                "query_presentations": dash.query_presentations[:5],  # preview
                "created_at": dash.created_at,
                "updated_at": dash.updated_at,
            },
            indent=2,
        )
    except OmniDashError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def create_dashboard(
    name: str,
    tiles: list[dict[str, Any]],
    description: str = "",
    folder_id: str = "",
    filters: list[dict[str, Any]] | None = None,
    model_id: str = "",
) -> str:
    """Create a new Omni dashboard from a tile specification.

    Each tile needs: name, chart_type, query (table + fields), and vis_config.

    Supported chart_types: line, bar, area, scatter, pie, table, number,
    combo, text, stacked_bar, grouped_bar, stacked_area, heatmap, vegalite.

    Args:
        name: Dashboard display name.
        tiles: Array of tile specs. Each tile:
            - name (str): Tile title
            - chart_type (str): One of the supported chart types
            - query (dict): {table, fields[], sorts[], filters[], limit}
              Fields MUST be qualified: "table_name.column_name"
            - vis_config (dict): Chart config (x_axis, y_axis[], value_format,
              series_config[], reference_lines[], color_values, etc.)
            - size (str): "full"|"half"|"third"|"quarter"|"two_thirds"
        description: Dashboard description.
        folder_id: Omni folder ID to create in.
        filters: Dashboard-level filters [{field, filter_type, label, default_value}].
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON with created dashboard URL and ID.
    """
    try:
        resolved_model_id = model_id or _get_shared_model_id()
        if not resolved_model_id:
            return json.dumps({
                "error": "No model_id provided and could not auto-discover one. "
                "Set OMNI_SHARED_MODEL_ID in .env or pass model_id."
            })

        definition = DashboardDefinition(
            name=name,
            model_id=resolved_model_id,
            description=description,
            tiles=tiles,
            filters=filters or [],
            folder_id=folder_id or None,
        )

        # Auto-position tiles on grid
        definition.tiles = LayoutManager.auto_position(definition.tiles)

        # Serialize to Omni API format
        payload = DashboardSerializer.to_omni_create_payload(definition)

        # Push to Omni
        result = _get_doc_svc().create_dashboard(
            payload, folder_id=folder_id or None
        )

        settings = get_settings()
        base_url = settings.omni_base_url.rstrip("/")
        url = f"{base_url}/dashboards/{result.document_id}"

        return json.dumps({
            "status": "created",
            "dashboard_id": result.document_id,
            "name": result.name,
            "url": url,
            "tile_count": len(tiles),
        })
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": f"Validation error: {e}"})


@mcp.tool()
def delete_dashboard(dashboard_id: str) -> str:
    """Delete a dashboard by ID.

    Args:
        dashboard_id: The dashboard identifier.

    Returns:
        JSON confirmation.
    """
    try:
        _get_doc_svc().delete_dashboard(dashboard_id)
        return json.dumps({"status": "deleted", "dashboard_id": dashboard_id})
    except OmniDashError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def export_dashboard(dashboard_id: str) -> str:
    """Export a dashboard's full definition (for backup or cloning).

    Args:
        dashboard_id: The dashboard identifier.

    Returns:
        JSON export of the complete dashboard structure.
    """
    try:
        export = _get_doc_svc().export_dashboard(dashboard_id)
        return json.dumps(export, indent=2)
    except OmniDashError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_topics(model_id: str = "") -> str:
    """List available data topics (tables/views) in an Omni model.

    Topics are the queryable entities — each corresponds to a dbt model
    or database table. Use this to discover what data is available.

    Args:
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON array of topics with name, label, description.
    """
    try:
        resolved_model_id = model_id or _get_shared_model_id()
        if not resolved_model_id:
            return json.dumps({"error": "No model_id found. Set OMNI_SHARED_MODEL_ID."})

        topics = _get_model_svc().list_topics(resolved_model_id)
        return json.dumps(
            [
                {
                    "name": t.name,
                    "label": t.label,
                    "description": t.description,
                }
                for t in topics
            ],
            indent=2,
        )
    except OmniDashError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_topic_fields(topic_name: str, model_id: str = "") -> str:
    """Get all fields (columns) for a specific topic.

    Use this to see what fields are available for building charts and queries.
    Field names must be qualified as "topic_name.field_name" in queries.

    Args:
        topic_name: The topic name (from list_topics).
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON with topic details and fields array.
    """
    try:
        resolved_model_id = model_id or _get_shared_model_id()
        if not resolved_model_id:
            return json.dumps({"error": "No model_id found. Set OMNI_SHARED_MODEL_ID."})

        detail = _get_model_svc().get_topic(resolved_model_id, topic_name)
        return json.dumps(
            {
                "name": detail.name,
                "label": detail.label,
                "description": detail.description,
                "fields": detail.fields,
                "views": detail.views,
            },
            indent=2,
        )
    except OmniDashError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def query_data(
    table: str,
    fields: list[str],
    sorts: list[dict[str, Any]] | None = None,
    filters: dict[str, Any] | None = None,
    limit: int = 25,
    model_id: str = "",
) -> str:
    """Run a query against Omni and return data rows.

    Use this to preview data before building a dashboard, or to check
    what values exist in a column.

    Args:
        table: Topic/table name (e.g., "mart_seo_weekly_funnel").
        fields: Qualified field names (e.g., ["mart_seo.week_start", "mart_seo.visits"]).
        sorts: Sort specs [{columnName, sortDescending}].
        filters: Filter map {qualified_field: {operator, value}}.
        limit: Max rows to return (default 25, max 1000).
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON with fields and data rows.
    """
    try:
        resolved_model_id = model_id or _get_shared_model_id()
        if not resolved_model_id:
            return json.dumps({"error": "No model_id found. Set OMNI_SHARED_MODEL_ID."})

        limit = min(limit, 1000)

        builder = QueryBuilder(resolved_model_id, table)
        builder._fields = fields  # Already qualified
        builder._limit = limit
        if sorts:
            builder._sorts = sorts
        if filters:
            builder._filters = filters

        spec = builder.build()
        result = _get_query_runner().run(spec)

        return json.dumps(
            {
                "fields": result.fields,
                "rows": result.rows[:limit],
                "row_count": result.row_count,
                "truncated": result.truncated,
            },
            indent=2,
            default=str,
        )
    except OmniDashError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_folders() -> str:
    """List all folders in the Omni org.

    Returns:
        JSON array of folders with id, name.
    """
    try:
        folders = _get_doc_svc().list_folders()
        return json.dumps(
            [
                {
                    "id": f.get("id", ""),
                    "name": f.get("name", ""),
                    "parent_id": f.get("parentId"),
                }
                for f in folders
            ],
            indent=2,
        )
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
