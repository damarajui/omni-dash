"""MCP server exposing omni-dash tools for Claude Code.

Tools:
  - list_dashboards: List dashboards in the Omni org
  - get_dashboard: Get dashboard details by ID
  - create_dashboard: Create a new dashboard from a spec
  - update_dashboard: Update an existing dashboard (replace tiles or metadata)
  - add_tiles_to_dashboard: Append tiles to an existing dashboard
  - update_tile: Update a single tile in-place (SQL, fields, filters, chart type)
  - delete_dashboard: Delete a dashboard by ID
  - clone_dashboard: Clone a dashboard with a new name
  - move_dashboard: Move a dashboard to a different folder
  - import_dashboard: Import a dashboard from an export payload
  - export_dashboard: Export a dashboard's full definition
  - list_topics: List available Omni model topics (data tables)
  - get_topic_fields: Get fields/columns for a topic
  - query_data: Run a query against Omni and return results
  - list_folders: List folders in the Omni org
  - suggest_chart: AI-powered chart type recommendation
  - validate_dashboard: Pre-flight validation for dashboard specs
  - profile_data: Data profiling and field statistics
  - generate_dashboard: NL-to-dashboard generation via AI

Run:
    uv run python -m omni_dash.mcp
"""

from __future__ import annotations

import json
import logging
import re
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
_ai_svc: Any = None  # OmniAIService, lazy-loaded
_shared_model_id: str = ""  # Cached model ID to avoid repeated API calls


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


def _get_ai_svc():
    """Lazy-init the Omni AI service."""
    global _ai_svc
    if _ai_svc is None:
        from omni_dash.api.ai import OmniAIService

        _ai_svc = OmniAIService(_get_client())
    return _ai_svc


def _get_shared_model_id() -> str:
    """Get the shared model ID from settings or discover it.

    Result is cached to avoid hitting the Omni API on every tool call.
    """
    global _shared_model_id
    if _shared_model_id:
        return _shared_model_id
    settings = get_settings()
    model_id = settings.omni_shared_model_id
    if model_id:
        _shared_model_id = model_id
        return model_id
    # Fall back to finding a shared model
    models = _get_model_svc().list_models()
    for m in models:
        if m.model_kind == "shared":
            _shared_model_id = m.id
            return m.id
    if models:
        _shared_model_id = models[0].id
        return models[0].id
    return ""


def _resolve_table_name(table: str, model_id: str) -> str:
    """Resolve a topic name to its base_view name for querying.

    Omni's query API requires underscored names (no spaces). If the table
    name contains spaces (e.g., "Google Ads Performance"), look up the
    topic to find its base_view.
    """
    if " " not in table:
        return table
    try:
        detail = _get_model_svc().get_topic_native(model_id, table)
        if detail.base_view:
            return detail.base_view
    except Exception:
        pass
    return table


def _resolve_model_id_from_export(
    export_data: dict[str, Any], user_model_id: str = ""
) -> str:
    """Resolve the base model ID from an export payload or user override."""
    if user_model_id:
        return user_model_id
    doc = export_data.get("document", {})
    model_id = doc.get("sharedModelId", "")
    if model_id:
        return model_id
    wb = export_data.get("workbookModel", {})
    model_id = wb.get("base_model_id", "")
    if model_id:
        return model_id
    dash = export_data.get("dashboard", {})
    model_id = dash.get("model", {}).get("baseModelId", "")
    if model_id:
        return model_id
    return _get_shared_model_id()


def _build_dashboard_url(dashboard_id: str) -> str:
    """Build the full Omni dashboard URL."""
    settings = get_settings()
    base_url = settings.omni_base_url.rstrip("/")
    return f"{base_url}/dashboards/{dashboard_id}"


def _validate_tile_fields(
    tiles: list[dict[str, Any]], model_id: str
) -> list[str]:
    """Check that all field references in tiles exist in the Omni model.

    Returns a list of error strings (empty if all fields are valid).
    """
    errors: list[str] = []
    try:
        model_svc = _get_model_svc()
        tables = {
            t.get("query", {}).get("table", "")
            for t in tiles
            if t.get("query", {}).get("table")
        }
        available: dict[str, set[str]] = {}
        for tbl in tables:
            try:
                detail = model_svc.get_topic_native(model_id, tbl)
                qualified = set()
                for f in detail.fields:
                    fname = f.get("name", "")
                    qualified.add(fname)
                    qualified.add(f"{tbl}.{fname}")
                    # Also accept base_view-qualified names (PR #41 changed
                    # field qualification to use base_view instead of topic name)
                    qname = f.get("qualified_name", "")
                    if qname:
                        qualified.add(qname)
                if detail.base_view and detail.base_view != tbl:
                    # Also accept table reference via base_view name
                    available[detail.base_view] = qualified
                available[tbl] = qualified
            except Exception:
                pass  # Table not found — will surface as field errors

        for t in tiles:
            tile_name = t.get("name", "Untitled")
            q = t.get("query", {})
            tbl = q.get("table", "")
            if tbl not in available:
                continue  # Can't validate without topic metadata
            valid_fields = available[tbl]
            for field in q.get("fields", []):
                if field not in valid_fields:
                    errors.append(
                        f"Tile '{tile_name}': field '{field}' not found "
                        f"in topic '{tbl}'. Check get_topic_fields for "
                        f"valid field names."
                    )
    except Exception:
        pass  # Don't block creation if validation itself fails
    return errors


def _create_via_import_fallback(
    doc_svc: DocumentService,
    payload: dict[str, Any],
    *,
    name: str,
    folder_id: str | None,
) -> str:
    """Create a dashboard via import when the create endpoint fails.

    Converts a create-format payload into an import-format payload and
    calls the import endpoint instead.

    Returns:
        The new dashboard ID.
    """
    model_id = payload.get("modelId", "")
    memberships = []
    # Build tile layouts (half-width, 2 per row, 51px tall)
    layouts: list[dict[str, Any]] = []
    for idx, qp in enumerate(payload.get("queryPresentations", []), start=1):
        memberships.append({
            "queryPresentation": {
                "name": qp.get("name", ""),
                "query": {"queryJson": qp.get("query", {})},
                "visConfig": qp.get("visConfig", {}),
                "isSql": qp.get("isSql", False),
                "queryIdentifierMapKey": qp.get("queryIdentifierMapKey", str(idx)),
                "type": "query",
                "prefersChart": qp.get("prefersChart", True),
                "filterOrder": [],
                "resultConfig": {},
                "aiConfig": {},
            }
        })
        pos = qp.get("position", {})
        layouts.append({
            "i": str(idx),
            "x": pos.get("x", 0),
            "y": pos.get("y", (idx - 1) * 6),
            "w": pos.get("w", 12),
            "h": pos.get("h", 6),
        })

    export_payload: dict[str, Any] = {
        "exportVersion": "0.1",
        "document": {
            "name": name or payload.get("name", "Untitled"),
            "modelId": model_id,
        },
        "dashboard": {
            "metadataVersion": 2,
            "ephemeral": ",".join(f"{i}:" for i in range(1, len(memberships) + 1)),
            "crossfilterEnabled": False,
            "facetFilters": False,
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": memberships,
                "filterConfig": payload.get("filterConfig", {}),
                "filterConfigVersion": 0,
                "filterOrder": payload.get("filterOrder", []),
            },
            "metadata": {
                "layouts": {"lg": layouts},
                "textTiles": [],
                "hiddenTiles": [],
                "tileSettings": {},
                "tileFilterMap": {},
                "tileControlMap": {},
            },
        },
        "workbookModel": {"base_model_id": model_id},
    }
    if folder_id:
        export_payload["document"]["folderId"] = folder_id

    result = doc_svc.import_dashboard(
        export_payload,
        base_model_id=model_id,
        name=name,
        folder_id=folder_id,
    )
    return result.document_id


def _create_with_vis_configs(
    payload: dict[str, Any],
    *,
    name: str,
    folder_id: str | None,
) -> tuple[str, str]:
    """Create a dashboard, then patch vis configs via export→reimport.

    Omni's create endpoint ignores visConfig in queryPresentations,
    so we: (1) strip vis configs, (2) create skeleton, (3) export,
    (4) inject vis configs with stale jsonHash removed, (5) reimport,
    (6) delete skeleton.

    Returns:
        (dashboard_id, dashboard_name) of the final dashboard.
    """
    import copy

    # Extract vis configs and query overrides before sending.
    # Deep-copy queryPresentations to avoid mutating the caller's payload.
    qps = copy.deepcopy(payload.get("queryPresentations", []))
    payload["queryPresentations"] = qps
    vis_configs_by_name: dict[str, dict] = {}
    query_overrides_by_name: dict[str, dict] = {}
    for qp in qps:
        vc = qp.pop("visConfig", None)
        if vc and vc.get("visType"):
            vis_configs_by_name[qp["name"]] = vc
        # Preserve query fields that Omni may drop during export
        q = qp.get("query", {})
        overrides: dict[str, Any] = {}
        if q.get("filters"):
            overrides["filters"] = q["filters"]
        if q.get("pivots"):
            overrides["pivots"] = q["pivots"]
        if q.get("sorts"):
            overrides["sorts"] = q["sorts"]
        if overrides:
            query_overrides_by_name[qp["name"]] = overrides

    doc_svc = _get_doc_svc()
    try:
        result = doc_svc.create_dashboard(payload, folder_id=folder_id)
        skeleton_id = result.document_id
    except OmniDashError as create_err:
        # Fallback: if the create endpoint fails (400/404 in some Omni
        # envs), create via import instead.
        err_str = str(create_err).lower()
        if any(tok in err_str for tok in ("404", "not found", "400", "bad request")):
            logger.warning(
                "create_dashboard failed (%s), falling back to import",
                create_err,
            )
            skeleton_id = _create_via_import_fallback(
                doc_svc, payload, name=name, folder_id=folder_id,
            )
        else:
            raise

    if not vis_configs_by_name:
        return skeleton_id, name or "Untitled"

    # Patch vis configs via export→reimport
    export_data = doc_svc.export_dashboard(skeleton_id)
    patched = copy.deepcopy(export_data)
    dash = patched.get("dashboard", {})
    qpc = dash.get("queryPresentationCollection", {})
    for membership in qpc.get("queryPresentationCollectionMemberships", []):
        qp_data = membership.get("queryPresentation", {})
        tile_name = qp_data.get("name", "")
        if tile_name in vis_configs_by_name:
            vc_patch = vis_configs_by_name[tile_name]
            existing_vc = qp_data.setdefault("visConfig", {})
            existing_vc["visType"] = vc_patch.get("visType")
            # chartType can be explicitly null in exports; fall back to
            # visType mapping, then to "line".
            patched_ct = vc_patch.get("chartType") or existing_vc.get("chartType")
            if not patched_ct:
                _CT_FALLBACK = {
                    "omni-kpi": "kpi",
                    "omni-table": "table",
                    "omni-markdown": "markdown",
                }
                patched_ct = _CT_FALLBACK.get(
                    vc_patch.get("visType", ""), "line"
                )
            existing_vc["chartType"] = patched_ct
            if "spec" in vc_patch:
                existing_vc["spec"] = vc_patch["spec"]
                # Omni reads "config" but our serializer writes "spec" —
                # copy to both so the reimport picks it up.
                existing_vc["config"] = vc_patch["spec"]
            if "config" in vc_patch:
                existing_vc["config"] = vc_patch["config"]
            if vc_patch.get("fields"):
                existing_vc["fields"] = vc_patch["fields"]
            existing_vc.pop("jsonHash", None)

        # Patch query data (filters, pivots, sorts) into queryJson.
        # Export stores query under query.queryJson, not query directly.
        # Always apply overrides — Omni's create endpoint may write
        # placeholder values that differ from the user's intended config.
        if tile_name in query_overrides_by_name:
            q_overrides = query_overrides_by_name[tile_name]
            q_json = qp_data.get("query", {}).get("queryJson", {})
            if q_json:
                for key in ("filters", "pivots", "sorts"):
                    if key in q_overrides:
                        q_json[key] = q_overrides[key]

    # Inject dashboard-level filters from original payload into the export.
    # Omni's create endpoint ignores filterConfig, so we carry it through.
    orig_fc = payload.get("filterConfig", {})
    orig_fo = payload.get("filterOrder", [])
    if orig_fc:
        qpc.setdefault("filterConfig", {}).update(orig_fc)
    if orig_fo:
        existing_fo = qpc.get("filterOrder", [])
        qpc["filterOrder"] = existing_fo + [
            f for f in orig_fo if f not in existing_fo
        ]

    reimport_model_id = _resolve_model_id_from_export(patched)
    reimport_result = doc_svc.import_dashboard(
        patched,
        base_model_id=reimport_model_id,
        name=name,
        folder_id=folder_id,
    )

    # Guard: if reimport returned empty ID, preserve skeleton for recovery
    if not reimport_result.document_id:
        logger.error(
            "Reimport returned empty document_id; skeleton %s preserved",
            skeleton_id,
        )
        raise OmniDashError(
            "Import succeeded but returned no document_id. "
            f"Skeleton dashboard {skeleton_id} preserved for manual recovery."
        )

    # Delete the skeleton (best-effort)
    try:
        doc_svc.delete_dashboard(skeleton_id)
    except Exception as e:
        logger.warning("Failed to delete skeleton %s: %s", skeleton_id, e)

    return reimport_result.document_id, reimport_result.name


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
    except Exception as e:
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
        total_tiles = len(dash.query_presentations)
        preview_limit = 10
        result_data: dict[str, Any] = {
            "document_id": dash.document_id,
            "name": dash.name,
            "model_id": dash.model_id,
            "tile_count": total_tiles,
            "query_presentations": dash.query_presentations[:preview_limit],
            "created_at": dash.created_at,
            "updated_at": dash.updated_at,
        }
        if total_tiles > preview_limit:
            result_data["note"] = (
                f"Showing first {preview_limit} of {total_tiles} tiles. "
                "Use export_dashboard to see all tiles."
            )
        return json.dumps(result_data, indent=2)
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
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
        if not tiles:
            return json.dumps({"error": "tiles array cannot be empty."})

        resolved_model_id = model_id or _get_shared_model_id()
        if not resolved_model_id:
            return json.dumps({
                "error": "No model_id provided and could not auto-discover one. "
                "Set OMNI_SHARED_MODEL_ID in .env or pass model_id."
            })

        # Auto-validate field references before creating
        field_errors = _validate_tile_fields(tiles, resolved_model_id)
        if field_errors:
            return json.dumps({
                "error": "Invalid field references — dashboard NOT created.",
                "field_errors": field_errors,
                "hint": "Use get_topic_fields to see valid field names.",
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

        # Create with vis config patching (Omni ignores vis configs on create)
        dash_id, dash_name = _create_with_vis_configs(
            payload, name=name, folder_id=folder_id or None,
        )

        return json.dumps({
            "status": "created",
            "dashboard_id": dash_id,
            "name": dash_name,
            "url": _build_dashboard_url(dash_id),
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
    except Exception as e:
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
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def import_dashboard(
    export_data: dict[str, Any],
    name: str | None = None,
    folder_id: str | None = None,
    model_id: str = "",
) -> str:
    """Import a dashboard from an export payload (from export_dashboard or backup).

    Args:
        export_data: The full export payload (from export_dashboard).
        name: Optional name override for the imported dashboard.
        folder_id: Optional folder ID to import into.
        model_id: Omni model ID. Auto-resolved from export if omitted.

    Returns:
        JSON with imported dashboard ID and URL.
    """
    try:
        resolved_model_id = _resolve_model_id_from_export(export_data, model_id)
        if not resolved_model_id:
            return json.dumps({"error": "Could not resolve model_id from export data."})

        result = _get_doc_svc().import_dashboard(
            export_data,
            base_model_id=resolved_model_id,
            name=name,
            folder_id=folder_id,
        )
        return json.dumps({
            "status": "imported",
            "dashboard_id": result.document_id,
            "name": result.name,
            "url": _build_dashboard_url(result.document_id),
        })
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": f"Import error: {e}"})


@mcp.tool()
def clone_dashboard(
    dashboard_id: str,
    new_name: str,
    folder_id: str | None = None,
    model_id: str = "",
) -> str:
    """Clone an existing dashboard with a new name, optionally into a different folder.

    Args:
        dashboard_id: The source dashboard ID to clone.
        new_name: Name for the cloned dashboard.
        folder_id: Optional folder ID for the clone. Defaults to same folder.
        model_id: Omni model ID. Auto-resolved from export if omitted.

    Returns:
        JSON with source and new dashboard IDs.
    """
    try:
        export_data = _get_doc_svc().export_dashboard(dashboard_id)
        resolved_model_id = _resolve_model_id_from_export(export_data, model_id)

        result = _get_doc_svc().import_dashboard(
            export_data,
            base_model_id=resolved_model_id,
            name=new_name,
            folder_id=folder_id,
        )
        return json.dumps({
            "status": "cloned",
            "source_dashboard_id": dashboard_id,
            "new_dashboard_id": result.document_id,
            "name": result.name,
            "url": _build_dashboard_url(result.document_id),
        })
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def move_dashboard(
    dashboard_id: str,
    target_folder_id: str,
    model_id: str = "",
) -> str:
    """Move a dashboard to a different folder.

    Since Omni has no native move API, this exports the dashboard,
    re-imports it to the target folder, and deletes the original.

    Args:
        dashboard_id: The dashboard to move.
        target_folder_id: The destination folder ID.
        model_id: Omni model ID. Auto-resolved from export if omitted.

    Returns:
        JSON with old and new dashboard IDs.
    """
    try:
        export_data = _get_doc_svc().export_dashboard(dashboard_id)
        original_name = export_data.get("document", {}).get("name", "")
        resolved_model_id = _resolve_model_id_from_export(export_data, model_id)

        result = _get_doc_svc().import_dashboard(
            export_data,
            base_model_id=resolved_model_id,
            name=original_name,
            folder_id=target_folder_id,
        )

        try:
            _get_doc_svc().delete_dashboard(dashboard_id)
        except Exception as del_err:
            return json.dumps({
                "status": "partial",
                "warning": f"Dashboard copied but original not deleted: {del_err}",
                "old_dashboard_id": dashboard_id,
                "new_dashboard_id": result.document_id,
                "url": _build_dashboard_url(result.document_id),
            })

        return json.dumps({
            "status": "moved",
            "old_dashboard_id": dashboard_id,
            "new_dashboard_id": result.document_id,
            "target_folder_id": target_folder_id,
            "url": _build_dashboard_url(result.document_id),
        })
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def update_dashboard(
    dashboard_id: str,
    name: str | None = None,
    tiles: list[dict[str, Any]] | None = None,
    description: str = "",
    filters: list[dict[str, Any]] | None = None,
    folder_id: str | None = None,
    model_id: str = "",
) -> str:
    """Update an existing dashboard by replacing its content.

    If tiles are provided, the dashboard is rebuilt with the new tile spec
    (same format as create_dashboard). If tiles are omitted, only metadata
    (name, folder) is updated. The original dashboard is deleted after the
    new one is created.

    Args:
        dashboard_id: The dashboard to update.
        name: New dashboard name (keeps original if omitted).
        tiles: New tile specs (same format as create_dashboard). If omitted,
            existing tiles are preserved and only metadata changes.
        description: Dashboard description.
        filters: Dashboard-level filters.
        folder_id: Move to this folder (keeps original if omitted).
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON with old and new dashboard IDs.
    """
    try:
        export_data = _get_doc_svc().export_dashboard(dashboard_id)
        doc = export_data.get("document", {})
        effective_name = name or doc.get("name", "Untitled")
        effective_folder = folder_id or doc.get("folderId")
        resolved_model_id = _resolve_model_id_from_export(export_data, model_id)

        if tiles is not None:
            # Full tile replacement — build new dashboard from spec
            definition = DashboardDefinition(
                name=effective_name,
                model_id=resolved_model_id,
                description=description,
                tiles=tiles,
                filters=filters or [],
                folder_id=effective_folder,
            )
            definition.tiles = LayoutManager.auto_position(definition.tiles)
            payload = DashboardSerializer.to_omni_create_payload(definition)
            new_id, new_name = _create_with_vis_configs(
                payload, name=effective_name, folder_id=effective_folder,
            )
        else:
            # Metadata-only update — re-import with modifications
            imp_result = _get_doc_svc().import_dashboard(
                export_data,
                base_model_id=resolved_model_id,
                name=effective_name,
                folder_id=effective_folder,
            )
            new_id, new_name = imp_result.document_id, imp_result.name

        # Delete the old dashboard after successful creation
        try:
            _get_doc_svc().delete_dashboard(dashboard_id)
        except OmniDashError as del_err:
            return json.dumps({
                "status": "partial",
                "warning": f"New dashboard created but original not deleted: {del_err}",
                "old_dashboard_id": dashboard_id,
                "new_dashboard_id": new_id,
                "name": new_name,
                "url": _build_dashboard_url(new_id),
            })

        return json.dumps({
            "status": "updated",
            "old_dashboard_id": dashboard_id,
            "new_dashboard_id": new_id,
            "name": new_name,
            "url": _build_dashboard_url(new_id),
        })
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": f"Validation error: {e}"})


@mcp.tool()
def add_tiles_to_dashboard(
    dashboard_id: str,
    tiles: list[dict[str, Any]],
    model_id: str = "",
) -> str:
    """Add new tiles to an existing dashboard without replacing existing ones.

    New tiles are appended after the current tiles. Uses the same tile spec
    format as create_dashboard.

    Preserves all existing dashboard state (filters, vis configs, SQL tiles,
    layout, custom metadata) by injecting new tiles into the raw export
    rather than round-tripping through DashboardDefinition.

    Args:
        dashboard_id: The dashboard to add tiles to.
        tiles: New tile specs to append (same format as create_dashboard).
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON with updated dashboard ID and tile counts.
    """
    import copy

    try:
        if not tiles:
            return json.dumps({"error": "No tiles provided."})

        doc_svc = _get_doc_svc()

        # 1. Export existing dashboard (raw — preserves everything)
        export_data = doc_svc.export_dashboard(dashboard_id)
        doc = export_data.get("document", {})
        effective_name = doc.get("name", "Untitled")
        effective_folder = doc.get("folderId")
        resolved_model_id = _resolve_model_id_from_export(export_data, model_id)

        # Auto-validate field references before adding
        field_errors = _validate_tile_fields(tiles, resolved_model_id)
        if field_errors:
            return json.dumps({
                "error": "Invalid field references — tiles NOT added.",
                "field_errors": field_errors,
                "hint": "Use get_topic_fields to see valid field names.",
            })

        # Count existing tiles
        orig_dash = export_data.get("dashboard", {})
        orig_qpc = orig_dash.get("queryPresentationCollection", {})
        orig_memberships = orig_qpc.get(
            "queryPresentationCollectionMemberships", []
        )
        previous_count = len(orig_memberships)

        # 2. Serialize new tiles through our pipeline
        new_def = DashboardDefinition(
            name="__add_tiles_tmp__",
            model_id=resolved_model_id,
            tiles=tiles,
        )
        new_payload = DashboardSerializer.to_omni_create_payload(new_def)

        # 3. Create temp skeleton with new tiles (gets vis configs applied)
        temp_id, _ = _create_with_vis_configs(
            new_payload,
            name="__add_tiles_tmp__",
            folder_id=effective_folder,
        )

        # Wrap remaining steps so temp skeleton is always cleaned up
        try:
            # 4. Export temp skeleton (new tiles now in export format)
            temp_export = doc_svc.export_dashboard(temp_id)
            temp_dash = temp_export.get("dashboard", {})
            temp_qpc = temp_dash.get("queryPresentationCollection", {})
            temp_memberships = temp_qpc.get(
                "queryPresentationCollectionMemberships", []
            )

            # 5. Calculate layout offset — position new tiles below existing ones
            orig_layout = (
                orig_dash.get("metadata", {}).get("layouts", {}).get("lg", [])
            )
            max_y = 0
            for item in orig_layout:
                bottom = item.get("y", 0) + item.get("h", 0)
                if bottom > max_y:
                    max_y = bottom

            # 6. Merge temp export's memberships into original export
            patched = copy.deepcopy(export_data)
            patched_dash = patched.get("dashboard", {})
            patched_qpc = patched_dash.get("queryPresentationCollection", {})
            patched_memberships = patched_qpc.get(
                "queryPresentationCollectionMemberships", []
            )
            patched_memberships.extend(temp_memberships)

            # Re-index queryIdentifierMapKey on ALL memberships (1-indexed).
            # Temp memberships have keys like "1", which collide with
            # original memberships. Omni deduplicates by this key, causing
            # new tiles to be silently dropped on import.
            for idx, membership in enumerate(patched_memberships, start=1):
                qp = membership.get("queryPresentation", {})
                qp["queryIdentifierMapKey"] = str(idx)

            # 7. Merge layout items with offset AND update ephemeral field.
            # The `ephemeral` field maps layout indices to tile miniUuids:
            #   "1:ecYPPwXE,2:0fhTwpih,..."
            # Without updating it, Omni silently ignores new tiles on import.
            temp_layout = (
                temp_dash.get("metadata", {}).get("layouts", {}).get("lg", [])
            )
            patched_layout = (
                patched_dash.get("metadata", {}).get("layouts", {}).get("lg", [])
            )

            # Get ephemeral entries from temp export for new tiles
            temp_ephemeral = temp_dash.get("ephemeral", "")
            temp_eph_parts = [
                p.strip() for p in temp_ephemeral.split(",") if p.strip()
            ]
            # Build mapping of temp layout index -> miniUuid
            temp_eph_map: dict[str, str] = {}
            for part in temp_eph_parts:
                if ":" in part:
                    idx, mini = part.split(":", 1)
                    temp_eph_map[idx] = mini

            # Also get miniUuids directly from temp memberships as fallback
            temp_mini_uuids = []
            for tm in temp_memberships:
                qp = tm.get("queryPresentation", {})
                mini = qp.get("miniUuid", "")
                if mini:
                    temp_mini_uuids.append(mini)

            # Build new ephemeral entries for the appended tiles.
            # Use max existing layout `i` (not membership count) to avoid
            # index collisions with text tiles or other non-query layout items.
            def _safe_int(v: Any) -> int:
                try:
                    return int(v)
                except (ValueError, TypeError):
                    return 0

            max_existing_i = max(
                (_safe_int(item.get("i", 0)) for item in patched_layout),
                default=0,
            )
            new_eph_parts: list[str] = []
            base_idx = max_existing_i
            for idx_offset, item in enumerate(temp_layout):
                new_i = str(base_idx + 1)
                old_i = str(item.get("i", idx_offset + 1))
                # Get miniUuid from ephemeral map, or from membership list
                mini = temp_eph_map.get(old_i, "")
                if not mini and idx_offset < len(temp_mini_uuids):
                    mini = temp_mini_uuids[idx_offset]
                if mini:
                    new_eph_parts.append(f"{new_i}:{mini}")

                new_item = dict(item)
                new_item["i"] = new_i
                new_item["y"] = new_item.get("y", 0) + max_y
                patched_layout.append(new_item)
                base_idx += 1

            # Append new ephemeral entries to original
            orig_ephemeral = patched_dash.get("ephemeral", "")
            if new_eph_parts:
                sep = "," if orig_ephemeral else ""
                patched_dash["ephemeral"] = (
                    orig_ephemeral + sep + ",".join(new_eph_parts)
                )

            # 8. Reimport merged export
            reimport_model_id = _resolve_model_id_from_export(patched)
            reimport_result = doc_svc.import_dashboard(
                patched,
                base_model_id=reimport_model_id,
                name=effective_name,
                folder_id=effective_folder,
            )
            new_id = reimport_result.document_id
            new_name = reimport_result.name

            # Guard: don't delete original if reimport returned empty ID
            if not new_id:
                raise OmniDashError(
                    "Import succeeded but returned no document_id. "
                    f"Original dashboard {dashboard_id} preserved."
                )
        finally:
            # Always clean up temp skeleton, even if reimport fails
            try:
                doc_svc.delete_dashboard(temp_id)
            except Exception as e:
                logger.warning("Failed to delete temp %s: %s", temp_id, e)

        # 9. Delete original (only after successful reimport with valid ID)
        try:
            doc_svc.delete_dashboard(dashboard_id)
        except Exception as e:
            logger.warning("Failed to delete original %s during add_tiles cleanup: %s", dashboard_id, e)

        return json.dumps({
            "status": "updated",
            "dashboard_id": new_id,
            "name": new_name,
            "previous_tile_count": previous_count,
            "new_tile_count": previous_count + len(tiles),
            "tiles_added": len(tiles),
            "url": _build_dashboard_url(new_id),
        })
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": f"Validation error: {e}"})


@mcp.tool()
def update_tile(
    dashboard_id: str,
    tile_name: str,
    sql: str | None = None,
    fields: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    chart_type: str | None = None,
    title: str | None = None,
) -> str:
    """Update a single tile in an existing dashboard without affecting other tiles.

    Modifies only the specified tile — all other tiles, filters, vis configs,
    and layout are preserved. Works by patching the raw export directly.

    Args:
        dashboard_id: The dashboard containing the tile.
        tile_name: Name of the tile to update (must match exactly).
        sql: New SQL query. Sets the tile to SQL mode (isSql=True).
        fields: New field list (e.g., ["table.col1", "table.col2"]).
        filters: New filter config (Omni format: {field: {kind, type, values}}).
        chart_type: New chart type (e.g., "bar", "line", "number").
        title: New tile title/name.

    Returns:
        JSON with updated dashboard ID and URL.
    """
    import copy

    try:
        if not any([sql, fields, filters, chart_type, title]):
            return json.dumps({"error": "No updates specified."})

        doc_svc = _get_doc_svc()

        # 1. Export dashboard
        export_data = doc_svc.export_dashboard(dashboard_id)
        doc = export_data.get("document", {})
        effective_name = doc.get("name", "Untitled")
        effective_folder = doc.get("folderId")

        patched = copy.deepcopy(export_data)
        dash = patched.get("dashboard", {})
        qpc = dash.get("queryPresentationCollection", {})
        memberships = qpc.get("queryPresentationCollectionMemberships", [])

        # 2. Find tile by name
        target_qp = None
        for membership in memberships:
            qp = membership.get("queryPresentation", {})
            if qp.get("name") == tile_name:
                target_qp = qp
                break

        if target_qp is None:
            available = [
                m.get("queryPresentation", {}).get("name", "")
                for m in memberships
            ]
            return json.dumps({
                "error": f"Tile '{tile_name}' not found.",
                "available_tiles": available,
            })

        # 3. Patch specified fields
        modified = False

        if sql is not None:
            target_qp["isSql"] = True
            q = target_qp.setdefault("query", {})
            q_json = q.setdefault("queryJson", {})
            q_json["userEditedSQL"] = sql
            modified = True

        if fields is not None:
            q = target_qp.setdefault("query", {})
            q_json = q.setdefault("queryJson", {})
            q_json["fields"] = fields
            modified = True

        if filters is not None:
            q = target_qp.setdefault("query", {})
            q_json = q.setdefault("queryJson", {})
            q_json["filters"] = filters
            modified = True

        if chart_type is not None:
            from omni_dash.dashboard.serializer import _CHART_TYPE_TO_OMNI

            omni_ct = _CHART_TYPE_TO_OMNI.get(chart_type, chart_type)
            vc = target_qp.setdefault("visConfig", {})
            vc["chartType"] = omni_ct
            # Update visType based on chart type
            if omni_ct in ("kpi", "summaryValue"):
                vc["visType"] = "omni-kpi"
            elif omni_ct == "table":
                vc["visType"] = "omni-table"
            elif omni_ct == "markdown":
                vc["visType"] = "omni-markdown"
            elif omni_ct == "code":
                vc["visType"] = "vegalite"
            else:
                vc["visType"] = "basic"
            modified = True

        if title is not None:
            target_qp["name"] = title
            modified = True

        if modified:
            vc = target_qp.setdefault("visConfig", {})
            vc.pop("jsonHash", None)

        # 4. Reimport modified export
        reimport_model_id = _resolve_model_id_from_export(patched)
        reimport_result = doc_svc.import_dashboard(
            patched,
            base_model_id=reimport_model_id,
            name=effective_name,
            folder_id=effective_folder,
        )
        new_id = reimport_result.document_id

        # Guard: don't delete original if reimport returned empty ID
        if not new_id:
            raise OmniDashError(
                "Import succeeded but returned no document_id. "
                f"Original dashboard {dashboard_id} preserved."
            )

        # 5. Delete original
        try:
            doc_svc.delete_dashboard(dashboard_id)
        except Exception as e:
            logger.warning("Failed to delete original %s after update_tile: %s", dashboard_id, e)
            return json.dumps({
                "status": "partial",
                "warning": f"Tile updated but original dashboard not deleted: {e}",
                "old_dashboard_id": dashboard_id,
                "dashboard_id": new_id,
                "tile_name": title or tile_name,
                "url": _build_dashboard_url(new_id),
            })

        return json.dumps({
            "status": "updated",
            "dashboard_id": new_id,
            "tile_name": title or tile_name,
            "url": _build_dashboard_url(new_id),
        })
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": f"Unexpected error: {e}"})


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
                    "base_view": t.base_view,
                }
                for t in topics
            ],
            indent=2,
        )
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
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

        detail = _get_model_svc().get_topic_native(resolved_model_id, topic_name)
        fields = detail.fields
        total = len(fields)
        truncated = total > 200
        if truncated:
            fields = fields[:200]
        result: dict[str, Any] = {
            "name": detail.name,
            "label": detail.label,
            "description": detail.description,
            "base_view": detail.base_view,
            "views": detail.views,
            "field_count": total,
            "fields": fields,
        }
        if truncated:
            result["truncated"] = True
            result["note"] = (
                f"Showing first 200 of {total} fields. "
                "Use query_data to explore specific fields."
            )
        return json.dumps(result, indent=2)
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
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
        sorts: Sort specs [{column_name, sort_descending}].
        filters: Filter map {qualified_field: {operator, value}}.
        limit: Max rows to return (default 25, max 1000).
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON with fields and data rows.
    """
    try:
        if not table:
            return json.dumps({"error": "table is required"})
        if not fields:
            return json.dumps({"error": "fields list cannot be empty"})

        resolved_model_id = model_id or _get_shared_model_id()
        if not resolved_model_id:
            return json.dumps({"error": "No model_id found. Set OMNI_SHARED_MODEL_ID."})

        # Resolve topic names with spaces to base_view names
        resolved_table = _resolve_table_name(table, resolved_model_id)

        limit = min(limit, 1000)

        builder = QueryBuilder(resolved_model_id, resolved_table)
        builder.fields(fields)
        builder.limit(limit)
        if sorts:
            for s in sorts:
                col = s.get("column_name") or s.get("columnName", "")
                desc = s.get("sort_descending", s.get("sortDescending", False))
                builder.sort(col, descending=desc)
        if filters:
            # Pass filters through directly — Omni filter format is opaque
            # and may use {kind, type, values} or {operator, value}
            builder._filters = filters

        spec = builder.build()
        result = _get_query_runner().run(spec)

        rows = result.rows or []
        return json.dumps(
            {
                "fields": result.fields,
                "rows": rows[:limit],
                "row_count": result.row_count,
                "truncated": result.truncated,
            },
            indent=2,
            default=str,
        )
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
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
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Wave 2: Intelligent tools
# ---------------------------------------------------------------------------


@mcp.tool()
def suggest_chart(
    table: str,
    fields: list[str] | None = None,
    model_id: str = "",
) -> str:
    """Suggest the best chart type for a given table and fields.

    Analyzes field types to recommend chart_type, vis_config skeleton,
    and reasoning. If fields are omitted, all fields from the table are analyzed.

    Args:
        table: Topic/table name (e.g., "mart_seo_weekly_funnel").
        fields: Optional list of specific fields to analyze.
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON with chart_type, confidence, reasoning, vis_config, alternatives.
    """
    try:
        from omni_dash.ai.chart_recommender import classify_field, recommend_chart

        mid = model_id or _get_shared_model_id()
        if not mid:
            return json.dumps({"error": "No model_id found. Set OMNI_SHARED_MODEL_ID."})
        model_svc = _get_model_svc()
        detail = model_svc.get_topic_native(mid, table)

        # Filter to requested fields if specified
        all_fields = detail.fields
        if fields:
            field_set = set(fields)
            all_fields = [
                f for f in all_fields
                if f.get("name", "") in field_set
                or f.get("qualified_name", "") in field_set
                or f"{table}.{f.get('name', '')}" in field_set
            ]

        classified = [classify_field(f) for f in all_fields]
        rec = recommend_chart(classified)

        return json.dumps(
            {
                "chart_type": rec.chart_type,
                "confidence": rec.confidence,
                "reasoning": rec.reasoning,
                "vis_config": rec.vis_config,
                "alternatives": rec.alternatives,
                "fields_analyzed": len(classified),
            },
            indent=2,
        )
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def validate_dashboard(
    tiles: list[dict[str, Any]],
    model_id: str = "",
) -> str:
    """Validate a dashboard spec before creating it.

    Checks field existence, chart type validity, axis/sort alignment,
    format codes, and KPI limits. Returns errors and warnings.

    Args:
        tiles: Array of tile specs (same format as create_dashboard).
        model_id: Omni model ID for field existence checks.

    Returns:
        JSON with valid (bool), errors (list), warnings (list).
    """
    try:
        from omni_dash.dashboard.definition import (
            FilterSpec,
            SortSpec,
            Tile,
            TileQuery,
            TileVisConfig,
        )
        from omni_dash.dashboard.validator import validate_definition

        parsed_tiles = []
        for t in tiles:
            q = t.get("query", {})
            vc = t.get("vis_config", {})
            sorts = [
                SortSpec(
                    column_name=s.get("column_name", ""),
                    sort_descending=s.get("sort_descending", False),
                )
                for s in q.get("sorts", [])
            ]
            filters = [
                FilterSpec(
                    field=f.get("field", ""),
                    operator=f.get("operator", "is"),
                    value=f.get("value"),
                )
                for f in q.get("filters", [])
            ]
            parsed_tiles.append(
                Tile(
                    name=t.get("name", "Untitled"),
                    chart_type=t.get("chart_type", "line"),
                    query=TileQuery(
                        table=q.get("table", ""),
                        fields=q.get("fields", []),
                        sorts=sorts,
                        filters=filters,
                        limit=q.get("limit", 200),
                    ),
                    vis_config=TileVisConfig(
                        x_axis=vc.get("x_axis"),
                        y_axis=vc.get("y_axis", []),
                        value_format=vc.get("value_format"),
                        y_axis_format=vc.get("y_axis_format"),
                    ),
                )
            )

        mid = model_id or _get_shared_model_id()
        definition = DashboardDefinition(
            name="validation_check",
            model_id=mid,
            tiles=parsed_tiles,
        )

        # Optionally fetch available fields for existence checks
        available_fields: dict[str, set[str]] | None = None
        if mid:
            try:
                model_svc = _get_model_svc()
                tables = {t.query.table for t in parsed_tiles if t.query.table}
                available_fields = {}
                for tbl in tables:
                    detail = model_svc.get_topic_native(mid, tbl)
                    available_fields[tbl] = {
                        f.get("name", "") for f in detail.fields
                    } | {
                        f"{tbl}.{f.get('name', '')}" for f in detail.fields
                    }
            except Exception:
                pass

        result = validate_definition(definition, available_fields)

        return json.dumps(
            {
                "valid": result.valid,
                "errors": result.errors,
                "warnings": result.warnings,
            },
            indent=2,
        )
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def profile_data(
    table: str,
    fields: list[str] | None = None,
    sample_size: int = 100,
    model_id: str = "",
) -> str:
    """Profile data in a table to understand field distributions.

    Runs a small query and computes per-field stats: distinct_count,
    null_count, min, max, sample_values, inferred_type. Helps pick
    chart types and formats intelligently.

    Args:
        table: Topic/table name.
        fields: Specific fields to profile. If omitted, profiles all.
        sample_size: Number of rows to sample (default 100, max 1000).
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON with per-field profile stats.
    """
    try:
        mid = model_id or _get_shared_model_id()
        if not mid:
            return json.dumps({"error": "No model_id found. Set OMNI_SHARED_MODEL_ID."})
        model_svc = _get_model_svc()
        query_runner = _get_query_runner()

        # Resolve topic names with spaces to base_view names
        resolved_table = _resolve_table_name(table, mid)

        auto_discovered = False
        if not fields:
            try:
                detail = model_svc.get_topic_native(mid, table)
                all_field_count = len(detail.fields)
                fields = [f.get("name", "") for f in detail.fields[:20]]
                if all_field_count > 20:
                    auto_discovered = True
            except Exception:
                return json.dumps(
                    {"error": f"Could not discover fields for '{table}'. "
                     "Pass explicit field names to profile, or check that "
                     "the table exists with list_topics."}
                )

        qualified = [f if "." in f else f"{resolved_table}.{f}" for f in fields]

        builder = QueryBuilder(table=resolved_table, model_id=mid)
        builder.fields(qualified)
        builder.limit(min(sample_size, 1000))
        result = query_runner.run(builder.build())

        profiles: dict[str, Any] = {}
        for field_name in qualified:
            col_values = []
            for row in result.rows:
                val = row.get(field_name)
                col_values.append(val)

            non_null = [v for v in col_values if v is not None]
            distinct = set(str(v) for v in non_null)

            profile: dict[str, Any] = {
                "sample_count": len(col_values),
                "null_count": len(col_values) - len(non_null),
                "distinct_count": len(distinct),
            }

            if non_null:
                sample_strs = [str(v) for v in non_null[:5]]
                profile["sample_values"] = sample_strs

                try:
                    nums = [float(v) for v in non_null]
                    profile["min"] = min(nums)
                    profile["max"] = max(nums)
                    profile["inferred_type"] = "number"
                except (ValueError, TypeError):
                    profile["min"] = str(min(non_null, key=str))
                    profile["max"] = str(max(non_null, key=str))
                    # Detect dates by ISO format (YYYY-MM-DD), not hyphens
                    sample = str(non_null[0])
                    is_date = False
                    if 8 <= len(sample) <= 25:
                        if re.match(r"^\d{4}[-/]\d{2}[-/]\d{2}", sample):
                            is_date = True
                    profile["inferred_type"] = "date" if is_date else "string"

            profiles[field_name] = profile

        response: dict[str, Any] = {
            "table": table,
            "row_count": result.row_count,
            "fields": profiles,
        }
        if auto_discovered:
            response["note"] = (
                f"Profiled first 20 of {all_field_count} fields. "
                "Pass specific field names to profile others."
            )
        return json.dumps(response, indent=2, default=str)
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def generate_dashboard(
    prompt: str,
    folder_id: str = "",
    model_id: str = "",
) -> str:
    """Generate a complete dashboard from natural language.

    Uses AI to analyze available data, select appropriate chart types,
    and create a full dashboard. This is the most powerful tool —
    describe what you want and get a working dashboard URL.

    Args:
        prompt: Natural language description (e.g., "Show me weekly SEO trends").
        folder_id: Omni folder ID to create in.
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON with dashboard URL and ID.
    """
    try:
        from omni_dash.ai.omni_adapter import OmniModelAdapter
        from omni_dash.ai.service import DashboardAI

        mid = model_id or _get_shared_model_id()
        if not mid:
            return json.dumps({"error": "No model_id found. Set OMNI_SHARED_MODEL_ID."})
        model_svc = _get_model_svc()

        adapter = OmniModelAdapter(model_svc, mid)

        def _ai_query_fn(table: str, fields: list[str], limit: int) -> list[dict]:
            """Bridge AI query_data tool to Omni query runner."""
            from omni_dash.api.queries import QueryBuilder

            builder = QueryBuilder(mid, table)
            builder.fields(fields)
            builder.limit(min(limit, 25))
            spec = builder.build()
            result = _get_query_runner().run(spec)
            return result.rows[:limit]

        ai_model = os.environ.get("OMNI_DASH_AI_MODEL", "claude-sonnet-4-5-20250929")
        ai = DashboardAI(
            adapter, model=ai_model, query_fn=_ai_query_fn,
        )

        result = ai.generate(prompt)
        definition = result.definition
        definition.model_id = mid
        if folder_id:
            definition.folder_id = folder_id

        # Auto-position tiles on grid (same as create_dashboard)
        definition.tiles = LayoutManager.auto_position(definition.tiles)

        payload = DashboardSerializer.to_omni_create_payload(definition)
        n_qps = len(payload.get("queryPresentations", []))
        logger.info(
            "generate_dashboard: %d tiles, model_id=%s",
            n_qps,
            payload.get("modelId", "MISSING"),
        )
        if n_qps == 0:
            return json.dumps({
                "error": "AI generated 0 tiles — nothing to create. "
                "Try a more specific prompt or check available tables with list_topics.",
                "reasoning": result.reasoning,
                "tool_calls_made": result.tool_calls_made,
            })

        dash_id, dash_name = _create_with_vis_configs(
            payload, name=definition.name, folder_id=folder_id or None,
        )
        url = _build_dashboard_url(dash_id)

        return json.dumps(
            {
                "dashboard_id": dash_id,
                "dashboard_name": dash_name,
                "url": url,
                "tile_count": definition.tile_count,
                "tool_calls_made": result.tool_calls_made,
                "reasoning": result.reasoning,
            },
            indent=2,
        )
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Omni native AI tools
# ---------------------------------------------------------------------------


@mcp.tool()
def ai_generate_query(
    prompt: str,
    topic_name: str = "",
    model_id: str = "",
) -> str:
    """Convert natural language to a structured Omni query using Omni's native AI.

    Omni's AI has full knowledge of the semantic model — joins, measures,
    dimensions, and relationships. Returns a query spec that can be run
    directly or used to build dashboard tiles.

    Args:
        prompt: Natural language query (e.g., "Show me revenue by month").
        topic_name: Optional topic to scope the query to.
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON with table, fields, sorts, filters, and the raw Omni response.
    """
    try:
        mid = model_id or _get_shared_model_id()
        ai = _get_ai_svc()
        result = ai.generate_query(
            mid,
            prompt,
            topic_name=topic_name or None,
        )
        return json.dumps({
            "table": result.table,
            "fields": result.fields,
            "sorts": result.sorts,
            "filters": result.filters,
            "limit": result.limit,
            "calculations": result.calculations,
        }, indent=2)
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": f"AI query generation failed: {e}"})


@mcp.tool()
def ai_pick_topic(
    prompt: str,
    model_id: str = "",
) -> str:
    """Use Omni's AI to pick the best data topic for a question.

    Analyzes the prompt and the semantic model to find the most relevant
    topic (table/view) to query.

    Args:
        prompt: Natural language question (e.g., "What are our top customers?").
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON with the recommended topic name.
    """
    try:
        mid = model_id or _get_shared_model_id()
        ai = _get_ai_svc()
        topic = ai.pick_topic(mid, prompt)
        return json.dumps({"topic": topic})
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": f"Topic selection failed: {e}"})


@mcp.tool()
def ai_analyze(
    prompt: str,
    topic_name: str = "",
    model_id: str = "",
) -> str:
    """Run an AI-powered data analysis using Omni's native AI.

    Omni's AI will explore the data, run queries, and return a markdown
    summary with insights. Takes 15-60 seconds for complex analyses.

    Args:
        prompt: Analysis request (e.g., "Analyze our churn patterns").
        topic_name: Optional topic to scope the analysis.
        model_id: Omni model ID. Auto-discovered if omitted.

    Returns:
        JSON with the AI's analysis, summary, and actions taken.
    """
    try:
        mid = model_id or _get_shared_model_id()
        ai = _get_ai_svc()
        job = ai.create_job(
            mid,
            prompt,
            topic_name=topic_name or None,
        )
        result = ai.wait_for_job(job.job_id, timeout=120.0)
        return json.dumps({
            "summary": result.result_summary,
            "message": result.message,
            "topic": result.topic,
            "actions_count": len(result.actions),
            "omni_chat_url": result.omni_chat_url,
        }, indent=2)
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": f"AI analysis failed: {e}"})


# ---------------------------------------------------------------------------
# Dashboard filter management
# ---------------------------------------------------------------------------


@mcp.tool()
def get_dashboard_filters(dashboard_id: str) -> str:
    """Get the filter configuration for a dashboard.

    Returns the current filters, their types, values, and ordering.
    Useful for understanding what filters exist before updating them.

    Args:
        dashboard_id: The dashboard identifier.

    Returns:
        JSON with filters dict, filterOrder list, and controls.
    """
    try:
        result = _get_doc_svc().get_filters(dashboard_id)
        return json.dumps(result, indent=2)
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": f"Failed to get filters: {e}"})


@mcp.tool()
def update_dashboard_filters(
    dashboard_id: str,
    filters: dict[str, Any] | None = None,
    filter_order: list[str] | None = None,
    clear_existing_draft: bool = False,
) -> str:
    """Update existing filter values on a dashboard.

    Can only modify filters that already exist. Filter IDs must match
    those returned by get_dashboard_filters. To add NEW filters,
    use update_dashboard with filter configs instead.

    Args:
        dashboard_id: The dashboard to update.
        filters: Dict of filter_id -> {kind, type, values, fieldName, ...}.
        filter_order: New ordering of filter IDs.
        clear_existing_draft: Discard existing draft before applying. Use if
            you get a 409 draft conflict error.

    Returns:
        JSON with updated filter configuration.
    """
    try:
        result = _get_doc_svc().update_filters(
            dashboard_id,
            filters=filters,
            filter_order=filter_order,
            clear_existing_draft=clear_existing_draft,
        )
        return json.dumps(result, indent=2)
    except OmniDashError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": f"Failed to update filters: {e}"})
