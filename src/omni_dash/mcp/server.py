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
    model_id = settings.omni_shared_model_id
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
                detail = model_svc.get_topic(model_id, tbl)
                qualified = set()
                for f in detail.fields:
                    fname = f.get("name", "")
                    qualified.add(fname)
                    qualified.add(f"{tbl}.{fname}")
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
    """Create a dashboard via import when the create endpoint returns 404.

    Converts a create-format payload into an import-format payload and
    calls the import endpoint instead.

    Returns:
        The new dashboard ID.
    """
    model_id = payload.get("modelId", "")
    memberships = []
    for idx, qp in enumerate(payload.get("queryPresentations", []), start=1):
        memberships.append({
            "queryPresentation": {
                "name": qp.get("name", ""),
                "query": {"queryJson": qp.get("query", {})},
                "visConfig": qp.get("visConfig", {}),
                "isSql": qp.get("isSql", False),
                "queryIdentifierMapKey": qp.get("queryIdentifierMapKey", str(idx)),
            }
        })

    export_payload: dict[str, Any] = {
        "exportVersion": "0.1",
        "document": {
            "name": name or payload.get("name", "Untitled"),
            "modelId": model_id,
        },
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": memberships,
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

    # Extract vis configs and query overrides before sending
    # (create endpoint ignores visConfig; export may lose filters/pivots)
    vis_configs_by_name: dict[str, dict] = {}
    query_overrides_by_name: dict[str, dict] = {}
    for qp in payload.get("queryPresentations", []):
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
        # Fallback: if the create endpoint fails (404 in some Omni envs),
        # create via import instead.
        err_str = str(create_err).lower()
        if "404" in err_str or "not found" in err_str:
            logger.warning(
                "create_dashboard returned 404, falling back to import: %s",
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
            existing_vc = qp_data.get("visConfig", {})
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
            if "config" in vc_patch:
                existing_vc["config"] = vc_patch["config"]
            if vc_patch.get("fields"):
                existing_vc["fields"] = vc_patch["fields"]
            existing_vc.pop("jsonHash", None)

        # Patch query data (filters, pivots, sorts) into queryJson
        # Export stores query under query.queryJson, not query directly
        if tile_name in query_overrides_by_name:
            q_overrides = query_overrides_by_name[tile_name]
            q_json = qp_data.get("query", {}).get("queryJson", {})
            if q_json:
                for key in ("filters", "pivots", "sorts"):
                    if key in q_overrides and not q_json.get(key):
                        q_json[key] = q_overrides[key]

    reimport_model_id = _resolve_model_id_from_export(patched)
    reimport_result = doc_svc.import_dashboard(
        patched,
        base_model_id=reimport_model_id,
        name=name,
        folder_id=folder_id,
    )

    # Delete the skeleton (best-effort)
    try:
        doc_svc.delete_dashboard(skeleton_id)
    except Exception:
        pass

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
        except OmniDashError as del_err:
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

        # 7. Merge layout items with offset
        temp_layout = (
            temp_dash.get("metadata", {}).get("layouts", {}).get("lg", [])
        )
        patched_layout = (
            patched_dash.get("metadata", {}).get("layouts", {}).get("lg", [])
        )
        base_idx = previous_count
        for item in temp_layout:
            new_item = dict(item)
            new_item["i"] = base_idx + 1
            new_item["y"] = new_item.get("y", 0) + max_y
            patched_layout.append(new_item)
            base_idx += 1

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

        # 9. Cleanup: delete original + temp skeleton
        for del_id in (dashboard_id, temp_id):
            try:
                doc_svc.delete_dashboard(del_id)
            except Exception:
                pass

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
            q = target_qp.get("query", {})
            q_json = q.get("queryJson", q)
            q_json["userEditedSQL"] = sql
            modified = True

        if fields is not None:
            q = target_qp.get("query", {})
            q_json = q.get("queryJson", q)
            q_json["fields"] = fields
            modified = True

        if filters is not None:
            q = target_qp.get("query", {})
            q_json = q.get("queryJson", q)
            q_json["filters"] = filters
            modified = True

        if chart_type is not None:
            from omni_dash.dashboard.serializer import _CHART_TYPE_TO_OMNI

            omni_ct = _CHART_TYPE_TO_OMNI.get(chart_type, chart_type)
            vc = target_qp.get("visConfig", {})
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
            vc = target_qp.get("visConfig", {})
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

        # 5. Delete original
        try:
            doc_svc.delete_dashboard(dashboard_id)
        except Exception:
            pass

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
                "base_view": detail.base_view,
                "views": detail.views,
                "field_count": len(detail.fields),
                "fields": detail.fields,
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
        sorts: Sort specs [{column_name, sort_descending}].
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
            # Normalize sort keys to snake_case (Omni API format)
            normalized = []
            for s in sorts:
                normalized.append({
                    "column_name": s.get("column_name") or s.get("columnName", ""),
                    "sort_descending": s.get("sort_descending", s.get("sortDescending", False)),
                })
            builder._sorts = normalized
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
        model_svc = _get_model_svc()
        detail = model_svc.get_topic(mid, table)

        # Filter to requested fields if specified
        all_fields = detail.fields
        if fields:
            field_set = set(fields)
            all_fields = [
                f for f in all_fields
                if f.get("name", "") in field_set
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
                    detail = model_svc.get_topic(mid, tbl)
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
        model_svc = _get_model_svc()
        query_runner = _get_query_runner()

        if not fields:
            try:
                detail = model_svc.get_topic(mid, table)
                fields = [f.get("name", "") for f in detail.fields[:20]]
            except Exception:
                # Fallback for non-topic views (e.g. Snowflake views not
                # registered as Omni topics): discover fields via a small
                # query and extract column names from the result.
                try:
                    builder = QueryBuilder(table=table, model_id=mid)
                    builder._fields = [f"{table}.*"]
                    builder._limit = 1
                    sample_result = query_runner.run(builder.build())
                    if sample_result.fields:
                        fields = sample_result.fields[:20]
                    elif sample_result.rows:
                        fields = list(sample_result.rows[0].keys())[:20]
                    else:
                        return json.dumps(
                            {"error": f"Could not discover fields for '{table}'."}
                        )
                except Exception:
                    return json.dumps(
                        {"error": f"Could not discover fields for '{table}'. "
                         "The table may not exist or is not queryable."}
                    )

        qualified = [f if "." in f else f"{table}.{f}" for f in fields]

        builder = QueryBuilder(table=table, model_id=mid)
        builder._fields = qualified
        builder._limit = min(sample_size, 1000)
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
                    sample = str(non_null[0])
                    if len(sample) >= 8 and ("-" in sample or "/" in sample):
                        profile["inferred_type"] = "date"
                    else:
                        profile["inferred_type"] = "string"

            profiles[field_name] = profile

        return json.dumps(
            {
                "table": table,
                "row_count": result.row_count,
                "fields": profiles,
            },
            indent=2,
            default=str,
        )
    except OmniDashError as e:
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
        model_svc = _get_model_svc()

        adapter = OmniModelAdapter(model_svc, mid)
        ai = DashboardAI(adapter, model="claude-sonnet-4-5-20250929")

        result = ai.generate(prompt)
        definition = result.definition
        definition.model_id = mid
        if folder_id:
            definition.folder_id = folder_id

        payload = DashboardSerializer.to_omni_create_payload(definition)
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
