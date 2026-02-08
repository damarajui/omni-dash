"""Dashboard/document CRUD operations against the Omni API."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field

from omni_dash.api.client import OmniClient
from omni_dash.exceptions import DocumentNotFoundError, OmniAPIError

logger = logging.getLogger(__name__)


def _extract_records(result: Any) -> list[dict[str, Any]]:
    """Extract the records list from a paginated Omni API response.

    The Omni API returns ``{pageInfo: {...}, records: [...]}`` for list
    endpoints.  Older or mock responses may return a plain list.
    """
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        if "records" in result:
            return result["records"]
        # Fallback for unknown dict shapes
        for key in ("documents", "folders"):
            if key in result and isinstance(result[key], list):
                return result[key]
    return []


# -- Response models --


class DocumentSummary(BaseModel):
    """Lightweight document metadata returned by list endpoints."""

    id: str
    name: str
    document_type: str = ""
    folder_id: str | None = None
    created_at: str = ""
    updated_at: str = ""
    model_id: str = ""


class DashboardResponse(BaseModel):
    """Response from creating or fetching a dashboard."""

    document_id: str
    name: str
    model_id: str = ""
    query_presentations: list[dict[str, Any]] = Field(default_factory=list)
    layouts: list[dict[str, Any]] = Field(default_factory=list)
    text_tiles: list[dict[str, Any]] = Field(default_factory=list)
    tile_settings: dict[str, Any] = Field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""


class ImportResponse(BaseModel):
    """Response from importing a dashboard."""

    document_id: str
    name: str = ""
    success: bool = True


# -- Service --


class DocumentService:
    """High-level dashboard/document management.

    Wraps the Omni API endpoints for creating, reading, exporting, and
    importing dashboards. Handles response parsing and error mapping.
    """

    def __init__(self, client: OmniClient):
        self._client = client

    def create_dashboard(
        self,
        payload: dict[str, Any],
        *,
        folder_id: str | None = None,
    ) -> DashboardResponse:
        """Create a new dashboard via POST /api/v1/documents.

        Args:
            payload: Omni-format document payload containing modelId,
                     name, and queryPresentations.
            folder_id: Optional folder to create the dashboard in.

        Returns:
            DashboardResponse with the created document's metadata.
        """
        payload = {**payload}  # Don't mutate caller's dict
        if folder_id:
            payload["folderId"] = folder_id

        result = self._client.post("/api/v1/documents", json=payload)

        if not result or not isinstance(result, dict):
            raise OmniAPIError(0, "Empty response from document creation")

        # The create endpoint returns {dashboard: {...}, workbook: {...}}.
        # The document ID is workbook.identifier (short ID for URLs),
        # the full UUID is workbook.id, and the name is workbook.name.
        workbook = result.get("workbook", {})
        dashboard = result.get("dashboard", {})

        # Use workbook.identifier (short URL ID) as primary, fall back
        # to nested IDs, then top-level fields for backwards compat.
        doc_id = (
            workbook.get("identifier")
            or workbook.get("id")
            or dashboard.get("dashboardId")
            or result.get("identifier")
            or result.get("id", "")
        )

        return DashboardResponse(
            document_id=doc_id,
            name=workbook.get("name", result.get("name", payload.get("name", ""))),
            model_id=payload.get("modelId", ""),
            query_presentations=result.get("queryPresentations", []),
            layouts=dashboard.get("metadata", {}).get("layouts", {}).get("lg", []),
            text_tiles=dashboard.get("metadata", {}).get("textTiles", []),
            tile_settings=dashboard.get("metadata", {}).get("tileSettings", {}),
            created_at=workbook.get("createdAt", result.get("createdAt", "")),
            updated_at=workbook.get("updatedAt", result.get("updatedAt", "")),
        )

    def get_dashboard(self, document_id: str) -> DashboardResponse:
        """Fetch a dashboard by ID."""
        result = self._client.get(f"/api/v1/documents/{document_id}")
        if not result or not isinstance(result, dict):
            raise DocumentNotFoundError(document_id)

        return DashboardResponse(
            document_id=result.get("id", document_id),
            name=result.get("name", ""),
            model_id=result.get("modelId", ""),
            query_presentations=result.get("queryPresentations", []),
            layouts=result.get("layouts", []),
            text_tiles=result.get("textTiles", []),
            tile_settings=result.get("tileSettings", {}),
            created_at=result.get("createdAt", ""),
            updated_at=result.get("updatedAt", ""),
        )

    def list_dashboards(self, folder_id: str | None = None) -> list[DocumentSummary]:
        """List all dashboards, optionally filtered by folder.

        Handles the paginated Omni response format ``{pageInfo, records}``
        and automatically fetches subsequent pages.
        """
        params: dict[str, str] = {"pageSize": "100"}
        if folder_id:
            params["folderId"] = folder_id

        all_docs: list[DocumentSummary] = []
        while True:
            result = self._client.get("/api/v1/documents", params=params)
            if not result:
                break

            records = _extract_records(result)
            for d in records:
                folder = d.get("folder") or {}
                all_docs.append(
                    DocumentSummary(
                        id=d.get("identifier", d.get("id", "")),
                        name=d.get("name", ""),
                        document_type="dashboard" if d.get("hasDashboard") else "workbook",
                        folder_id=folder.get("id") if folder else None,
                        created_at=d.get("createdAt", ""),
                        updated_at=d.get("updatedAt", ""),
                        model_id=d.get("connectionId", ""),
                    )
                )

            page_info = result.get("pageInfo", {}) if isinstance(result, dict) else {}
            if page_info.get("hasNextPage") and page_info.get("nextCursor"):
                params["cursor"] = page_info["nextCursor"]
            else:
                break

        return all_docs

    def export_dashboard(self, document_id: str) -> dict[str, Any]:
        """Export a dashboard's full definition (beta endpoint).

        Uses GET /api/unstable/documents/:id/export which returns the
        complete dashboard structure including layout, queries, and
        workbook model.
        """
        result = self._client.get(
            f"/api/unstable/documents/{document_id}/export",
            timeout=60.0,
        )
        if not result or not isinstance(result, dict):
            raise DocumentNotFoundError(document_id)
        return result

    def import_dashboard(
        self,
        export_data: dict[str, Any],
        base_model_id: str,
        *,
        name: str | None = None,
        folder_id: str | None = None,
    ) -> ImportResponse:
        """Import a dashboard from an export payload (beta endpoint).

        Uses POST /api/unstable/documents/import.

        Args:
            export_data: Full export payload (from export_dashboard or YAML file).
            base_model_id: Target Omni model ID.
            name: Override the dashboard name.
            folder_id: Destination folder.

        Returns:
            ImportResponse with the new document ID.
        """
        doc = {**export_data.get("document", {})}  # Copy to avoid mutating caller's dict
        if name:
            doc["name"] = name
        if folder_id:
            doc["folderId"] = folder_id

        payload = {
            "baseModelId": base_model_id,
            "dashboard": export_data.get("dashboard", {}),
            "document": doc,
            "exportVersion": export_data.get("exportVersion", "0.1"),
            "workbookModel": export_data.get("workbookModel", {}),
        }

        result = self._client.post(
            "/api/unstable/documents/import",
            json=payload,
            timeout=60.0,
        )

        if not result or not isinstance(result, dict):
            raise OmniAPIError(0, "Empty response from dashboard import")

        return ImportResponse(
            document_id=result.get("identifier", result.get("id", result.get("documentId", ""))),
            name=result.get("name", name or ""),
            success=True,
        )

    def delete_dashboard(self, document_id: str) -> None:
        """Delete a dashboard by ID."""
        self._client.delete(f"/api/v1/documents/{document_id}")
        logger.info("Deleted dashboard %s", document_id)

    def list_folders(self) -> list[dict[str, Any]]:
        """List all folders in the organization.

        Handles the paginated ``{pageInfo, records}`` response format.
        """
        params: dict[str, str] = {"pageSize": "100"}
        all_folders: list[dict[str, Any]] = []
        while True:
            result = self._client.get("/api/v1/folders", params=params)
            if not result:
                break

            all_folders.extend(_extract_records(result))

            page_info = result.get("pageInfo", {}) if isinstance(result, dict) else {}
            if page_info.get("hasNextPage") and page_info.get("nextCursor"):
                params["cursor"] = page_info["nextCursor"]
            else:
                break

        return all_folders

    def download_dashboard(
        self,
        dashboard_id: str,
        *,
        file_format: str = "pdf",
    ) -> bytes:
        """Download a rendered dashboard (PDF, PNG, CSV).

        Returns raw bytes. The caller is responsible for writing to disk.
        """
        valid_formats = {"pdf", "png", "csv"}
        if file_format not in valid_formats:
            raise ValueError(f"file_format must be one of {valid_formats}, got '{file_format}'")

        result = self._client.get_raw(
            f"/api/v1/dashboards/{dashboard_id}/download",
            params={"format": file_format},
            timeout=120.0,
        )
        return result
