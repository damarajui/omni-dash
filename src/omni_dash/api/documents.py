"""Dashboard/document CRUD operations against the Omni API."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field

from omni_dash.api.client import OmniClient
from omni_dash.exceptions import DocumentNotFoundError, OmniAPIError

logger = logging.getLogger(__name__)


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
        if folder_id:
            payload["folderId"] = folder_id

        result = self._client.post("/api/v1/documents", json=payload)

        if not result or not isinstance(result, dict):
            raise OmniAPIError(0, "Empty response from document creation")

        return DashboardResponse(
            document_id=result.get("id", result.get("documentId", "")),
            name=result.get("name", payload.get("name", "")),
            model_id=result.get("modelId", payload.get("modelId", "")),
            query_presentations=result.get("queryPresentations", []),
            layouts=result.get("layouts", []),
            text_tiles=result.get("textTiles", []),
            tile_settings=result.get("tileSettings", {}),
            created_at=result.get("createdAt", ""),
            updated_at=result.get("updatedAt", ""),
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
        """List all dashboards, optionally filtered by folder."""
        params: dict[str, str] = {}
        if folder_id:
            params["folderId"] = folder_id

        result = self._client.get("/api/v1/documents", params=params)

        if not result:
            return []

        docs = result if isinstance(result, list) else result.get("documents", [])

        return [
            DocumentSummary(
                id=d.get("id", ""),
                name=d.get("name", ""),
                document_type=d.get("documentType", d.get("type", "")),
                folder_id=d.get("folderId"),
                created_at=d.get("createdAt", ""),
                updated_at=d.get("updatedAt", ""),
                model_id=d.get("modelId", ""),
            )
            for d in docs
        ]

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
        payload = {
            "baseModelId": base_model_id,
            "dashboard": export_data.get("dashboard", {}),
            "document": export_data.get("document", {}),
            "exportVersion": export_data.get("exportVersion", "0.1"),
            "workbookModel": export_data.get("workbookModel", {}),
        }

        if name:
            payload["document"]["name"] = name
        if folder_id:
            payload["document"]["folderId"] = folder_id

        result = self._client.post(
            "/api/unstable/documents/import",
            json=payload,
            timeout=60.0,
        )

        if not result or not isinstance(result, dict):
            raise OmniAPIError(0, "Empty response from dashboard import")

        return ImportResponse(
            document_id=result.get("id", result.get("documentId", "")),
            name=result.get("name", name or ""),
            success=True,
        )

    def delete_dashboard(self, document_id: str) -> None:
        """Delete a dashboard by ID."""
        self._client.delete(f"/api/v1/documents/{document_id}")
        logger.info("Deleted dashboard %s", document_id)

    def move_dashboard(
        self, document_id: str, folder_id: str
    ) -> None:
        """Move a dashboard to a different folder."""
        self._client.put(
            f"/api/v1/documents/{document_id}",
            json={"folderId": folder_id},
        )

    def add_label(self, document_id: str, label: str) -> None:
        """Add a label to a document."""
        self._client.post(f"/api/v1/documents/{document_id}/labels/{label}")

    def remove_label(self, document_id: str, label: str) -> None:
        """Remove a label from a document."""
        self._client.delete(f"/api/v1/documents/{document_id}/labels/{label}")

    def list_folders(self) -> list[dict[str, Any]]:
        """List all folders in the organization."""
        result = self._client.get("/api/v1/folders")
        if not result:
            return []
        return result if isinstance(result, list) else result.get("folders", [])

    def download_dashboard(
        self,
        dashboard_id: str,
        *,
        file_format: str = "pdf",
    ) -> bytes | dict:
        """Download a rendered dashboard (PDF, PNG, CSV).

        Note: This returns raw content for binary formats. The caller
        is responsible for writing to disk.
        """
        result = self._client.get(
            f"/api/v1/dashboards/{dashboard_id}/download",
            params={"format": file_format},
            timeout=120.0,
        )
        return result
