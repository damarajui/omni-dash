"""Tests for omni_dash.api.documents — DocumentService CRUD."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from omni_dash.api.documents import (
    DashboardResponse,
    DocumentService,
    DocumentSummary,
    ImportResponse,
)
from omni_dash.exceptions import DocumentNotFoundError, OmniAPIError


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def service(mock_client):
    return DocumentService(mock_client)


class TestCreateDashboard:
    def test_returns_response(self, service, mock_client):
        mock_client.post.return_value = {
            "id": "doc-123",
            "name": "My Dashboard",
            "modelId": "m-1",
        }
        result = service.create_dashboard({"name": "My Dashboard", "modelId": "m-1"})
        assert isinstance(result, DashboardResponse)
        assert result.document_id == "doc-123"
        assert result.name == "My Dashboard"

    def test_does_not_mutate_payload(self, service, mock_client):
        mock_client.post.return_value = {"id": "x", "name": "n"}
        original = {"name": "n", "modelId": "m"}
        service.create_dashboard(original, folder_id="f-1")
        assert "folderId" not in original

    def test_adds_folder_id(self, service, mock_client):
        mock_client.post.return_value = {"id": "x", "name": "n"}
        service.create_dashboard({"name": "n"}, folder_id="f-1")
        call_payload = mock_client.post.call_args[1]["json"]
        assert call_payload["folderId"] == "f-1"

    def test_empty_response_raises(self, service, mock_client):
        mock_client.post.return_value = None
        with pytest.raises(OmniAPIError, match="Empty response"):
            service.create_dashboard({})


class TestGetDashboard:
    def test_returns_dashboard(self, service, mock_client):
        mock_client.get.return_value = {
            "id": "doc-1",
            "name": "Test",
            "modelId": "m",
            "queryPresentations": [{"id": "qp1"}],
        }
        result = service.get_dashboard("doc-1")
        assert result.document_id == "doc-1"
        assert len(result.query_presentations) == 1

    def test_not_found(self, service, mock_client):
        mock_client.get.return_value = None
        with pytest.raises(DocumentNotFoundError):
            service.get_dashboard("missing")


class TestListDashboards:
    def test_returns_list(self, service, mock_client):
        mock_client.get.return_value = [
            {"id": "d1", "name": "A", "documentType": "dashboard"},
            {"id": "d2", "name": "B", "documentType": "dashboard"},
        ]
        result = service.list_dashboards()
        assert len(result) == 2
        assert all(isinstance(d, DocumentSummary) for d in result)

    def test_empty_returns_empty(self, service, mock_client):
        mock_client.get.return_value = None
        assert service.list_dashboards() == []

    def test_dict_response_format(self, service, mock_client):
        mock_client.get.return_value = {
            "documents": [{"id": "d1", "name": "A"}]
        }
        result = service.list_dashboards()
        assert len(result) == 1


class TestExportDashboard:
    def test_returns_dict(self, service, mock_client):
        mock_client.get.return_value = {"exportVersion": "0.1", "document": {}}
        result = service.export_dashboard("doc-1")
        assert "exportVersion" in result

    def test_not_found(self, service, mock_client):
        mock_client.get.return_value = None
        with pytest.raises(DocumentNotFoundError):
            service.export_dashboard("missing")


class TestImportDashboard:
    def test_returns_response(self, service, mock_client):
        mock_client.post.return_value = {"id": "new-doc", "name": "Imported"}
        result = service.import_dashboard(
            {"document": {"name": "X"}, "exportVersion": "0.1"},
            base_model_id="m-1",
        )
        assert isinstance(result, ImportResponse)
        assert result.document_id == "new-doc"

    def test_does_not_mutate_export_data(self, service, mock_client):
        mock_client.post.return_value = {"id": "x", "name": "n"}
        doc = {"name": "Original"}
        export_data = {"document": doc, "exportVersion": "0.1"}
        service.import_dashboard(export_data, base_model_id="m", name="Override")
        assert doc["name"] == "Original"  # not mutated


class TestDeleteDashboard:
    def test_calls_delete(self, service, mock_client):
        mock_client.delete.return_value = None
        service.delete_dashboard("doc-1")
        mock_client.delete.assert_called_once_with("/api/v1/documents/doc-1")


class TestDownloadDashboard:
    def test_returns_bytes(self, service, mock_client):
        mock_client.get_raw.return_value = b"pdf-bytes"
        result = service.download_dashboard("d1", file_format="pdf")
        assert result == b"pdf-bytes"

    def test_invalid_format_raises(self, service, mock_client):
        with pytest.raises(ValueError, match="file_format must be"):
            service.download_dashboard("d1", file_format="docx")

    def test_csv_format(self, service, mock_client):
        mock_client.get_raw.return_value = b"a,b\n1,2"
        result = service.download_dashboard("d1", file_format="csv")
        assert result == b"a,b\n1,2"
