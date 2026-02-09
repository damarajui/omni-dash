"""Tests for the MCP server tools."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from omni_dash.mcp import server as mcp_server


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_globals():
    """Reset lazy-loaded globals between tests."""
    mcp_server._client = None
    mcp_server._doc_svc = None
    mcp_server._model_svc = None
    mcp_server._query_runner = None
    yield
    mcp_server._client = None
    mcp_server._doc_svc = None
    mcp_server._model_svc = None
    mcp_server._query_runner = None


@pytest.fixture
def mock_doc_svc():
    svc = MagicMock()
    mcp_server._doc_svc = svc
    return svc


@pytest.fixture
def mock_model_svc():
    svc = MagicMock()
    mcp_server._model_svc = svc
    return svc


@pytest.fixture
def mock_query_runner():
    runner = MagicMock()
    mcp_server._query_runner = runner
    return runner


@pytest.fixture
def mock_client():
    client = MagicMock()
    mcp_server._client = client
    return client


# ---------------------------------------------------------------------------
# list_dashboards
# ---------------------------------------------------------------------------


class TestListDashboards:
    def test_returns_dashboard_list(self, mock_doc_svc):
        from omni_dash.api.documents import DocumentSummary

        mock_doc_svc.list_dashboards.return_value = [
            DocumentSummary(
                id="abc123",
                name="SEO Funnel",
                document_type="dashboard",
                folder_id="folder1",
                updated_at="2026-01-01",
            ),
        ]

        result = json.loads(mcp_server.list_dashboards())
        assert len(result) == 1
        assert result[0]["id"] == "abc123"
        assert result[0]["name"] == "SEO Funnel"

    def test_passes_folder_id(self, mock_doc_svc):
        mock_doc_svc.list_dashboards.return_value = []
        mcp_server.list_dashboards(folder_id="f123")
        mock_doc_svc.list_dashboards.assert_called_once_with(folder_id="f123")

    def test_handles_error(self, mock_doc_svc):
        from omni_dash.exceptions import OmniAPIError

        mock_doc_svc.list_dashboards.side_effect = OmniAPIError(500, "boom")
        result = json.loads(mcp_server.list_dashboards())
        assert "error" in result


# ---------------------------------------------------------------------------
# get_dashboard
# ---------------------------------------------------------------------------


class TestGetDashboard:
    def test_returns_dashboard_details(self, mock_doc_svc):
        from omni_dash.api.documents import DashboardResponse

        mock_doc_svc.get_dashboard.return_value = DashboardResponse(
            document_id="abc123",
            name="My Dashboard",
            model_id="model1",
            query_presentations=[{"id": "qp1"}, {"id": "qp2"}],
            created_at="2026-01-01",
            updated_at="2026-01-02",
        )

        result = json.loads(mcp_server.get_dashboard("abc123"))
        assert result["document_id"] == "abc123"
        assert result["tile_count"] == 2

    def test_handles_not_found(self, mock_doc_svc):
        from omni_dash.exceptions import DocumentNotFoundError

        mock_doc_svc.get_dashboard.side_effect = DocumentNotFoundError("xyz")
        result = json.loads(mcp_server.get_dashboard("xyz"))
        assert "error" in result


# ---------------------------------------------------------------------------
# create_dashboard
# ---------------------------------------------------------------------------


class TestCreateDashboard:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_doc_svc")
    @patch("omni_dash.mcp.server.get_settings")
    def test_creates_dashboard(self, mock_settings, mock_get_doc_svc, _mock_model_id):
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        svc = MagicMock()
        svc.create_dashboard.return_value = DashboardResponse(
            document_id="skeleton123",
            name="Test Dashboard",
        )
        # Mock the export→reimport flow for vis config patching
        svc.export_dashboard.return_value = {
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Line Chart",
                                "visConfig": {
                                    "visType": None,
                                    "chartType": "line",
                                    "spec": {},
                                    "fields": [],
                                    "jsonHash": "stale",
                                },
                            }
                        }
                    ]
                }
            },
            "document": {"sharedModelId": "model-123"},
            "workbookModel": {},
            "exportVersion": "0.1",
        }
        svc.import_dashboard.return_value = ImportResponse(
            document_id="final123",
            name="Test Dashboard",
        )
        mock_get_doc_svc.return_value = svc

        tiles = [
            {
                "name": "Line Chart",
                "chart_type": "line",
                "query": {
                    "table": "mart_seo",
                    "fields": ["mart_seo.week", "mart_seo.visits"],
                },
                "vis_config": {
                    "x_axis": "mart_seo.week",
                    "y_axis": ["mart_seo.visits"],
                },
                "size": "half",
            }
        ]

        result = json.loads(mcp_server.create_dashboard(
            name="Test Dashboard",
            tiles=tiles,
        ))
        assert result["status"] == "created"
        assert result["dashboard_id"] == "final123"
        assert "url" in result
        # Verify skeleton was deleted
        svc.delete_dashboard.assert_called_once_with("skeleton123")

    @patch.object(mcp_server, "_get_shared_model_id", return_value="")
    def test_returns_error_without_model_id(self, _):
        result = json.loads(mcp_server.create_dashboard(
            name="Test",
            tiles=[],
        ))
        assert "error" in result

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_handles_validation_error(self, _):
        # Empty tiles should still create a definition (it may fail later at Omni)
        # but invalid chart_type will fail Pydantic validation
        tiles = [
            {
                "name": "Bad",
                "chart_type": "nonexistent_type",
                "query": {"table": "t", "fields": ["t.f"]},
            }
        ]
        result = json.loads(mcp_server.create_dashboard(
            name="Test",
            tiles=tiles,
        ))
        assert "error" in result


# ---------------------------------------------------------------------------
# delete_dashboard
# ---------------------------------------------------------------------------


class TestDeleteDashboard:
    def test_deletes_successfully(self, mock_doc_svc):
        mock_doc_svc.delete_dashboard.return_value = None
        result = json.loads(mcp_server.delete_dashboard("abc123"))
        assert result["status"] == "deleted"

    def test_handles_error(self, mock_doc_svc):
        from omni_dash.exceptions import DocumentNotFoundError

        mock_doc_svc.delete_dashboard.side_effect = DocumentNotFoundError("xyz")
        result = json.loads(mcp_server.delete_dashboard("xyz"))
        assert "error" in result


# ---------------------------------------------------------------------------
# export_dashboard
# ---------------------------------------------------------------------------


class TestExportDashboard:
    def test_exports_dashboard(self, mock_doc_svc):
        mock_doc_svc.export_dashboard.return_value = {
            "document": {"name": "Exported"},
            "dashboard": {"tiles": []},
        }
        result = json.loads(mcp_server.export_dashboard("abc123"))
        assert result["document"]["name"] == "Exported"


# ---------------------------------------------------------------------------
# list_topics
# ---------------------------------------------------------------------------


class TestListTopics:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_returns_topics(self, _, mock_model_svc):
        from omni_dash.api.models import TopicSummary

        mock_model_svc.list_topics.return_value = [
            TopicSummary(name="mart_seo", label="SEO", description="SEO data", base_view="mart_seo_view"),
            TopicSummary(name="mart_paid", label="Paid", description="Paid data", base_view="mart_paid_view"),
        ]

        result = json.loads(mcp_server.list_topics())
        assert len(result) == 2
        assert result[0]["name"] == "mart_seo"

    @patch.object(mcp_server, "_get_shared_model_id", return_value="")
    def test_returns_error_without_model(self, _):
        result = json.loads(mcp_server.list_topics())
        assert "error" in result


# ---------------------------------------------------------------------------
# get_topic_fields
# ---------------------------------------------------------------------------


class TestGetTopicFields:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_returns_topic_detail(self, _, mock_model_svc):
        from omni_dash.api.models import TopicDetail

        mock_model_svc.get_topic.return_value = TopicDetail(
            name="mart_seo",
            label="SEO Funnel",
            fields=[{"name": "week_start", "type": "date"}],
            views=[{"name": "mart_seo"}],
        )

        result = json.loads(mcp_server.get_topic_fields("mart_seo"))
        assert result["name"] == "mart_seo"
        assert len(result["fields"]) == 1


# ---------------------------------------------------------------------------
# query_data
# ---------------------------------------------------------------------------


class TestQueryData:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_query_runner")
    def test_runs_query(self, mock_get_runner, _):
        from omni_dash.api.queries import QueryResult

        runner = MagicMock()
        runner.run.return_value = QueryResult(
            fields=["mart_seo.week", "mart_seo.visits"],
            rows=[{"mart_seo.week": "2026-01-01", "mart_seo.visits": 100}],
            row_count=1,
        )
        mock_get_runner.return_value = runner

        result = json.loads(mcp_server.query_data(
            table="mart_seo",
            fields=["mart_seo.week", "mart_seo.visits"],
            limit=10,
        ))
        assert result["row_count"] == 1
        assert len(result["rows"]) == 1

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_query_runner")
    def test_caps_limit_at_1000(self, mock_get_runner, _):
        from omni_dash.api.queries import QueryResult

        runner = MagicMock()
        runner.run.return_value = QueryResult(fields=[], rows=[], row_count=0)
        mock_get_runner.return_value = runner

        mcp_server.query_data(
            table="t",
            fields=["t.f"],
            limit=5000,
        )
        spec = runner.run.call_args[0][0]
        assert spec.limit == 1000


# ---------------------------------------------------------------------------
# list_folders
# ---------------------------------------------------------------------------


class TestListFolders:
    def test_returns_folders(self, mock_doc_svc):
        mock_doc_svc.list_folders.return_value = [
            {"id": "f1", "name": "Folder 1", "parentId": None},
            {"id": "f2", "name": "Subfolder", "parentId": "f1"},
        ]
        result = json.loads(mcp_server.list_folders())
        assert len(result) == 2
        assert result[1]["parent_id"] == "f1"


# ---------------------------------------------------------------------------
# import_dashboard
# ---------------------------------------------------------------------------


SAMPLE_EXPORT = {
    "document": {"name": "Exported", "sharedModelId": "model-abc", "folderId": "f1"},
    "dashboard": {"model": {"baseModelId": "model-abc"}, "tiles": []},
    "workbookModel": {"base_model_id": "model-abc"},
    "exportVersion": "0.1",
}


class TestImportDashboard:
    def test_imports_successfully(self, mock_doc_svc):
        from omni_dash.api.documents import ImportResponse

        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="imported-1", name="My Import"
        )

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.import_dashboard(
                export_data=SAMPLE_EXPORT,
            ))

        assert result["status"] == "imported"
        assert result["dashboard_id"] == "imported-1"
        assert "url" in result

    def test_passes_name_override(self, mock_doc_svc):
        from omni_dash.api.documents import ImportResponse

        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="x", name="Override"
        )

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            mcp_server.import_dashboard(
                export_data=SAMPLE_EXPORT, name="Override"
            )

        _, kwargs = mock_doc_svc.import_dashboard.call_args
        assert kwargs["name"] == "Override"

    def test_resolves_model_from_export(self, mock_doc_svc):
        from omni_dash.api.documents import ImportResponse

        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="x", name="X"
        )

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            mcp_server.import_dashboard(export_data=SAMPLE_EXPORT)

        _, kwargs = mock_doc_svc.import_dashboard.call_args
        assert kwargs["base_model_id"] == "model-abc"

    def test_handles_error(self, mock_doc_svc):
        from omni_dash.exceptions import OmniAPIError

        mock_doc_svc.import_dashboard.side_effect = OmniAPIError(400, "bad")
        result = json.loads(mcp_server.import_dashboard(export_data=SAMPLE_EXPORT))
        assert "error" in result


# ---------------------------------------------------------------------------
# clone_dashboard
# ---------------------------------------------------------------------------


class TestCloneDashboard:
    def test_clones_successfully(self, mock_doc_svc):
        from omni_dash.api.documents import ImportResponse

        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="clone-1", name="Cloned"
        )

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.clone_dashboard(
                dashboard_id="orig-1", new_name="Cloned"
            ))

        assert result["status"] == "cloned"
        assert result["source_dashboard_id"] == "orig-1"
        assert result["new_dashboard_id"] == "clone-1"

    def test_passes_folder_override(self, mock_doc_svc):
        from omni_dash.api.documents import ImportResponse

        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="x", name="X"
        )

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            mcp_server.clone_dashboard(
                dashboard_id="orig", new_name="X", folder_id="new-folder"
            )

        _, kwargs = mock_doc_svc.import_dashboard.call_args
        assert kwargs["folder_id"] == "new-folder"

    def test_handles_export_error(self, mock_doc_svc):
        from omni_dash.exceptions import DocumentNotFoundError

        mock_doc_svc.export_dashboard.side_effect = DocumentNotFoundError("bad")
        result = json.loads(mcp_server.clone_dashboard(
            dashboard_id="bad", new_name="X"
        ))
        assert "error" in result


# ---------------------------------------------------------------------------
# move_dashboard
# ---------------------------------------------------------------------------


class TestMoveDashboard:
    def test_moves_successfully(self, mock_doc_svc):
        from omni_dash.api.documents import ImportResponse

        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="moved-1", name="Exported"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.move_dashboard(
                dashboard_id="orig-1", target_folder_id="f2"
            ))

        assert result["status"] == "moved"
        assert result["old_dashboard_id"] == "orig-1"
        assert result["new_dashboard_id"] == "moved-1"
        assert result["target_folder_id"] == "f2"

    def test_deletes_original(self, mock_doc_svc):
        from omni_dash.api.documents import ImportResponse

        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="x", name="X"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            mcp_server.move_dashboard(dashboard_id="orig-1", target_folder_id="f2")

        mock_doc_svc.delete_dashboard.assert_called_once_with("orig-1")

    def test_partial_on_delete_failure(self, mock_doc_svc):
        from omni_dash.api.documents import ImportResponse
        from omni_dash.exceptions import OmniAPIError

        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="new-1", name="X"
        )
        mock_doc_svc.delete_dashboard.side_effect = OmniAPIError(500, "fail")

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.move_dashboard(
                dashboard_id="orig-1", target_folder_id="f2"
            ))

        assert result["status"] == "partial"
        assert "warning" in result
        assert result["old_dashboard_id"] == "orig-1"
        assert result["new_dashboard_id"] == "new-1"

    def test_handles_export_error(self, mock_doc_svc):
        from omni_dash.exceptions import DocumentNotFoundError

        mock_doc_svc.export_dashboard.side_effect = DocumentNotFoundError("bad")
        result = json.loads(mcp_server.move_dashboard(
            dashboard_id="bad", target_folder_id="f2"
        ))
        assert "error" in result


# ---------------------------------------------------------------------------
# update_dashboard
# ---------------------------------------------------------------------------


class TestUpdateDashboard:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch("omni_dash.mcp.server.get_settings")
    def test_updates_with_new_tiles(self, mock_settings, _, mock_doc_svc):
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT
        mock_doc_svc.create_dashboard.return_value = DashboardResponse(
            document_id="skeleton-1", name="Updated"
        )
        # _create_with_vis_configs will export the skeleton then reimport
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="updated-1", name="Updated"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        tiles = [{
            "name": "New Chart",
            "chart_type": "bar",
            "query": {"table": "t", "fields": ["t.a", "t.b"]},
            "vis_config": {"x_axis": "t.a", "y_axis": ["t.b"]},
            "size": "half",
        }]

        result = json.loads(mcp_server.update_dashboard(
            dashboard_id="orig-1", tiles=tiles, name="Updated"
        ))

        assert result["status"] == "updated"
        assert result["old_dashboard_id"] == "orig-1"
        assert result["new_dashboard_id"] == "updated-1"

    @patch("omni_dash.mcp.server.get_settings")
    def test_updates_name_only(self, mock_settings, mock_doc_svc):
        from omni_dash.api.documents import ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="renamed-1", name="New Name"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        result = json.loads(mcp_server.update_dashboard(
            dashboard_id="orig-1", name="New Name"
        ))

        assert result["status"] == "updated"
        assert result["name"] == "New Name"
        mock_doc_svc.import_dashboard.assert_called_once()

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch("omni_dash.mcp.server.get_settings")
    def test_partial_on_delete_failure(self, mock_settings, _, mock_doc_svc):
        from omni_dash.api.documents import DashboardResponse, ImportResponse
        from omni_dash.exceptions import OmniAPIError

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT
        mock_doc_svc.create_dashboard.return_value = DashboardResponse(
            document_id="skeleton-1", name="X"
        )
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="new-1", name="X"
        )
        # All delete calls fail — skeleton delete caught silently, orig delete → partial
        mock_doc_svc.delete_dashboard.side_effect = OmniAPIError(500, "fail")

        tiles = [{
            "name": "Chart",
            "chart_type": "line",
            "query": {"table": "t", "fields": ["t.a"]},
            "vis_config": {},
            "size": "half",
        }]

        result = json.loads(mcp_server.update_dashboard(
            dashboard_id="orig-1", tiles=tiles
        ))

        assert result["status"] == "partial"
        assert "warning" in result

    def test_handles_export_error(self, mock_doc_svc):
        from omni_dash.exceptions import DocumentNotFoundError

        mock_doc_svc.export_dashboard.side_effect = DocumentNotFoundError("bad")
        result = json.loads(mcp_server.update_dashboard(dashboard_id="bad"))
        assert "error" in result


# ---------------------------------------------------------------------------
# add_tiles_to_dashboard
# ---------------------------------------------------------------------------


class TestAddTilesToDashboard:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch("omni_dash.mcp.server.get_settings")
    @patch("omni_dash.mcp.server.DashboardSerializer")
    def test_adds_tiles(self, mock_serializer, mock_settings, _, mock_doc_svc):
        from omni_dash.api.documents import DashboardResponse, ImportResponse
        from omni_dash.dashboard.definition import (
            DashboardDefinition,
            Tile,
            TileQuery,
            TileVisConfig,
        )

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT

        existing_tile = Tile(
            name="Existing",
            chart_type="line",
            query=TileQuery(table="t", fields=["t.x"]),
            vis_config=TileVisConfig(),
        )
        mock_serializer.from_omni_export.return_value = DashboardDefinition(
            name="Dashboard",
            model_id="model-123",
            tiles=[existing_tile],
        )
        mock_serializer.to_omni_create_payload.return_value = {"name": "Dashboard"}

        mock_doc_svc.create_dashboard.return_value = DashboardResponse(
            document_id="skeleton-1", name="Dashboard"
        )
        # _create_with_vis_configs reimport
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="updated-1", name="Dashboard"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        new_tiles = [{
            "name": "New Bar",
            "chart_type": "bar",
            "query": {"table": "t", "fields": ["t.a", "t.b"]},
            "vis_config": {"x_axis": "t.a", "y_axis": ["t.b"]},
            "size": "half",
        }]

        result = json.loads(mcp_server.add_tiles_to_dashboard(
            dashboard_id="orig-1", tiles=new_tiles
        ))

        assert result["status"] == "updated"
        assert result["previous_tile_count"] == 1
        assert result["tiles_added"] == 1
        assert result["new_tile_count"] == 2

    def test_rejects_empty_tiles(self, mock_doc_svc):
        result = json.loads(mcp_server.add_tiles_to_dashboard(
            dashboard_id="x", tiles=[]
        ))
        assert "error" in result

    def test_handles_export_error(self, mock_doc_svc):
        from omni_dash.exceptions import DocumentNotFoundError

        mock_doc_svc.export_dashboard.side_effect = DocumentNotFoundError("bad")
        result = json.loads(mcp_server.add_tiles_to_dashboard(
            dashboard_id="bad",
            tiles=[{"name": "X", "chart_type": "line", "query": {"table": "t", "fields": ["t.f"]}}],
        ))
        assert "error" in result

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch("omni_dash.mcp.server.get_settings")
    @patch("omni_dash.mcp.server.DashboardSerializer")
    def test_partial_on_delete_failure(self, mock_serializer, mock_settings, _, mock_doc_svc):
        from omni_dash.api.documents import DashboardResponse
        from omni_dash.dashboard.definition import DashboardDefinition
        from omni_dash.exceptions import OmniAPIError

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT
        mock_serializer.from_omni_export.return_value = DashboardDefinition(
            name="D", model_id="m", tiles=[]
        )
        mock_serializer.to_omni_create_payload.return_value = {"name": "D"}
        mock_doc_svc.create_dashboard.return_value = DashboardResponse(
            document_id="new-1", name="D"
        )
        mock_doc_svc.delete_dashboard.side_effect = OmniAPIError(500, "fail")

        tiles = [{
            "name": "X",
            "chart_type": "line",
            "query": {"table": "t", "fields": ["t.f"]},
            "vis_config": {},
            "size": "half",
        }]

        result = json.loads(mcp_server.add_tiles_to_dashboard(
            dashboard_id="orig-1", tiles=tiles
        ))

        assert result["status"] == "partial"
        assert "warning" in result


# ---------------------------------------------------------------------------
# Helper: _resolve_model_id_from_export
# ---------------------------------------------------------------------------


class TestResolveModelIdFromExport:
    def test_uses_user_override(self):
        result = mcp_server._resolve_model_id_from_export({}, "user-model")
        assert result == "user-model"

    def test_uses_document_shared_model_id(self):
        export = {"document": {"sharedModelId": "doc-model"}}
        result = mcp_server._resolve_model_id_from_export(export)
        assert result == "doc-model"

    def test_uses_workbook_model(self):
        export = {"document": {}, "workbookModel": {"base_model_id": "wb-model"}}
        result = mcp_server._resolve_model_id_from_export(export)
        assert result == "wb-model"

    def test_uses_dashboard_model(self):
        export = {
            "document": {},
            "workbookModel": {},
            "dashboard": {"model": {"baseModelId": "dash-model"}},
        }
        result = mcp_server._resolve_model_id_from_export(export)
        assert result == "dash-model"

    @patch.object(mcp_server, "_get_shared_model_id", return_value="fallback")
    def test_falls_back_to_discovery(self, _):
        result = mcp_server._resolve_model_id_from_export({"document": {}})
        assert result == "fallback"


# ---------------------------------------------------------------------------
# MCP tool registration
# ---------------------------------------------------------------------------


class TestMCPRegistration:
    def test_all_tools_registered(self):
        import asyncio

        async def check():
            tools = await mcp_server.mcp.list_tools()
            return {t.name for t in tools}

        names = asyncio.run(check())
        expected = {
            "list_dashboards",
            "get_dashboard",
            "create_dashboard",
            "update_dashboard",
            "add_tiles_to_dashboard",
            "delete_dashboard",
            "clone_dashboard",
            "move_dashboard",
            "import_dashboard",
            "export_dashboard",
            "list_topics",
            "get_topic_fields",
            "query_data",
            "list_folders",
        }
        assert expected == names

    def test_tool_count(self):
        import asyncio

        async def check():
            return len(await mcp_server.mcp.list_tools())

        assert asyncio.run(check()) == 14
