"""Tests for the MCP server tools."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from omni_dash.exceptions import OmniAPIError
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
    mcp_server._shared_model_id = ""
    yield
    mcp_server._client = None
    mcp_server._doc_svc = None
    mcp_server._model_svc = None
    mcp_server._query_runner = None
    mcp_server._shared_model_id = ""


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
            tiles=[{"name": "T", "chart_type": "line", "query": {"table": "t", "fields": ["t.f"]}}],
        ))
        assert "error" in result
        assert "model_id" in result["error"].lower() or "OMNI_SHARED_MODEL_ID" in result["error"]

    def test_returns_error_with_empty_tiles(self):
        result = json.loads(mcp_server.create_dashboard(
            name="Test",
            tiles=[],
        ))
        assert "error" in result
        assert "empty" in result["error"].lower()

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

        mock_model_svc.get_topic_native.return_value = TopicDetail(
            name="mart_seo",
            label="SEO Funnel",
            fields=[{"name": "week_start", "type": "date"}],
            views=[{"name": "mart_seo"}],
        )

        result = json.loads(mcp_server.get_topic_fields("mart_seo"))
        assert result["name"] == "mart_seo"
        assert len(result["fields"]) == 1

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_truncates_large_field_lists(self, _, mock_model_svc):
        from omni_dash.api.models import TopicDetail

        big_fields = [{"name": f"field_{i}", "type": "dimension"} for i in range(300)]
        mock_model_svc.get_topic_native.return_value = TopicDetail(
            name="big_topic",
            label="Big",
            fields=big_fields,
            views=[],
        )

        result = json.loads(mcp_server.get_topic_fields("big_topic"))
        assert result["field_count"] == 300
        assert len(result["fields"]) == 200
        assert result["truncated"] is True
        assert "200 of 300" in result["note"]


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

    def test_empty_table_returns_error(self):
        result = json.loads(mcp_server.query_data(table="", fields=["t.f"]))
        assert "error" in result
        assert "table" in result["error"]

    def test_empty_fields_returns_error(self):
        result = json.loads(mcp_server.query_data(table="t", fields=[]))
        assert "error" in result
        assert "fields" in result["error"]

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_query_runner")
    @patch.object(mcp_server, "_resolve_table_name", return_value="mart_google_ads_performance")
    def test_resolves_topic_with_spaces(self, mock_resolve, mock_get_runner, _):
        from omni_dash.api.queries import QueryResult

        runner = MagicMock()
        runner.run.return_value = QueryResult(
            fields=["mart_google_ads_performance.spend"],
            rows=[{"mart_google_ads_performance.spend": 100}],
            row_count=1,
        )
        mock_get_runner.return_value = runner

        result = json.loads(mcp_server.query_data(
            table="Google Ads Performance",
            fields=["mart_google_ads_performance.spend"],
        ))
        assert result["row_count"] == 1
        mock_resolve.assert_called_once_with("Google Ads Performance", "model-123")


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
    """Tests for the raw-export-injection add_tiles flow."""

    # Rich sample export with QPC structure and layout
    _EXPORT_WITH_QPC = {
        "document": {"name": "Dashboard", "sharedModelId": "model-abc", "folderId": "f1"},
        "dashboard": {
            "model": {"baseModelId": "model-abc"},
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    {
                        "queryPresentation": {
                            "name": "Existing Tile",
                            "query": {"queryJson": {"table": "t", "fields": ["t.x"]}},
                            "visConfig": {"visType": "basic", "chartType": "line"},
                        }
                    }
                ]
            },
            "metadata": {
                "layouts": {
                    "lg": [{"i": "1", "x": 0, "y": 0, "w": 12, "h": 40}]
                }
            },
            "filterConfig": {"abc123": {"fieldName": "t.date", "kind": "date_range"}},
            "filterOrder": ["abc123"],
        },
        "workbookModel": {"base_model_id": "model-abc"},
        "exportVersion": "0.1",
    }

    # Temp skeleton export (returned when exporting the new-tile skeleton)
    _TEMP_EXPORT = {
        "document": {"name": "__add_tiles_tmp__", "sharedModelId": "model-abc"},
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    {
                        "queryPresentation": {
                            "name": "New Bar",
                            "query": {"queryJson": {"table": "t", "fields": ["t.a", "t.b"]}},
                            "visConfig": {"visType": "basic", "chartType": "bar"},
                        }
                    }
                ]
            },
            "metadata": {
                "layouts": {
                    "lg": [{"i": "1", "x": 0, "y": 0, "w": 6, "h": 40}]
                }
            },
        },
        "workbookModel": {"base_model_id": "model-abc"},
        "exportVersion": "0.1",
    }

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch("omni_dash.mcp.server.get_settings")
    @patch("omni_dash.mcp.server.DashboardSerializer")
    def test_adds_tiles(self, mock_serializer, mock_settings, _, mock_doc_svc):
        import copy
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_serializer.to_omni_create_payload.return_value = {
            "name": "__add_tiles_tmp__",
            "queryPresentations": [{"name": "New Bar", "visConfig": {"visType": "basic", "chartType": "bar"}}],
        }

        # _create_with_vis_configs skeleton export (minimal — vis config patching uses this)
        skeleton_export = {
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {"queryPresentation": {"name": "New Bar", "visConfig": {"visType": "basic"}}}
                    ]
                }
            },
            "workbookModel": {"base_model_id": "model-abc"},
        }

        # export_dashboard called 3 times:
        # 1. Original dashboard, 2. Skeleton (inside _create_with_vis_configs), 3. Temp (after vis patching)
        mock_doc_svc.export_dashboard.side_effect = [
            copy.deepcopy(self._EXPORT_WITH_QPC),  # Original
            copy.deepcopy(skeleton_export),          # Skeleton (inside _create_with_vis_configs)
            copy.deepcopy(self._TEMP_EXPORT),        # Temp export (for merging)
        ]
        mock_doc_svc.create_dashboard.return_value = DashboardResponse(
            document_id="skeleton-1", name="__add_tiles_tmp__"
        )
        # import_dashboard called 2 times:
        # 1. _create_with_vis_configs reimport, 2. Final merged reimport
        mock_doc_svc.import_dashboard.side_effect = [
            ImportResponse(document_id="temp-1", name="__add_tiles_tmp__"),  # vis config patched
            ImportResponse(document_id="updated-1", name="Dashboard"),       # final merged
        ]
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

        # Verify the merged export preserves original filterConfig
        # The final import_dashboard call is the 2nd one
        import_call = mock_doc_svc.import_dashboard.call_args_list[-1]
        imported_data = import_call[0][0] if import_call[0] else import_call[1].get("export_data")
        if imported_data:
            dash = imported_data.get("dashboard", {})
            assert dash.get("filterConfig") == {"abc123": {"fieldName": "t.date", "kind": "date_range"}}
            assert dash.get("filterOrder") == ["abc123"]
            memberships = dash["queryPresentationCollection"]["queryPresentationCollectionMemberships"]
            assert len(memberships) == 2

            # Layout i values must be strings (Omni rejects integers)
            layouts = dash.get("metadata", {}).get("layouts", {}).get("lg", [])
            for layout_item in layouts:
                assert isinstance(layout_item["i"], str), (
                    f"Layout i must be string, got {type(layout_item['i']).__name__}: {layout_item['i']}"
                )

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
    def test_delete_failure_still_succeeds(self, mock_serializer, mock_settings, _, mock_doc_svc):
        """Delete failures are silently ignored — new dashboard is still created."""
        import copy
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_serializer.to_omni_create_payload.return_value = {
            "name": "__add_tiles_tmp__",
            "queryPresentations": [{"name": "X", "visConfig": {"visType": "basic", "chartType": "line"}}],
        }

        skeleton_export = {
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {"queryPresentation": {"name": "X", "visConfig": {"visType": "basic"}}}
                    ]
                }
            },
            "workbookModel": {"base_model_id": "model-abc"},
        }

        # 3 export calls: original, skeleton (inside _create_with_vis_configs), temp
        mock_doc_svc.export_dashboard.side_effect = [
            copy.deepcopy(self._EXPORT_WITH_QPC),
            copy.deepcopy(skeleton_export),
            copy.deepcopy(self._TEMP_EXPORT),
        ]
        mock_doc_svc.create_dashboard.return_value = DashboardResponse(
            document_id="skeleton-1", name="tmp"
        )
        # 2 import calls: vis config reimport, final merged
        mock_doc_svc.import_dashboard.side_effect = [
            ImportResponse(document_id="temp-1", name="tmp"),
            ImportResponse(document_id="new-1", name="D"),
        ]
        # Delete failures are silently caught (called for skeleton + original + temp)
        mock_doc_svc.delete_dashboard.side_effect = Exception("fail")

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

        # Delete failures are best-effort — the new dashboard is still created
        assert result["status"] == "updated"
        assert result["dashboard_id"] == "new-1"


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
            "update_tile",
            "delete_dashboard",
            "clone_dashboard",
            "move_dashboard",
            "import_dashboard",
            "export_dashboard",
            "list_topics",
            "get_topic_fields",
            "query_data",
            "list_folders",
            "suggest_chart",
            "validate_dashboard",
            "profile_data",
            "generate_dashboard",
            "ai_generate_query",
            "ai_pick_topic",
            "ai_analyze",
            "get_dashboard_filters",
            "update_dashboard_filters",
        }
        assert expected == names

    def test_tool_count(self):
        import asyncio

        async def check():
            return len(await mcp_server.mcp.list_tools())

        assert asyncio.run(check()) == 24


# ---------------------------------------------------------------------------
# Bug 4: profile_data fallback for non-topic views
# ---------------------------------------------------------------------------


class TestProfileDataFallback:
    """profile_data should fall back to query discovery for non-topic views."""

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_profile_data_topic_success(self, _mock_mid, mock_model_svc, mock_query_runner):
        from omni_dash.api.models import TopicDetail
        from omni_dash.api.queries import QueryResult

        mock_model_svc.get_topic_native.return_value = TopicDetail(
            name="mart_seo",
            label="SEO",
            fields=[{"name": "week_start"}, {"name": "visits"}],
        )
        mock_query_runner.run.return_value = QueryResult(
            fields=["mart_seo.week_start", "mart_seo.visits"],
            rows=[
                {"mart_seo.week_start": "2026-01-01", "mart_seo.visits": 100},
            ],
            row_count=1,
        )

        result = json.loads(mcp_server.profile_data("mart_seo"))
        assert "mart_seo.week_start" in result["fields"]
        assert "mart_seo.visits" in result["fields"]
        assert "error" not in result

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_profile_data_non_topic_returns_helpful_error(self, _mock_mid, mock_model_svc, mock_query_runner):
        """When topic introspection fails, return a clear error with guidance."""
        mock_model_svc.get_topic_native.side_effect = Exception("Topic not found")

        result = json.loads(mcp_server.profile_data("unknown_view"))
        assert "error" in result
        assert "unknown_view" in result["error"]
        assert "Pass explicit field names" in result["error"]

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_profile_data_with_explicit_fields_bypasses_discovery(self, _mock_mid, mock_model_svc, mock_query_runner):
        """Passing explicit fields skips topic introspection entirely."""
        from omni_dash.api.queries import QueryResult

        mock_query_runner.run.return_value = QueryResult(
            fields=["mart_seo.visits"],
            rows=[{"mart_seo.visits": 100}],
            row_count=1,
        )

        result = json.loads(mcp_server.profile_data(
            "mart_seo", fields=["mart_seo.visits"],
        ))
        assert "error" not in result
        assert "mart_seo.visits" in result["fields"]
        mock_model_svc.get_topic_native.assert_not_called()


# ---------------------------------------------------------------------------
# suggest_chart
# ---------------------------------------------------------------------------


class TestSuggestChart:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_date_and_measure_recommends_line(self, _, mock_model_svc):
        from omni_dash.api.models import TopicDetail

        mock_model_svc.get_topic_native.return_value = TopicDetail(
            name="mart_seo",
            label="SEO",
            fields=[
                {"name": "week_start", "type": "date", "label": "Week Start"},
                {"name": "visits", "type": "number", "label": "Visits", "aggregation": "sum"},
            ],
        )

        result = json.loads(mcp_server.suggest_chart(table="mart_seo"))
        assert result["chart_type"] == "line"
        assert result["confidence"] > 0
        assert "fields_analyzed" in result
        assert result["fields_analyzed"] == 2

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_category_and_measure_recommends_bar(self, _, mock_model_svc):
        from omni_dash.api.models import TopicDetail

        mock_model_svc.get_topic_native.return_value = TopicDetail(
            name="mart_channels",
            label="Channels",
            fields=[
                {"name": "channel_name", "type": "string", "label": "Channel"},
                {"name": "revenue", "type": "number", "label": "Revenue", "aggregation": "sum"},
            ],
        )

        result = json.loads(mcp_server.suggest_chart(table="mart_channels"))
        assert result["chart_type"] == "bar"
        assert result["confidence"] > 0
        assert "alternatives" in result


    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_omni_dimension_types_still_recommend_line(self, _, mock_model_svc):
        """When Omni returns type='dimension' for all fields, name heuristics kick in."""
        from omni_dash.api.models import TopicDetail

        mock_model_svc.get_topic_native.return_value = TopicDetail(
            name="mart_ads",
            label="Ads",
            fields=[
                {"name": "date", "type": "dimension"},
                {"name": "spend", "type": "dimension"},
                {"name": "clicks", "type": "dimension"},
            ],
        )

        result = json.loads(mcp_server.suggest_chart(table="mart_ads"))
        assert result["chart_type"] == "line", (
            f"Expected line for date+spend+clicks (all dimension), got {result['chart_type']}"
        )
        assert result["confidence"] >= 0.85

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_handles_topic_error(self, _, mock_model_svc):
        from omni_dash.exceptions import OmniAPIError

        mock_model_svc.get_topic_native.side_effect = OmniAPIError(404, "Topic not found")

        result = json.loads(mcp_server.suggest_chart(table="nonexistent"))
        assert "error" in result


# ---------------------------------------------------------------------------
# validate_dashboard
# ---------------------------------------------------------------------------


class TestValidateDashboard:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_valid_spec_passes(self, _, mock_model_svc):
        # Mock get_topic so field existence checks pass
        from omni_dash.api.models import TopicDetail

        mock_model_svc.get_topic_native.return_value = TopicDetail(
            name="t",
            label="T",
            fields=[
                {"name": "date"},
                {"name": "value"},
            ],
        )

        tiles = [{
            "name": "Chart",
            "chart_type": "line",
            "query": {"table": "t", "fields": ["t.date", "t.value"]},
            "vis_config": {"x_axis": "t.date", "y_axis": ["t.value"]},
        }]
        result = json.loads(mcp_server.validate_dashboard(tiles=tiles))
        assert result["valid"] is True
        assert result["errors"] == []

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_invalid_chart_type_returns_error(self, _):
        """Tile with invalid chart_type returns a JSON error."""
        tiles = [{
            "name": "Bad Chart",
            "chart_type": "nonexistent_type",
            "query": {"table": "t", "fields": ["t.x"]},
        }]
        result = json.loads(mcp_server.validate_dashboard(tiles=tiles))
        assert "error" in result

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_missing_fields_returns_error(self, _):
        """Tile with empty fields list returns a JSON error."""
        tiles = [{
            "name": "Empty Fields",
            "chart_type": "line",
            "query": {"table": "t", "fields": []},
        }]
        result = json.loads(mcp_server.validate_dashboard(tiles=tiles))
        assert "error" in result

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_sort_field_not_in_query_warns(self, _, mock_model_svc):
        """Sort field not in query fields should produce a warning."""
        mock_model_svc.get_topic_native.side_effect = Exception("skip field check")

        tiles = [{
            "name": "Sorted Chart",
            "chart_type": "line",
            "query": {
                "table": "t",
                "fields": ["t.date", "t.value"],
                "sorts": [{"column_name": "t.other_field", "sort_descending": True}],
            },
            "vis_config": {"x_axis": "t.date", "y_axis": ["t.value"]},
        }]
        result = json.loads(mcp_server.validate_dashboard(tiles=tiles))
        assert result["valid"] is True  # Warnings don't make it invalid
        assert len(result["warnings"]) > 0
        assert any("t.other_field" in w for w in result["warnings"])


# ---------------------------------------------------------------------------
# generate_dashboard
# ---------------------------------------------------------------------------


class TestGenerateDashboard:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_create_with_vis_configs", return_value=("dash-gen-1", "SEO Trends"))
    @patch("omni_dash.mcp.server.DashboardSerializer")
    @patch("omni_dash.mcp.server.get_settings")
    def test_generates_dashboard(self, mock_settings, mock_serializer, _mock_create, _mock_mid, mock_model_svc):
        from omni_dash.dashboard.definition import DashboardDefinition, Tile, TileQuery

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_serializer.to_omni_create_payload.return_value = {
            "name": "SEO Trends",
            "queryPresentations": [{"queryIdentifierMapKey": "1"}],
        }

        mock_definition = DashboardDefinition(
            name="SEO Trends",
            model_id="model-123",
            tiles=[Tile(
                name="Visits Over Time",
                chart_type="line",
                query=TileQuery(table="mart_seo", fields=["mart_seo.week", "mart_seo.visits"]),
            )],
        )
        mock_ai_result = MagicMock()
        mock_ai_result.definition = mock_definition
        mock_ai_result.tool_calls_made = 3
        mock_ai_result.reasoning = "Generated from SEO data"

        mock_ai_cls = MagicMock()
        mock_ai_cls.return_value.generate.return_value = mock_ai_result

        with patch("omni_dash.ai.omni_adapter.OmniModelAdapter") as _mock_adapter, \
             patch("omni_dash.ai.service.DashboardAI", mock_ai_cls), \
             patch("omni_dash.mcp.server.LayoutManager") as mock_lm:
            mock_lm.auto_position.side_effect = lambda tiles: tiles
            result = json.loads(mcp_server.generate_dashboard(prompt="Show me SEO trends"))

        assert result["dashboard_id"] == "dash-gen-1"
        assert "url" in result
        assert result["tile_count"] == 1
        assert result["tool_calls_made"] == 3

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_generate_ai_failure_returns_error(self, _, mock_model_svc):
        """When the AI service raises, generate_dashboard returns an error."""
        mock_ai_cls = MagicMock()
        mock_ai_cls.return_value.generate.side_effect = RuntimeError("AI service unavailable")

        with patch("omni_dash.ai.omni_adapter.OmniModelAdapter"), \
             patch("omni_dash.ai.service.DashboardAI", mock_ai_cls):
            result = json.loads(mcp_server.generate_dashboard(prompt="anything"))

        assert "error" in result
        assert "AI service unavailable" in result["error"]

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_create_with_vis_configs")
    @patch("omni_dash.mcp.server.DashboardSerializer")
    @patch("omni_dash.mcp.server.get_settings")
    def test_generate_passes_folder_id(self, mock_settings, mock_serializer, mock_create, _, mock_model_svc):
        from omni_dash.dashboard.definition import DashboardDefinition, Tile, TileQuery

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_serializer.to_omni_create_payload.return_value = {
            "name": "D",
            "queryPresentations": [{"queryIdentifierMapKey": "1"}],
        }
        mock_create.return_value = ("dash-f1", "D")

        mock_definition = DashboardDefinition(
            name="D",
            model_id="model-123",
            tiles=[Tile(name="C", chart_type="bar", query=TileQuery(table="t", fields=["t.x"]))],
        )
        mock_ai_result = MagicMock()
        mock_ai_result.definition = mock_definition
        mock_ai_result.tool_calls_made = 1
        mock_ai_result.reasoning = "ok"

        mock_ai_cls = MagicMock()
        mock_ai_cls.return_value.generate.return_value = mock_ai_result

        with patch("omni_dash.ai.omni_adapter.OmniModelAdapter"), \
             patch("omni_dash.ai.service.DashboardAI", mock_ai_cls), \
             patch("omni_dash.mcp.server.LayoutManager"):
            mcp_server.generate_dashboard(prompt="test", folder_id="folder-abc")

        _, kwargs = mock_create.call_args
        assert kwargs["folder_id"] == "folder-abc"


# ---------------------------------------------------------------------------
# MCP Error Recovery
# ---------------------------------------------------------------------------


class TestQueryDataErrors:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_query_runner")
    def test_nonexistent_table(self, mock_get_runner, _):
        from omni_dash.exceptions import OmniAPIError

        runner = MagicMock()
        runner.run.side_effect = OmniAPIError(404, "Table not found")
        mock_get_runner.return_value = runner

        result = json.loads(mcp_server.query_data(
            table="nonexistent",
            fields=["nonexistent.x"],
        ))
        assert "error" in result

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_query_runner")
    def test_empty_result_returns_zero_rows(self, mock_get_runner, _):
        from omni_dash.api.queries import QueryResult

        runner = MagicMock()
        runner.run.return_value = QueryResult(fields=["t.x"], rows=[], row_count=0)
        mock_get_runner.return_value = runner

        result = json.loads(mcp_server.query_data(
            table="t",
            fields=["t.x"],
        ))
        assert result["row_count"] == 0
        assert result["rows"] == []


class TestProfileDataErrors:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_profile_data_empty_table(self, _, mock_model_svc, mock_query_runner):
        from omni_dash.api.models import TopicDetail
        from omni_dash.api.queries import QueryResult

        mock_model_svc.get_topic_native.return_value = TopicDetail(
            name="empty_table",
            label="Empty",
            fields=[{"name": "col1"}, {"name": "col2"}],
        )
        mock_query_runner.run.return_value = QueryResult(
            fields=["empty_table.col1", "empty_table.col2"],
            rows=[],
            row_count=0,
        )

        result = json.loads(mcp_server.profile_data("empty_table"))
        assert "error" not in result
        assert result["row_count"] == 0
        # Field profiles should still exist but with zero sample_count
        assert "empty_table.col1" in result["fields"]
        assert result["fields"]["empty_table.col1"]["sample_count"] == 0
        assert result["fields"]["empty_table.col1"]["null_count"] == 0


class TestCloneDashboardErrors:
    def test_handles_import_error_after_export(self, mock_doc_svc):
        from omni_dash.exceptions import OmniAPIError

        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT
        mock_doc_svc.import_dashboard.side_effect = OmniAPIError(500, "Import failed")

        result = json.loads(mcp_server.clone_dashboard(
            dashboard_id="orig", new_name="Clone"
        ))
        assert "error" in result
        assert "Import failed" in result["error"] or "500" in result["error"]


class TestUpdateDashboardErrors:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch("omni_dash.mcp.server.get_settings")
    def test_update_with_empty_tiles_creates_empty_dashboard(self, mock_settings, _, mock_doc_svc):
        """tiles=[] (empty list, not None) means replace with zero tiles."""
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT
        mock_doc_svc.create_dashboard.return_value = DashboardResponse(
            document_id="skeleton-1", name="Updated"
        )
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="updated-1", name="Updated"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        # Empty tiles list — should still process without crashing
        result = json.loads(mcp_server.update_dashboard(
            dashboard_id="orig-1", tiles=[], name="Updated"
        ))
        assert result["status"] == "updated"

    def test_update_nonexistent_dashboard(self, mock_doc_svc):
        from omni_dash.exceptions import DocumentNotFoundError

        mock_doc_svc.export_dashboard.side_effect = DocumentNotFoundError("missing")
        result = json.loads(mcp_server.update_dashboard(dashboard_id="missing"))
        assert "error" in result


class TestMoveDashboardErrors:
    def test_handles_import_error(self, mock_doc_svc):
        from omni_dash.exceptions import OmniAPIError

        mock_doc_svc.export_dashboard.return_value = SAMPLE_EXPORT
        mock_doc_svc.import_dashboard.side_effect = OmniAPIError(500, "Import failed")

        result = json.loads(mcp_server.move_dashboard(
            dashboard_id="orig", target_folder_id="f2"
        ))
        assert "error" in result


# ---------------------------------------------------------------------------
# Full CRUD Chain
# ---------------------------------------------------------------------------


class TestCRUDChain:
    """End-to-end: create -> get -> clone -> delete."""

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_doc_svc")
    @patch("omni_dash.mcp.server.get_settings")
    def test_full_lifecycle(self, mock_settings, mock_get_doc_svc, _):
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        svc = MagicMock()
        mock_get_doc_svc.return_value = svc

        # Step 1: Create
        svc.create_dashboard.return_value = DashboardResponse(
            document_id="skel-1", name="CRUD Test"
        )
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
                                    "jsonHash": "x",
                                },
                            }
                        }
                    ]
                }
            },
            "document": {"sharedModelId": "model-123", "name": "CRUD Test", "folderId": None},
            "workbookModel": {},
            "exportVersion": "0.1",
        }
        svc.import_dashboard.return_value = ImportResponse(
            document_id="final-1", name="CRUD Test"
        )
        svc.delete_dashboard.return_value = None

        tiles = [{
            "name": "Line Chart",
            "chart_type": "line",
            "query": {"table": "t", "fields": ["t.d", "t.v"]},
            "vis_config": {"x_axis": "t.d", "y_axis": ["t.v"]},
            "size": "half",
        }]
        create_result = json.loads(mcp_server.create_dashboard(
            name="CRUD Test", tiles=tiles
        ))
        assert create_result["status"] == "created"
        created_id = create_result["dashboard_id"]
        assert created_id == "final-1"

        # Step 2: Get
        # Reset _doc_svc so that get_dashboard uses the injected svc
        mcp_server._doc_svc = svc
        svc.get_dashboard.return_value = DashboardResponse(
            document_id=created_id,
            name="CRUD Test",
            model_id="model-123",
            query_presentations=[{"id": "qp1"}],
            created_at="2026-01-01",
            updated_at="2026-01-02",
        )
        get_result = json.loads(mcp_server.get_dashboard(created_id))
        assert get_result["document_id"] == created_id
        assert get_result["name"] == "CRUD Test"
        assert get_result["tile_count"] == 1

        # Step 3: Clone
        svc.export_dashboard.return_value = SAMPLE_EXPORT
        svc.import_dashboard.return_value = ImportResponse(
            document_id="clone-1", name="CRUD Clone"
        )
        clone_result = json.loads(mcp_server.clone_dashboard(
            dashboard_id=created_id, new_name="CRUD Clone"
        ))
        assert clone_result["status"] == "cloned"
        assert clone_result["source_dashboard_id"] == created_id
        assert clone_result["new_dashboard_id"] == "clone-1"

        # Step 4: Delete original
        svc.delete_dashboard.return_value = None
        delete_result = json.loads(mcp_server.delete_dashboard(created_id))
        assert delete_result["status"] == "deleted"
        assert delete_result["dashboard_id"] == created_id

        # Step 5: Delete clone
        delete_clone_result = json.loads(mcp_server.delete_dashboard("clone-1"))
        assert delete_clone_result["status"] == "deleted"


# ---------------------------------------------------------------------------
# suggest_chart: field filtering
# ---------------------------------------------------------------------------


class TestSuggestChartFieldFiltering:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_filters_to_requested_fields(self, _, mock_model_svc):
        from omni_dash.api.models import TopicDetail

        mock_model_svc.get_topic_native.return_value = TopicDetail(
            name="mart_seo",
            label="SEO",
            fields=[
                {"name": "week_start", "type": "date", "label": "Week Start"},
                {"name": "visits", "type": "number", "label": "Visits", "aggregation": "sum"},
                {"name": "revenue", "type": "number", "label": "Revenue", "aggregation": "sum"},
                {"name": "channel_name", "type": "string", "label": "Channel"},
            ],
        )

        # Request only 1 measure field — should recommend "number" (KPI)
        result = json.loads(mcp_server.suggest_chart(
            table="mart_seo",
            fields=["visits"],
        ))
        assert result["chart_type"] == "number"
        assert result["fields_analyzed"] == 1

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_qualified_field_names_filter(self, _, mock_model_svc):
        from omni_dash.api.models import TopicDetail

        mock_model_svc.get_topic_native.return_value = TopicDetail(
            name="mart_seo",
            label="SEO",
            fields=[
                {"name": "week_start", "type": "date", "label": "Week Start"},
                {"name": "visits", "type": "number", "label": "Visits", "aggregation": "sum"},
            ],
        )

        # Use qualified names like "mart_seo.visits"
        result = json.loads(mcp_server.suggest_chart(
            table="mart_seo",
            fields=["mart_seo.week_start", "mart_seo.visits"],
        ))
        assert result["fields_analyzed"] == 2
        assert result["chart_type"] == "line"


# ---------------------------------------------------------------------------
# Bug #1: chartType:null fallback in _create_with_vis_configs
# ---------------------------------------------------------------------------


class TestChartTypeNullFallback:
    """When visConfig.chartType is None, _create_with_vis_configs should
    fall back to visType mapping (omni-kpi→kpi, etc.) then to 'line'.
    """

    def _make_skeleton_export(self, *, vis_type: str, chart_type: str | None) -> dict:
        """Build a minimal export payload for one tile with the given vis/chart types."""
        return {
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Tile A",
                                "visConfig": {
                                    "visType": vis_type,
                                    "chartType": chart_type,
                                    "spec": {},
                                    "fields": [],
                                    "jsonHash": "stale",
                                },
                            }
                        }
                    ]
                }
            },
            "document": {"sharedModelId": "model-123", "name": "Test", "folderId": None},
            "workbookModel": {},
            "exportVersion": "0.1",
        }

    @patch.object(mcp_server, "_get_doc_svc")
    def test_null_chart_type_falls_back_to_vis_type(self, mock_get_doc_svc):
        """chartType=None + visType='omni-kpi' → patched chartType should be 'kpi'."""
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        svc = MagicMock()
        mock_get_doc_svc.return_value = svc

        svc.create_dashboard.return_value = DashboardResponse(
            document_id="skel-1", name="Test"
        )
        svc.export_dashboard.return_value = self._make_skeleton_export(
            vis_type="omni-kpi", chart_type=None,
        )
        svc.import_dashboard.return_value = ImportResponse(
            document_id="final-1", name="Test"
        )
        svc.delete_dashboard.return_value = None

        # Build a payload that has vis config with visType=omni-kpi
        payload = {
            "queryPresentations": [
                {
                    "name": "Tile A",
                    "visConfig": {"visType": "omni-kpi", "chartType": None},
                    "query": {},
                }
            ],
        }

        mcp_server._create_with_vis_configs(
            payload, name="Test", folder_id=None,
        )

        # Verify the reimported export has chartType patched to "kpi"
        import_call = svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        memberships = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ]
        patched_vc = memberships[0]["queryPresentation"]["visConfig"]
        assert patched_vc["chartType"] == "kpi"
        assert "jsonHash" not in patched_vc

    @patch.object(mcp_server, "_get_doc_svc")
    def test_null_chart_type_defaults_to_line(self, mock_get_doc_svc):
        """chartType=None + visType='basic' (not in fallback map) → chartType='line'."""
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        svc = MagicMock()
        mock_get_doc_svc.return_value = svc

        svc.create_dashboard.return_value = DashboardResponse(
            document_id="skel-1", name="Test"
        )
        svc.export_dashboard.return_value = self._make_skeleton_export(
            vis_type="basic", chart_type=None,
        )
        svc.import_dashboard.return_value = ImportResponse(
            document_id="final-1", name="Test"
        )
        svc.delete_dashboard.return_value = None

        payload = {
            "queryPresentations": [
                {
                    "name": "Tile A",
                    "visConfig": {"visType": "basic", "chartType": None},
                    "query": {},
                }
            ],
        }

        mcp_server._create_with_vis_configs(
            payload, name="Test", folder_id=None,
        )

        import_call = svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        memberships = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ]
        patched_vc = memberships[0]["queryPresentation"]["visConfig"]
        assert patched_vc["chartType"] == "line"


# ---------------------------------------------------------------------------
# Bug #4: update_tile MCP tool
# ---------------------------------------------------------------------------


_UPDATE_TILE_EXPORT: dict[str, Any] = {
    "document": {"name": "D", "sharedModelId": "m", "folderId": "f"},
    "dashboard": {
        "queryPresentationCollection": {
            "queryPresentationCollectionMemberships": [
                {
                    "queryPresentation": {
                        "name": "Revenue",
                        "query": {
                            "queryJson": {"table": "t", "fields": ["t.rev"]},
                        },
                        "visConfig": {
                            "visType": "basic",
                            "chartType": "line",
                        },
                    }
                }
            ]
        },
        "metadata": {
            "layouts": {
                "lg": [{"i": "1", "x": 0, "y": 0, "w": 12, "h": 40}]
            }
        },
    },
    "workbookModel": {"base_model_id": "m"},
    "exportVersion": "0.1",
}


class TestUpdateTile:
    """Tests for the update_tile MCP tool (Bug #4)."""

    def test_updates_sql(self, mock_doc_svc):
        """Providing sql= sets isSql=True and userEditedSQL in the export."""
        import copy
        from omni_dash.api.documents import ImportResponse

        mock_doc_svc.export_dashboard.return_value = copy.deepcopy(_UPDATE_TILE_EXPORT)
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="new-1", name="D"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.update_tile(
                dashboard_id="dash-1",
                tile_name="Revenue",
                sql="SELECT 1",
            ))

        assert result["status"] == "updated"
        # Inspect the payload passed to import_dashboard
        import_call = mock_doc_svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        qp = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        assert qp["isSql"] is True
        assert qp["query"]["queryJson"]["userEditedSQL"] == "SELECT 1"

    def test_updates_chart_type(self, mock_doc_svc):
        """Providing chart_type= sets visConfig.chartType and visType."""
        import copy
        from omni_dash.api.documents import ImportResponse

        mock_doc_svc.export_dashboard.return_value = copy.deepcopy(_UPDATE_TILE_EXPORT)
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="new-1", name="D"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.update_tile(
                dashboard_id="dash-1",
                tile_name="Revenue",
                chart_type="bar",
            ))

        assert result["status"] == "updated"
        import_call = mock_doc_svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        vc = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]["visConfig"]
        assert vc["chartType"] == "bar"
        assert vc["visType"] == "basic"

    def test_updates_title(self, mock_doc_svc):
        """Providing title= renames the QP."""
        import copy
        from omni_dash.api.documents import ImportResponse

        mock_doc_svc.export_dashboard.return_value = copy.deepcopy(_UPDATE_TILE_EXPORT)
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="new-1", name="D"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.update_tile(
                dashboard_id="dash-1",
                tile_name="Revenue",
                title="New Title",
            ))

        assert result["status"] == "updated"
        assert result["tile_name"] == "New Title"
        import_call = mock_doc_svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        qp = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        assert qp["name"] == "New Title"

    def test_tile_not_found(self, mock_doc_svc):
        """Non-existent tile_name returns error with available_tiles list."""
        import copy

        mock_doc_svc.export_dashboard.return_value = copy.deepcopy(_UPDATE_TILE_EXPORT)

        result = json.loads(mcp_server.update_tile(
            dashboard_id="dash-1",
            tile_name="Nonexistent",
            sql="SELECT 1",
        ))

        assert "error" in result
        assert "Nonexistent" in result["error"]
        assert result["available_tiles"] == ["Revenue"]

    def test_no_updates_specified(self, mock_doc_svc):
        """Calling with no optional args returns an error."""
        result = json.loads(mcp_server.update_tile(
            dashboard_id="dash-1",
            tile_name="Revenue",
        ))

        assert "error" in result
        assert "No updates specified" in result["error"]


# ---------------------------------------------------------------------------
# Bug #5: _create_with_vis_configs 404 fallback to import
# ---------------------------------------------------------------------------


class TestCreateDashboard404Fallback:
    """When create_dashboard returns 404, _create_with_vis_configs
    should fall back to import instead of failing.
    """

    @patch.object(mcp_server, "_get_doc_svc")
    def test_fallback_to_import_on_404(self, mock_get_doc_svc):
        """OmniDashError('404 Not Found') triggers _create_via_import_fallback."""
        from omni_dash.api.documents import ImportResponse
        from omni_dash.exceptions import OmniDashError

        svc = MagicMock()
        mock_get_doc_svc.return_value = svc

        # create_dashboard raises a 404 error
        svc.create_dashboard.side_effect = OmniDashError("404 Not Found")

        # import_dashboard is called twice: once by fallback (no vis configs
        # to patch → returns immediately), and once more isn't needed
        svc.import_dashboard.return_value = ImportResponse(
            document_id="fallback-1", name="Test"
        )

        payload = {
            "modelId": "m-1",
            "name": "Test",
            "queryPresentations": [
                {
                    "name": "Tile A",
                    "query": {"table": "t", "fields": ["t.x"]},
                }
            ],
        }

        dash_id, dash_name = mcp_server._create_with_vis_configs(
            payload, name="Test", folder_id=None,
        )

        assert dash_id == "fallback-1"
        # Verify import_dashboard was called (the fallback path)
        svc.import_dashboard.assert_called_once()

    @patch.object(mcp_server, "_get_doc_svc")
    def test_non_404_error_propagates(self, mock_get_doc_svc):
        """OmniDashError('500 Server Error') should re-raise, not fallback."""
        from omni_dash.exceptions import OmniDashError

        svc = MagicMock()
        mock_get_doc_svc.return_value = svc

        svc.create_dashboard.side_effect = OmniDashError("500 Server Error")

        payload = {
            "modelId": "m-1",
            "name": "Test",
            "queryPresentations": [
                {
                    "name": "Tile A",
                    "query": {"table": "t", "fields": ["t.x"]},
                }
            ],
        }

        with pytest.raises(OmniDashError, match="500 Server Error"):
            mcp_server._create_with_vis_configs(
                payload, name="Test", folder_id=None,
            )

        # import_dashboard should NOT have been called
        svc.import_dashboard.assert_not_called()


# ---------------------------------------------------------------------------
# Bug #2 regression: add_tiles preserves filterConfig and filterOrder
# ---------------------------------------------------------------------------


class TestAddTilesPreservesState:
    """Explicit test that the rewritten add_tiles preserves
    filterConfig and filterOrder from the original export.
    """

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch("omni_dash.mcp.server.get_settings")
    @patch("omni_dash.mcp.server.DashboardSerializer")
    def test_preserves_filter_config(self, mock_serializer, mock_settings, _, mock_doc_svc):
        import copy
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_serializer.to_omni_create_payload.return_value = {
            "name": "__add_tiles_tmp__",
            "queryPresentations": [
                {"name": "New Tile", "visConfig": {"visType": "basic", "chartType": "bar"}},
            ],
        }

        # Original export with filterConfig + filterOrder
        orig_export = {
            "document": {"name": "Dashboard", "sharedModelId": "model-abc", "folderId": "f1"},
            "dashboard": {
                "model": {"baseModelId": "model-abc"},
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Existing",
                                "query": {"queryJson": {"table": "t", "fields": ["t.x"]}},
                                "visConfig": {"visType": "basic", "chartType": "line"},
                            }
                        }
                    ]
                },
                "metadata": {"layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 12, "h": 40}]}},
                "filterConfig": {"f1": {"fieldName": "t.date", "kind": "date_range"}},
                "filterOrder": ["f1"],
            },
            "workbookModel": {"base_model_id": "model-abc"},
            "exportVersion": "0.1",
        }

        skeleton_export = {
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {"queryPresentation": {"name": "New Tile", "visConfig": {"visType": "basic"}}}
                    ]
                }
            },
            "workbookModel": {"base_model_id": "model-abc"},
        }

        temp_export = {
            "document": {"name": "__add_tiles_tmp__", "sharedModelId": "model-abc"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "New Tile",
                                "query": {"queryJson": {"table": "t", "fields": ["t.a"]}},
                                "visConfig": {"visType": "basic", "chartType": "bar"},
                            }
                        }
                    ]
                },
                "metadata": {"layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 6, "h": 40}]}},
            },
            "workbookModel": {"base_model_id": "model-abc"},
            "exportVersion": "0.1",
        }

        mock_doc_svc.export_dashboard.side_effect = [
            copy.deepcopy(orig_export),
            copy.deepcopy(skeleton_export),
            copy.deepcopy(temp_export),
        ]
        mock_doc_svc.create_dashboard.return_value = DashboardResponse(
            document_id="skel-1", name="__add_tiles_tmp__"
        )
        mock_doc_svc.import_dashboard.side_effect = [
            ImportResponse(document_id="temp-1", name="__add_tiles_tmp__"),
            ImportResponse(document_id="merged-1", name="Dashboard"),
        ]
        mock_doc_svc.delete_dashboard.return_value = None

        new_tiles = [{
            "name": "New Tile",
            "chart_type": "bar",
            "query": {"table": "t", "fields": ["t.a"]},
            "vis_config": {"x_axis": "t.a"},
            "size": "half",
        }]

        result = json.loads(mcp_server.add_tiles_to_dashboard(
            dashboard_id="orig-1", tiles=new_tiles,
        ))

        assert result["status"] == "updated"

        # The final import (2nd call) must contain the original filterConfig/filterOrder
        final_import = mock_doc_svc.import_dashboard.call_args_list[-1]
        imported_data = final_import[0][0]
        dash = imported_data["dashboard"]
        assert dash["filterConfig"] == {"f1": {"fieldName": "t.date", "kind": "date_range"}}
        assert dash["filterOrder"] == ["f1"]


class TestAddTilesEphemeralMerge:
    """add_tiles_to_dashboard must update the 'ephemeral' field so Omni
    includes new tiles during import. Without this, tiles are silently dropped.
    """

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch("omni_dash.mcp.server.get_settings")
    @patch("omni_dash.mcp.server.DashboardSerializer")
    def test_ephemeral_includes_new_tiles(self, mock_serializer, mock_settings, _, mock_doc_svc):
        import copy
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_serializer.to_omni_create_payload.return_value = {
            "name": "__add_tiles_tmp__",
            "queryPresentations": [
                {"name": "New Tile", "visConfig": {"visType": "basic", "chartType": "bar"}},
            ],
        }

        orig_export = {
            "document": {
                "name": "Dashboard", "sharedModelId": "model-abc",
                "ephemeral": "1:origMini1,2:origMini2",
                "lastItemIndex": 2,
            },
            "dashboard": {
                "ephemeral": "1:origMini1,2:origMini2",
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Tile A", "miniUuid": "origMini1",
                                "query": {"queryJson": {"table": "t", "fields": ["t.x"]}},
                                "visConfig": {"visType": "basic", "chartType": "line"},
                            }
                        },
                        {
                            "queryPresentation": {
                                "name": "Tile B", "miniUuid": "origMini2",
                                "query": {"queryJson": {"table": "t", "fields": ["t.y"]}},
                                "visConfig": {"visType": "basic", "chartType": "bar"},
                            }
                        },
                    ]
                },
                "metadata": {
                    "layouts": {
                        "lg": [
                            {"i": "1", "x": 0, "y": 0, "w": 12, "h": 40},
                            {"i": "2", "x": 0, "y": 40, "w": 12, "h": 40},
                        ]
                    }
                },
            },
            "workbookModel": {"base_model_id": "model-abc"},
            "exportVersion": "0.1",
        }

        temp_export = {
            "document": {"name": "__add_tiles_tmp__", "sharedModelId": "model-abc"},
            "dashboard": {
                "ephemeral": "1:newMiniA",
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "New Tile", "miniUuid": "newMiniA",
                                "query": {"queryJson": {"table": "t", "fields": ["t.a"]}},
                                "visConfig": {"visType": "basic", "chartType": "bar"},
                            }
                        }
                    ]
                },
                "metadata": {
                    "layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 6, "h": 40}]}
                },
            },
            "workbookModel": {"base_model_id": "model-abc"},
            "exportVersion": "0.1",
        }

        skeleton_export = {
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {"queryPresentation": {"name": "New Tile", "visConfig": {"visType": "basic"}}}
                    ]
                }
            },
            "workbookModel": {"base_model_id": "model-abc"},
        }

        mock_doc_svc.export_dashboard.side_effect = [
            copy.deepcopy(orig_export),
            copy.deepcopy(skeleton_export),
            copy.deepcopy(temp_export),
        ]
        mock_doc_svc.create_dashboard.return_value = DashboardResponse(
            document_id="skel-1", name="__add_tiles_tmp__"
        )
        mock_doc_svc.import_dashboard.side_effect = [
            ImportResponse(document_id="temp-1", name="__add_tiles_tmp__"),
            ImportResponse(document_id="merged-1", name="Dashboard"),
        ]
        mock_doc_svc.delete_dashboard.return_value = None

        new_tiles = [{
            "name": "New Tile",
            "chart_type": "bar",
            "query": {"table": "t", "fields": ["t.a"]},
            "vis_config": {},
            "size": "half",
        }]

        result = json.loads(mcp_server.add_tiles_to_dashboard(
            dashboard_id="orig-1", tiles=new_tiles,
        ))
        assert result["status"] == "updated"
        assert result["previous_tile_count"] == 2
        assert result["new_tile_count"] == 3

        # Verify ephemeral field includes new tile
        final_import = mock_doc_svc.import_dashboard.call_args_list[-1]
        imported_data = final_import[0][0]
        dash = imported_data["dashboard"]
        ephemeral = dash.get("ephemeral", "")

        # Must contain original entries AND new tile entry
        assert "1:origMini1" in ephemeral
        assert "2:origMini2" in ephemeral
        assert "3:newMiniA" in ephemeral

        # Verify 3 total memberships
        mems = dash["queryPresentationCollection"]["queryPresentationCollectionMemberships"]
        assert len(mems) == 3

        # Verify layout has 3 items with correct offset
        layouts = dash["metadata"]["layouts"]["lg"]
        assert len(layouts) == 3
        new_layout = layouts[2]
        assert new_layout["i"] == "3"
        assert new_layout["y"] >= 80  # Below existing tiles

        # queryIdentifierMapKey removed from temp memberships (new tile)
        # so Omni assigns fresh keys on import
        temp_qp = mems[2].get("queryPresentation", {})
        assert "queryIdentifierMapKey" not in temp_qp

        # Verify document-level ephemeral and lastItemIndex are updated
        doc = imported_data.get("document", {})
        doc_eph = doc.get("ephemeral", "")
        assert "3:newMiniA" in doc_eph, f"document.ephemeral missing new tile: {doc_eph}"
        assert doc.get("lastItemIndex") == 3


class TestAutoFieldValidation:
    """Tests that create_dashboard and add_tiles auto-validate field references."""

    def _mock_model_svc(self, valid_fields: dict[str, list[str]]):
        """Create a mock model service that returns specified fields per table."""
        model_svc = MagicMock()

        def get_topic(model_id, table_name):
            fields = valid_fields.get(table_name, [])
            topic = MagicMock()
            topic.fields = [{"name": f} for f in fields]
            return topic

        model_svc.get_topic_native.side_effect = get_topic
        return model_svc

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_model_svc")
    def test_create_blocks_invalid_fields(self, mock_get_model_svc, _):
        mock_get_model_svc.return_value = self._mock_model_svc({
            "mart_seo": ["week", "visits", "clicks"],
        })

        tiles = [{
            "name": "Bad Tile",
            "chart_type": "line",
            "query": {
                "table": "mart_seo",
                "fields": ["mart_seo.week", "mart_seo.nonexistent_field"],
            },
            "vis_config": {},
        }]

        result = json.loads(mcp_server.create_dashboard(name="Test", tiles=tiles))
        assert "error" in result
        assert "field_errors" in result
        assert len(result["field_errors"]) == 1
        assert "nonexistent_field" in result["field_errors"][0]

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_model_svc")
    @patch.object(mcp_server, "_get_doc_svc")
    @patch("omni_dash.mcp.server.get_settings")
    def test_create_allows_valid_fields(
        self, mock_settings, mock_doc_svc, mock_get_model_svc, _
    ):
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        mock_get_model_svc.return_value = self._mock_model_svc({
            "mart_seo": ["week", "visits"],
        })

        svc = MagicMock()
        svc.create_dashboard.return_value = DashboardResponse(
            document_id="skel-1", name="Test"
        )
        svc.export_dashboard.return_value = {
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {"queryPresentation": {"name": "Chart", "visConfig": {"chartType": "line"}}}
                    ]
                }
            },
            "document": {"sharedModelId": "model-123"},
            "workbookModel": {},
            "exportVersion": "0.1",
        }
        svc.import_dashboard.return_value = ImportResponse(document_id="ok-1", name="Test")
        mock_doc_svc.return_value = svc

        tiles = [{
            "name": "Chart",
            "chart_type": "line",
            "query": {"table": "mart_seo", "fields": ["mart_seo.week", "mart_seo.visits"]},
            "vis_config": {},
        }]

        result = json.loads(mcp_server.create_dashboard(name="Test", tiles=tiles))
        assert result["status"] == "created"

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_model_svc")
    @patch.object(mcp_server, "_get_doc_svc")
    def test_add_tiles_blocks_invalid_fields(
        self, mock_doc_svc, mock_get_model_svc, _
    ):
        mock_get_model_svc.return_value = self._mock_model_svc({
            "mart_seo": ["week", "visits"],
        })

        svc = MagicMock()
        svc.export_dashboard.return_value = {
            "document": {"name": "Dash", "sharedModelId": "model-123"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": []
                }
            },
            "workbookModel": {"base_model_id": "model-123"},
            "exportVersion": "0.1",
        }
        mock_doc_svc.return_value = svc

        tiles = [{
            "name": "Bad",
            "chart_type": "bar",
            "query": {"table": "mart_seo", "fields": ["mart_seo.fake_field"]},
            "vis_config": {},
        }]

        result = json.loads(mcp_server.add_tiles_to_dashboard(
            dashboard_id="dash-1", tiles=tiles,
        ))
        assert "error" in result
        assert "field_errors" in result
        assert "fake_field" in result["field_errors"][0]

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_model_svc")
    def test_create_catches_multiple_bad_fields(self, mock_get_model_svc, _):
        mock_get_model_svc.return_value = self._mock_model_svc({
            "ads": ["date", "spend", "clicks"],
        })

        tiles = [
            {
                "name": "KPI 1",
                "chart_type": "number",
                "query": {"table": "ads", "fields": ["ads.total_spend"]},
                "vis_config": {},
            },
            {
                "name": "KPI 2",
                "chart_type": "number",
                "query": {"table": "ads", "fields": ["ads.total_clicks"]},
                "vis_config": {},
            },
        ]

        result = json.loads(mcp_server.create_dashboard(name="Test", tiles=tiles))
        assert "error" in result
        assert len(result["field_errors"]) == 2
        assert "total_spend" in result["field_errors"][0]
        assert "total_clicks" in result["field_errors"][1]


class TestVisConfigSpecToConfig:
    """Test that vis config 'spec' is copied to 'config' during patching.

    Omni reads vis config from 'config' but our serializer writes to 'spec'.
    The _create_with_vis_configs patching must copy spec→config.
    """

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_doc_svc")
    @patch("omni_dash.mcp.server.get_settings")
    def test_spec_copied_to_config(self, mock_settings, mock_get_doc_svc, _):
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        svc = MagicMock()
        svc.create_dashboard.return_value = DashboardResponse(
            document_id="skel-1", name="Test"
        )
        # Skeleton export — Omni returns vis config with "config" key
        svc.export_dashboard.return_value = {
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [{
                        "queryPresentation": {
                            "name": "Spend KPI",
                            "visConfig": {
                                "visType": "omni-kpi",
                                "chartType": "kpi",
                                "config": {
                                    "alignment": "left",
                                    "markdownConfig": [{
                                        "type": "number",
                                        "config": {"field": {"row": "_first"}}
                                    }],
                                },
                            },
                        }
                    }]
                }
            },
            "document": {"sharedModelId": "model-123"},
            "workbookModel": {},
            "exportVersion": "0.1",
        }
        svc.import_dashboard.return_value = ImportResponse(
            document_id="final-1", name="Test"
        )
        mock_get_doc_svc.return_value = svc

        tiles = [{
            "name": "Spend KPI",
            "chart_type": "number",
            "query": {"table": "t", "fields": ["t.spend"]},
            "vis_config": {"value_format": "$#,##0"},
            "size": "quarter",
        }]

        result = json.loads(mcp_server.create_dashboard(name="Test", tiles=tiles))
        assert result["status"] == "created"

        # Check that the import payload has spec copied to config
        import_call = svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        qp = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        vc = qp.get("visConfig", {})

        # The "config" key must contain our custom markdownConfig with format
        assert "config" in vc, "visConfig must have 'config' key"
        mc = vc["config"].get("markdownConfig", [])
        assert len(mc) > 0, "markdownConfig must have entries"
        # Check format is set
        fmt = mc[0].get("config", {}).get("field", {}).get("format")
        assert fmt == "$#,##0", f"Expected format '$#,##0' but got {fmt}"


# ---------------------------------------------------------------------------
# Import fallback payload completeness
# ---------------------------------------------------------------------------


class TestImportFallbackPayload:
    """Verify that _create_via_import_fallback produces a payload with all
    fields the Omni import API requires (metadataVersion, filterOrder, etc.).
    """

    @patch.object(mcp_server, "_get_doc_svc")
    def test_fallback_payload_has_required_fields(self, mock_get_doc_svc):
        from omni_dash.api.documents import ImportResponse

        svc = MagicMock()
        mock_get_doc_svc.return_value = svc
        svc.import_dashboard.return_value = ImportResponse(
            document_id="fb-1", name="Test"
        )

        payload = {
            "modelId": "m-1",
            "name": "Test",
            "queryPresentations": [
                {
                    "name": "Tile A",
                    "query": {"table": "t", "fields": ["t.x"]},
                    "queryIdentifierMapKey": "1",
                },
                {
                    "name": "Tile B",
                    "query": {"table": "t", "fields": ["t.y"]},
                    "queryIdentifierMapKey": "2",
                },
            ],
        }

        mcp_server._create_via_import_fallback(
            svc, payload, name="Test", folder_id="f1",
        )

        call_args = svc.import_dashboard.call_args
        export_data = call_args[0][0]
        dash = export_data["dashboard"]

        # metadataVersion must be 2
        assert dash["metadataVersion"] == 2

        # ephemeral must be a string
        assert isinstance(dash["ephemeral"], str)

        # queryPresentationCollection must have filterOrder and filterConfig
        qpc = dash["queryPresentationCollection"]
        assert isinstance(qpc["filterOrder"], list)
        assert isinstance(qpc["filterConfig"], dict)
        assert qpc["filterConfigVersion"] == 0

        # metadata.layouts must exist
        assert "layouts" in dash["metadata"]
        assert "lg" in dash["metadata"]["layouts"]
        assert len(dash["metadata"]["layouts"]["lg"]) == 2

        # Each QP must have required fields
        for m in qpc["queryPresentationCollectionMemberships"]:
            qp = m["queryPresentation"]
            assert qp["type"] == "query"
            assert isinstance(qp["filterOrder"], list)

        # document must have folderId
        assert export_data["document"]["folderId"] == "f1"

    @patch.object(mcp_server, "_get_doc_svc")
    def test_400_error_triggers_fallback(self, mock_get_doc_svc):
        """OmniDashError('400 Bad Request') should trigger import fallback."""
        from omni_dash.api.documents import ImportResponse
        from omni_dash.exceptions import OmniDashError

        svc = MagicMock()
        mock_get_doc_svc.return_value = svc

        svc.create_dashboard.side_effect = OmniDashError("400 Bad Request: metadataVersion")
        svc.import_dashboard.return_value = ImportResponse(
            document_id="fb-400", name="Test"
        )

        payload = {
            "modelId": "m-1",
            "name": "Test",
            "queryPresentations": [
                {"name": "Tile A", "query": {"table": "t", "fields": ["t.x"]}},
            ],
        }

        dash_id, _ = mcp_server._create_with_vis_configs(
            payload, name="Test", folder_id=None,
        )
        assert dash_id == "fb-400"
        svc.import_dashboard.assert_called_once()


class TestProfileDataTypeInference:
    """Verify that profile_data correctly infers string vs date types."""

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_string_with_hyphens_not_detected_as_date(self, _mock_mid, mock_model_svc, mock_query_runner):
        from omni_dash.api.models import TopicDetail
        from omni_dash.api.queries import QueryResult

        mock_model_svc.get_topic_native.return_value = TopicDetail(
            name="t", label="", fields=[{"name": "campaign_name"}],
        )
        mock_query_runner.run.return_value = QueryResult(
            fields=["t.campaign_name"],
            rows=[
                {"t.campaign_name": "Search - Customer Support Automation"},
                {"t.campaign_name": "Display - Rmkt"},
            ],
            row_count=2,
        )

        result = json.loads(mcp_server.profile_data("t"))
        field_profile = result["fields"]["t.campaign_name"]
        assert field_profile["inferred_type"] == "string", (
            f"Expected string, got {field_profile['inferred_type']}"
        )

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_iso_date_detected_as_date(self, _mock_mid, mock_model_svc, mock_query_runner):
        from omni_dash.api.models import TopicDetail
        from omni_dash.api.queries import QueryResult

        mock_model_svc.get_topic_native.return_value = TopicDetail(
            name="t", label="", fields=[{"name": "week_start"}],
        )
        mock_query_runner.run.return_value = QueryResult(
            fields=["t.week_start"],
            rows=[{"t.week_start": "2026-02-27"}],
            row_count=1,
        )

        result = json.loads(mcp_server.profile_data("t"))
        field_profile = result["fields"]["t.week_start"]
        assert field_profile["inferred_type"] == "date"


# ---------------------------------------------------------------------------
# Bug #6: .get("visConfig", {}) returns detached dict when key missing
# ---------------------------------------------------------------------------


class TestVisConfigSetdefault:
    """When a skeleton export omits visConfig for a tile, the vis config
    patching in _create_with_vis_configs must still inject the vis config
    into the export. Previously used .get() which returned a detached dict.
    """

    @patch.object(mcp_server, "_get_doc_svc")
    def test_vis_config_injected_when_missing_from_export(self, mock_get_doc_svc):
        """visConfig missing from skeleton export → patching must create it."""
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        svc = MagicMock()
        mock_get_doc_svc.return_value = svc

        svc.create_dashboard.return_value = DashboardResponse(
            document_id="skel-1", name="Test"
        )
        # Skeleton export has NO visConfig on the tile
        svc.export_dashboard.return_value = {
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "KPI Spend",
                                # NO "visConfig" key at all
                            }
                        }
                    ]
                }
            },
            "document": {"sharedModelId": "model-123", "name": "Test", "folderId": None},
            "workbookModel": {},
            "exportVersion": "0.1",
        }
        svc.import_dashboard.return_value = ImportResponse(
            document_id="final-1", name="Test"
        )
        svc.delete_dashboard.return_value = None

        payload = {
            "queryPresentations": [
                {
                    "name": "KPI Spend",
                    "visConfig": {
                        "visType": "omni-kpi",
                        "chartType": "kpi",
                        "spec": {
                            "alignment": "left",
                            "markdownConfig": [
                                {"type": "number", "config": {"field": {"format": "$#,##0"}}}
                            ],
                        },
                        "fields": ["t.spend"],
                    },
                    "query": {},
                }
            ],
        }

        mcp_server._create_with_vis_configs(
            payload, name="Test", folder_id=None,
        )

        import_call = svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        qp = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        # visConfig must exist and have all patched fields
        assert "visConfig" in qp, "visConfig must be created when missing"
        vc = qp["visConfig"]
        assert vc["visType"] == "omni-kpi"
        assert vc["chartType"] == "kpi"
        assert "spec" in vc
        assert "config" in vc  # spec→config copy
        assert vc["fields"] == ["t.spend"]

    @patch.object(mcp_server, "_get_doc_svc")
    def test_multiple_tiles_vis_config_some_missing(self, mock_get_doc_svc):
        """Mix of tiles with and without visConfig in export — all get patched."""
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        svc = MagicMock()
        mock_get_doc_svc.return_value = svc

        svc.create_dashboard.return_value = DashboardResponse(
            document_id="skel-1", name="Test"
        )
        # Tile A has visConfig, Tile B does not
        svc.export_dashboard.return_value = {
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Tile A",
                                "visConfig": {"visType": "basic", "chartType": "line"},
                            }
                        },
                        {
                            "queryPresentation": {
                                "name": "Tile B",
                                # NO visConfig
                            }
                        },
                    ]
                }
            },
            "document": {"sharedModelId": "model-123", "name": "Test", "folderId": None},
            "workbookModel": {},
            "exportVersion": "0.1",
        }
        svc.import_dashboard.return_value = ImportResponse(
            document_id="final-1", name="Test"
        )
        svc.delete_dashboard.return_value = None

        payload = {
            "queryPresentations": [
                {
                    "name": "Tile A",
                    "visConfig": {
                        "visType": "basic",
                        "chartType": "bar",
                        "spec": {"version": 0, "configType": "cartesian"},
                    },
                    "query": {},
                },
                {
                    "name": "Tile B",
                    "visConfig": {
                        "visType": "omni-kpi",
                        "chartType": "kpi",
                        "spec": {"markdownConfig": [{"type": "number"}]},
                        "fields": ["t.count"],
                    },
                    "query": {},
                },
            ],
        }

        mcp_server._create_with_vis_configs(
            payload, name="Test", folder_id=None,
        )

        import_call = svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        memberships = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ]

        # Tile A (had visConfig) — patched in place
        vc_a = memberships[0]["queryPresentation"]["visConfig"]
        assert vc_a["chartType"] == "bar"
        assert vc_a["visType"] == "basic"

        # Tile B (missing visConfig) — must be created
        assert "visConfig" in memberships[1]["queryPresentation"], (
            "visConfig must be injected for tiles missing it"
        )
        vc_b = memberships[1]["queryPresentation"]["visConfig"]
        assert vc_b["chartType"] == "kpi"
        assert vc_b["visType"] == "omni-kpi"
        assert vc_b["fields"] == ["t.count"]


class TestUpdateTileNoVisConfig:
    """update_tile must work even when the export has no visConfig for the tile."""

    def test_chart_type_change_when_vis_config_missing(self, mock_doc_svc):
        """chart_type= on a tile with no visConfig in export → visConfig created."""
        import copy
        from omni_dash.api.documents import ImportResponse

        # Export with NO visConfig on the tile
        export = copy.deepcopy(_UPDATE_TILE_EXPORT)
        del export["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]["visConfig"]

        mock_doc_svc.export_dashboard.return_value = export
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="new-1", name="D"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.update_tile(
                dashboard_id="dash-1",
                tile_name="Revenue",
                chart_type="bar",
            ))

        assert result["status"] == "updated"
        import_call = mock_doc_svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        qp = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        assert "visConfig" in qp, "visConfig must be created when missing"
        assert qp["visConfig"]["chartType"] == "bar"
        assert qp["visConfig"]["visType"] == "basic"

    def test_title_change_when_vis_config_missing(self, mock_doc_svc):
        """title= change with no visConfig → jsonHash cleanup still works."""
        import copy
        from omni_dash.api.documents import ImportResponse

        export = copy.deepcopy(_UPDATE_TILE_EXPORT)
        del export["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]["visConfig"]

        mock_doc_svc.export_dashboard.return_value = export
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="new-1", name="D"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.update_tile(
                dashboard_id="dash-1",
                tile_name="Revenue",
                title="New Name",
            ))

        assert result["status"] == "updated"
        import_call = mock_doc_svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        qp = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        assert qp["name"] == "New Name"


class TestUpdateTileQueryMutation:
    """update_tile must persist query changes even when query/queryJson keys are missing."""

    def test_sql_update_when_query_missing(self, mock_doc_svc):
        """sql= on a tile with NO query key → query and queryJson created."""
        import copy
        from omni_dash.api.documents import ImportResponse

        export = copy.deepcopy(_UPDATE_TILE_EXPORT)
        # Remove the query entirely
        del export["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]["query"]

        mock_doc_svc.export_dashboard.return_value = export
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="new-1", name="D"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.update_tile(
                dashboard_id="dash-1",
                tile_name="Revenue",
                sql="SELECT 1",
            ))

        assert result["status"] == "updated"
        import_call = mock_doc_svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        qp = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        assert qp["isSql"] is True
        # The query and queryJson should be created with the SQL
        assert "query" in qp
        q = qp["query"]
        # queryJson key should exist and contain the SQL
        q_json = q.get("queryJson", q)
        assert q_json["userEditedSQL"] == "SELECT 1"

    def test_fields_update_when_queryjson_missing(self, mock_doc_svc):
        """fields= on a tile where query exists but queryJson is missing."""
        import copy
        from omni_dash.api.documents import ImportResponse

        export = copy.deepcopy(_UPDATE_TILE_EXPORT)
        # Flatten: query has fields directly, no queryJson wrapper
        qp = export["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        flat_query = {"table": "t", "fields": ["t.old_field"]}
        qp["query"] = flat_query  # No queryJson key

        mock_doc_svc.export_dashboard.return_value = export
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="new-1", name="D"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.update_tile(
                dashboard_id="dash-1",
                tile_name="Revenue",
                fields=["t.new_field_a", "t.new_field_b"],
            ))

        assert result["status"] == "updated"
        import_call = mock_doc_svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        qp = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        # Fields should be updated in-place
        q = qp["query"]
        q_json = q.get("queryJson", q)
        assert q_json["fields"] == ["t.new_field_a", "t.new_field_b"]


# ---------------------------------------------------------------------------
# Omni Native AI MCP Tools
# ---------------------------------------------------------------------------


class TestAIGenerateQuery:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-1")
    @patch.object(mcp_server, "_get_ai_svc")
    def test_basic_generate(self, mock_get_ai, _):
        from omni_dash.api.ai import GeneratedQuery

        mock_ai = MagicMock()
        mock_ai.generate_query.return_value = GeneratedQuery(
            table="orders",
            fields=["orders.total", "orders.date"],
            sorts=[{"column_name": "orders.date", "sort_descending": True}],
            limit=100,
        )
        mock_get_ai.return_value = mock_ai

        result = json.loads(mcp_server.ai_generate_query(prompt="Show revenue"))
        assert result["table"] == "orders"
        assert result["fields"] == ["orders.total", "orders.date"]
        assert result["limit"] == 100

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-1")
    @patch.object(mcp_server, "_get_ai_svc")
    def test_with_topic(self, mock_get_ai, _):
        from omni_dash.api.ai import GeneratedQuery

        mock_ai = MagicMock()
        mock_ai.generate_query.return_value = GeneratedQuery(table="t", fields=["t.x"])
        mock_get_ai.return_value = mock_ai

        mcp_server.ai_generate_query(prompt="test", topic_name="orders")
        mock_ai.generate_query.assert_called_once_with(
            "model-1", "test", topic_name="orders",
        )

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-1")
    @patch.object(mcp_server, "_get_ai_svc")
    def test_api_error_returns_json(self, mock_get_ai, _):
        mock_ai = MagicMock()
        mock_ai.generate_query.side_effect = OmniAPIError(403, "AI not enabled")
        mock_get_ai.return_value = mock_ai

        result = json.loads(mcp_server.ai_generate_query(prompt="test"))
        assert "error" in result
        assert "AI not enabled" in result["error"]


class TestAIPickTopic:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-1")
    @patch.object(mcp_server, "_get_ai_svc")
    def test_basic_pick(self, mock_get_ai, _):
        mock_ai = MagicMock()
        mock_ai.pick_topic.return_value = "customers"
        mock_get_ai.return_value = mock_ai

        result = json.loads(mcp_server.ai_pick_topic(prompt="top customers"))
        assert result["topic"] == "customers"

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-1")
    @patch.object(mcp_server, "_get_ai_svc")
    def test_error_returns_json(self, mock_get_ai, _):
        mock_ai = MagicMock()
        mock_ai.pick_topic.side_effect = Exception("Connection failed")
        mock_get_ai.return_value = mock_ai

        result = json.loads(mcp_server.ai_pick_topic(prompt="test"))
        assert "error" in result


class TestAIAnalyze:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-1")
    @patch.object(mcp_server, "_get_ai_svc")
    def test_basic_analyze(self, mock_get_ai, _):
        from omni_dash.api.ai import AIJobResult, AIJobStatus

        mock_ai = MagicMock()
        mock_ai.create_job.return_value = AIJobStatus(
            job_id="job-1", conversation_id="conv-1",
        )
        mock_ai.wait_for_job.return_value = AIJobResult(
            message="Churn is increasing",
            result_summary="5% monthly churn",
            topic="customers",
            actions=[{"type": "query"}],
        )
        mock_get_ai.return_value = mock_ai

        result = json.loads(mcp_server.ai_analyze(prompt="Analyze churn"))
        assert result["summary"] == "5% monthly churn"
        assert result["message"] == "Churn is increasing"
        assert result["topic"] == "customers"
        assert result["actions_count"] == 1

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-1")
    @patch.object(mcp_server, "_get_ai_svc")
    def test_analyze_timeout_returns_error(self, mock_get_ai, _):
        from omni_dash.api.ai import AIJobStatus

        mock_ai = MagicMock()
        mock_ai.create_job.return_value = AIJobStatus(job_id="job-1")
        mock_ai.wait_for_job.side_effect = OmniAPIError(0, "timed out")
        mock_get_ai.return_value = mock_ai

        result = json.loads(mcp_server.ai_analyze(prompt="test"))
        assert "error" in result
        assert "timed out" in result["error"]


# ---------------------------------------------------------------------------
# get_dashboard_filters
# ---------------------------------------------------------------------------


class TestGetDashboardFilters:
    @patch.object(mcp_server, "_get_doc_svc")
    def test_returns_filters(self, mock_get_doc):
        doc_svc = MagicMock()
        doc_svc.get_filters.return_value = {
            "identifier": "dash-1",
            "filters": {"f1": {"type": "string", "kind": "EQUALS", "values": ["active"]}},
            "filterOrder": ["f1"],
            "controls": [],
        }
        mock_get_doc.return_value = doc_svc

        result = json.loads(mcp_server.get_dashboard_filters("dash-1"))
        assert result["identifier"] == "dash-1"
        assert "f1" in result["filters"]

    @patch.object(mcp_server, "_get_doc_svc")
    def test_error_returns_json(self, mock_get_doc):
        doc_svc = MagicMock()
        doc_svc.get_filters.side_effect = Exception("Connection failed")
        mock_get_doc.return_value = doc_svc

        result = json.loads(mcp_server.get_dashboard_filters("dash-1"))
        assert "error" in result


# ---------------------------------------------------------------------------
# update_dashboard_filters
# ---------------------------------------------------------------------------


class TestUpdateDashboardFilters:
    @patch.object(mcp_server, "_get_doc_svc")
    def test_updates_filters(self, mock_get_doc):
        doc_svc = MagicMock()
        doc_svc.update_filters.return_value = {
            "filters": {"f1": {"values": ["shipped"]}},
            "filterOrder": ["f1"],
        }
        mock_get_doc.return_value = doc_svc

        result = json.loads(mcp_server.update_dashboard_filters(
            dashboard_id="dash-1",
            filters={"f1": {"values": ["shipped"]}},
        ))
        assert result["filters"]["f1"]["values"] == ["shipped"]

    @patch.object(mcp_server, "_get_doc_svc")
    def test_update_filter_order(self, mock_get_doc):
        doc_svc = MagicMock()
        doc_svc.update_filters.return_value = {"filterOrder": ["f2", "f1"]}
        mock_get_doc.return_value = doc_svc

        result = json.loads(mcp_server.update_dashboard_filters(
            dashboard_id="dash-1",
            filter_order=["f2", "f1"],
        ))
        assert result["filterOrder"] == ["f2", "f1"]

    @patch.object(mcp_server, "_get_doc_svc")
    def test_error_returns_json(self, mock_get_doc):
        doc_svc = MagicMock()
        doc_svc.update_filters.side_effect = ValueError("Must provide at least one")
        mock_get_doc.return_value = doc_svc

        result = json.loads(mcp_server.update_dashboard_filters(dashboard_id="dash-1"))
        assert "error" in result


# ---------------------------------------------------------------------------
# PR #42: Field validation, update_tile, and UX fixes
# ---------------------------------------------------------------------------


class TestFieldValidationBaseView:
    """_validate_tile_fields should accept base_view-qualified field names."""

    def _mock_model_svc(self, topic_name, base_view, field_names):
        model_svc = MagicMock()

        def get_topic(model_id, table_name):
            topic = MagicMock()
            topic.base_view = base_view
            topic.fields = [
                {
                    "name": f,
                    "qualified_name": f"{base_view}.{f}",
                }
                for f in field_names
            ]
            return topic

        model_svc.get_topic_native.side_effect = get_topic
        return model_svc

    @patch.object(mcp_server, "_get_model_svc")
    def test_accepts_base_view_qualified_fields(self, mock_get_model_svc):
        """Fields qualified with base_view should pass validation."""
        mock_get_model_svc.return_value = self._mock_model_svc(
            "Google Ads Performance", "google_ads_performance", ["clicks", "impressions"]
        )
        tiles = [
            {
                "name": "T1",
                "query": {
                    "table": "Google Ads Performance",
                    "fields": [
                        "google_ads_performance.clicks",
                        "google_ads_performance.impressions",
                    ],
                },
            }
        ]
        errors = mcp_server._validate_tile_fields(tiles, "model-1")
        assert errors == []

    @patch.object(mcp_server, "_get_model_svc")
    def test_accepts_topic_name_qualified_fields(self, mock_get_model_svc):
        """Fields qualified with topic name should also pass."""
        mock_get_model_svc.return_value = self._mock_model_svc(
            "orders", "fact_orders", ["order_id", "total"]
        )
        tiles = [
            {
                "name": "T1",
                "query": {
                    "table": "orders",
                    "fields": ["orders.order_id", "orders.total"],
                },
            }
        ]
        errors = mcp_server._validate_tile_fields(tiles, "model-1")
        assert errors == []

    @patch.object(mcp_server, "_get_model_svc")
    def test_rejects_truly_invalid_fields(self, mock_get_model_svc):
        """Non-existent fields should still be rejected."""
        mock_get_model_svc.return_value = self._mock_model_svc(
            "orders", "fact_orders", ["order_id", "total"]
        )
        tiles = [
            {
                "name": "T1",
                "query": {
                    "table": "orders",
                    "fields": ["orders.order_id", "orders.nonexistent"],
                },
            }
        ]
        errors = mcp_server._validate_tile_fields(tiles, "model-1")
        assert len(errors) == 1
        assert "nonexistent" in errors[0]


class TestUpdateTileSetdefaultFix:
    """update_tile should not create self-referencing dicts."""

    def test_sql_on_empty_query(self, mock_doc_svc):
        """Setting SQL on a tile with no queryJson should not create circular ref."""
        import copy
        from omni_dash.api.documents import ImportResponse

        # Export with no queryJson key
        export = copy.deepcopy(_UPDATE_TILE_EXPORT)
        qp = export["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        qp["query"] = {}  # Empty query — no queryJson

        mock_doc_svc.export_dashboard.return_value = export
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="new-1", name="D"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.update_tile(
                dashboard_id="dash-1",
                tile_name="Revenue",
                sql="SELECT 1",
            ))

        assert result["status"] == "updated"
        # Verify the import payload can be serialized without RecursionError
        import_call = mock_doc_svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        # This would crash with RecursionError if the circular ref bug existed
        json.dumps(imported_data)
        qp_data = imported_data["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        assert qp_data["isSql"] is True
        assert qp_data["query"]["queryJson"]["userEditedSQL"] == "SELECT 1"

    def test_fields_on_empty_query(self, mock_doc_svc):
        """Setting fields on a tile with no queryJson should work."""
        import copy
        from omni_dash.api.documents import ImportResponse

        export = copy.deepcopy(_UPDATE_TILE_EXPORT)
        qp = export["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        qp["query"] = {}

        mock_doc_svc.export_dashboard.return_value = export
        mock_doc_svc.import_dashboard.return_value = ImportResponse(
            document_id="new-1", name="D"
        )
        mock_doc_svc.delete_dashboard.return_value = None

        with patch("omni_dash.mcp.server.get_settings") as mock_s:
            mock_s.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
            result = json.loads(mcp_server.update_tile(
                dashboard_id="dash-1",
                tile_name="Revenue",
                fields=["t.rev", "t.cost"],
            ))

        assert result["status"] == "updated"
        import_call = mock_doc_svc.import_dashboard.call_args
        imported_data = import_call[0][0]
        json.dumps(imported_data)  # No RecursionError


class TestGetDashboardTruncation:
    """get_dashboard should warn when tiles are truncated."""

    def test_shows_note_when_tiles_truncated(self, mock_doc_svc):
        from omni_dash.api.documents import DashboardResponse

        mock_doc_svc.get_dashboard.return_value = DashboardResponse(
            document_id="abc",
            name="Big Dashboard",
            query_presentations=[{"id": f"qp{i}"} for i in range(15)],
        )

        result = json.loads(mcp_server.get_dashboard("abc"))
        assert result["tile_count"] == 15
        assert len(result["tiles"]) == 10
        assert "note" in result
        assert "15" in result["note"]

    def test_no_note_when_under_limit(self, mock_doc_svc):
        from omni_dash.api.documents import DashboardResponse

        mock_doc_svc.get_dashboard.return_value = DashboardResponse(
            document_id="abc",
            name="Small Dashboard",
            query_presentations=[{"id": "qp1"}, {"id": "qp2"}],
        )

        result = json.loads(mcp_server.get_dashboard("abc"))
        assert result["tile_count"] == 2
        assert "note" not in result


class TestProfileDataFieldLimit:
    """profile_data should note when fields are auto-limited."""

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-1")
    @patch.object(mcp_server, "_get_model_svc")
    @patch.object(mcp_server, "_get_query_runner")
    def test_notes_field_truncation(self, mock_qr, mock_model, _):
        topic = MagicMock()
        topic.fields = [
            {"name": f"field_{i}"} for i in range(50)
        ]
        mock_model.return_value.get_topic_native.return_value = topic

        runner = MagicMock()
        query_result = MagicMock()
        query_result.rows = [{"field_0": "val"}]
        query_result.row_count = 1
        runner.run.return_value = query_result
        mock_qr.return_value = runner

        result = json.loads(mcp_server.profile_data(table="big_table"))
        assert "note" in result
        assert "50" in result["note"]
        assert "20" in result["note"]

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-1")
    @patch.object(mcp_server, "_get_model_svc")
    @patch.object(mcp_server, "_get_query_runner")
    def test_no_note_when_under_limit(self, mock_qr, mock_model, _):
        topic = MagicMock()
        topic.fields = [{"name": f"field_{i}"} for i in range(5)]
        mock_model.return_value.get_topic_native.return_value = topic

        runner = MagicMock()
        query_result = MagicMock()
        query_result.rows = [{"field_0": "val"}]
        query_result.row_count = 1
        runner.run.return_value = query_result
        mock_qr.return_value = runner

        result = json.loads(mcp_server.profile_data(table="small_table"))
        assert "note" not in result


class TestEmptyModelIdCheck:
    """Tools should return clear error when no model_id is available."""

    @patch.object(mcp_server, "_get_shared_model_id", return_value="")
    def test_suggest_chart_empty_model(self, _):
        result = json.loads(mcp_server.suggest_chart(table="t"))
        assert "error" in result
        assert "model_id" in result["error"].lower() or "OMNI_SHARED_MODEL_ID" in result["error"]

    @patch.object(mcp_server, "_get_shared_model_id", return_value="")
    def test_profile_data_empty_model(self, _):
        result = json.loads(mcp_server.profile_data(table="t"))
        assert "error" in result

    @patch.object(mcp_server, "_get_shared_model_id", return_value="")
    def test_generate_dashboard_empty_model(self, _):
        result = json.loads(mcp_server.generate_dashboard(prompt="test"))
        assert "error" in result


class TestSuggestChartFieldMatchingBaseView:
    """suggest_chart should match fields by qualified_name (base_view.field)."""

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-1")
    @patch.object(mcp_server, "_get_model_svc")
    def test_matches_base_view_qualified_fields(self, mock_model, _):
        topic = MagicMock()
        topic.fields = [
            {"name": "clicks", "qualified_name": "google_ads.clicks", "type": "dimension"},
            {"name": "impressions", "qualified_name": "google_ads.impressions", "type": "dimension"},
            {"name": "cost", "qualified_name": "google_ads.cost", "type": "measure"},
        ]
        mock_model.return_value.get_topic_native.return_value = topic

        with patch("omni_dash.ai.chart_recommender.classify_field") as mock_clf, \
             patch("omni_dash.ai.chart_recommender.recommend_chart") as mock_rec:
            mock_clf.side_effect = lambda f: f
            mock_rec.return_value = MagicMock(
                chart_type="bar", confidence=0.9,
                reasoning="test", vis_config={}, alternatives=[],
            )
            # Use qualified_name format from get_topic_fields output
            result = json.loads(mcp_server.suggest_chart(
                table="Google Ads",
                fields=["google_ads.clicks", "google_ads.cost"],
            ))

        assert result.get("fields_analyzed") == 2


class TestCreateDashboardFilterCarrythrough:
    """create_dashboard should carry filterConfig through vis config patching."""

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch.object(mcp_server, "_get_doc_svc")
    @patch("omni_dash.mcp.server.get_settings")
    def test_filters_in_reimported_export(self, mock_settings, mock_get_doc_svc, _):
        from omni_dash.api.documents import DashboardResponse, ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        svc = MagicMock()
        svc.create_dashboard.return_value = DashboardResponse(
            document_id="skel-1", name="Test"
        )
        # Skeleton export has no filters (Omni ignores them on create)
        svc.export_dashboard.return_value = {
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Chart",
                                "visConfig": {"visType": None, "chartType": "line"},
                            }
                        }
                    ],
                    "filterConfig": {},
                    "filterOrder": [],
                }
            },
            "document": {"sharedModelId": "model-123"},
            "workbookModel": {},
            "exportVersion": "0.1",
        }
        svc.import_dashboard.return_value = ImportResponse(
            document_id="final-1", name="Test"
        )
        mock_get_doc_svc.return_value = svc

        tiles = [{
            "name": "Chart",
            "chart_type": "line",
            "query": {"table": "t", "fields": ["t.f"]},
            "vis_config": {"x_axis": "t.f"},
        }]
        # Pass dashboard-level filters
        filters = [{"field": "t.date", "filter_type": "date", "label": "Date Filter"}]

        result = json.loads(mcp_server.create_dashboard(
            name="Test", tiles=tiles, filters=filters,
        ))
        assert result["status"] == "created"

        # Check the reimport payload contains filterConfig
        import_call = svc.import_dashboard.call_args
        imported = import_call[0][0]
        qpc = imported["dashboard"]["queryPresentationCollection"]
        assert qpc.get("filterConfig"), "filterConfig should be non-empty"
        assert qpc.get("filterOrder"), "filterOrder should be non-empty"


class TestPerKeyCacheTTL:
    """Cache should evict keys independently, not all at once."""

    def test_fresh_key_survives_when_old_key_expires(self):
        import time
        from omni_dash.api.models import ModelService
        svc = ModelService(MagicMock(), cache_ttl=10)

        # Set old key, backdate its timestamp
        svc._set_cache("old", {"v": 1})
        svc._cache_ts_map["old"] = time.monotonic() - 20  # 20s ago, past TTL

        # Set fresh key
        svc._set_cache("fresh", {"v": 2})

        # Old key should be expired, fresh key should survive
        assert svc._get_cache("old") is None
        assert svc._get_cache("fresh") == {"v": 2}


class TestAddTilesOrphanCleanup:
    """Temp skeleton must be cleaned up even if reimport fails."""

    def test_temp_deleted_on_reimport_failure(self, mock_doc_svc):
        """If reimport fails, temp skeleton should still be deleted."""
        mcp_server._shared_model_id = "m1"

        orig_export = {
            "document": {"name": "Test", "folderId": "f1", "modelId": "m1"},
            "dashboard": {
                "ephemeral": "1:abc123",
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Existing",
                                "miniUuid": "abc123",
                                "query": {"queryJson": {"fields": ["t.a"]}},
                            }
                        }
                    ],
                },
                "metadata": {"layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 12, "h": 6}]}},
            },
            "workbookModel": {"base_model_id": "m1"},
        }
        temp_export = {
            "document": {"name": "__add_tiles_tmp__", "modelId": "m1"},
            "dashboard": {
                "ephemeral": "1:def456",
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "New Tile",
                                "miniUuid": "def456",
                                "query": {"queryJson": {"fields": ["t.b"]}},
                            }
                        }
                    ],
                },
                "metadata": {"layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 6, "h": 6}]}},
            },
        }

        # Mock _create_with_vis_configs to return a temp dashboard ID
        # and mock export to return temp export when called with that ID
        mock_doc_svc.export_dashboard.side_effect = [orig_export, temp_export]

        # Reimport in add_tiles (the merge step) FAILS
        mock_doc_svc.import_dashboard.side_effect = Exception("reimport boom")

        tiles = [{"name": "New Tile", "chart_type": "line", "query": {"table": "t", "fields": ["t.b"]}}]

        with (
            patch.object(mcp_server, "_validate_tile_fields", return_value=[]),
            patch.object(mcp_server, "_create_with_vis_configs", return_value=("temp-id", "__add_tiles_tmp__")),
        ):
            result = json.loads(mcp_server.add_tiles_to_dashboard("orig-id", tiles))

        assert "error" in result

        # Temp skeleton should be deleted even though reimport failed
        delete_calls = [
            c[0][0] for c in mock_doc_svc.delete_dashboard.call_args_list
        ]
        assert "temp-id" in delete_calls, (
            f"Temp skeleton not cleaned up. Delete calls: {delete_calls}"
        )

    def test_original_not_deleted_on_reimport_failure(self, mock_doc_svc):
        """If reimport fails, original dashboard should NOT be deleted."""
        mcp_server._shared_model_id = "m1"

        orig_export = {
            "document": {"name": "Test", "folderId": "f1", "modelId": "m1"},
            "dashboard": {
                "ephemeral": "1:abc123",
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Existing",
                                "miniUuid": "abc123",
                                "query": {"queryJson": {"fields": ["t.a"]}},
                            }
                        }
                    ],
                },
                "metadata": {"layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 12, "h": 6}]}},
            },
            "workbookModel": {"base_model_id": "m1"},
        }
        temp_export = {
            "document": {"name": "__add_tiles_tmp__", "modelId": "m1"},
            "dashboard": {
                "ephemeral": "1:def456",
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "New Tile",
                                "miniUuid": "def456",
                                "query": {"queryJson": {"fields": ["t.b"]}},
                            }
                        }
                    ],
                },
                "metadata": {"layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 6, "h": 6}]}},
            },
        }

        mock_doc_svc.export_dashboard.side_effect = [orig_export, temp_export]
        mock_doc_svc.import_dashboard.side_effect = Exception("reimport boom")

        tiles = [{"name": "New Tile", "chart_type": "line", "query": {"table": "t", "fields": ["t.b"]}}]

        with (
            patch.object(mcp_server, "_validate_tile_fields", return_value=[]),
            patch.object(mcp_server, "_create_with_vis_configs", return_value=("temp-id", "__add_tiles_tmp__")),
        ):
            mcp_server.add_tiles_to_dashboard("orig-id", tiles)

        # Original should NOT be in delete calls
        delete_calls = [
            c[0][0] for c in mock_doc_svc.delete_dashboard.call_args_list
        ]
        assert "orig-id" not in delete_calls, (
            f"Original dashboard deleted despite failed reimport! Calls: {delete_calls}"
        )


class TestProfileDataPublicAPI:
    """profile_data should use QueryBuilder public API."""

    def test_uses_public_fields_method(self, mock_model_svc, mock_query_runner):
        """Verify builder.fields() is called, not builder._fields assignment."""
        mcp_server._shared_model_id = "m1"

        # Mock topic fields
        mock_model_svc.get_topic_native.return_value = MagicMock(
            fields=[{"name": "col_a"}, {"name": "col_b"}],
            base_view="my_table",
        )

        # Mock query result
        mock_query_runner.run.return_value = MagicMock(
            rows=[{"my_table.col_a": 1, "my_table.col_b": "x"}],
            fields=["my_table.col_a", "my_table.col_b"],
            row_count=1,
        )

        with patch.object(mcp_server, "_resolve_table_name", return_value="my_table"):
            result = json.loads(mcp_server.profile_data("my_table"))

        assert "error" not in result
        # Verify fields were passed to the query
        call_args = mock_query_runner.run.call_args[0][0]
        assert "my_table.col_a" in call_args.fields
        assert "my_table.col_b" in call_args.fields

    def test_handles_bare_column_names_in_rows(self, mock_model_svc, mock_query_runner):
        """Arrow results may return bare column names instead of qualified.
        profile_data should fall back to bare names when qualified lookup fails."""
        mcp_server._shared_model_id = "m1"

        mock_model_svc.get_topic_native.return_value = MagicMock(
            fields=[{"name": "col_a"}, {"name": "col_b"}],
            base_view="my_table",
        )

        # Rows have BARE column names (as Arrow decoder returns)
        mock_query_runner.run.return_value = MagicMock(
            rows=[
                {"col_a": 10, "col_b": "hello"},
                {"col_a": 20, "col_b": "world"},
                {"col_a": None, "col_b": "test"},
            ],
            fields=["col_a", "col_b"],
            row_count=3,
        )

        with patch.object(mcp_server, "_resolve_table_name", return_value="my_table"):
            result = json.loads(mcp_server.profile_data("my_table"))

        assert "error" not in result
        fields = result["fields"]

        # col_a should have found values via bare name fallback
        col_a = fields["my_table.col_a"]
        assert col_a["sample_count"] == 3
        assert col_a["null_count"] == 1
        assert col_a["distinct_count"] == 2
        assert col_a["inferred_type"] == "number"

        # col_b should also have found values
        col_b = fields["my_table.col_b"]
        assert col_b["sample_count"] == 3
        assert col_b["null_count"] == 0
        assert col_b["distinct_count"] == 3


class TestQueryDataSortPublicAPI:
    """query_data should use QueryBuilder.sort() for auto-qualification."""

    def test_sorts_auto_qualified(self, mock_query_runner):
        """Bare sort column names should be auto-qualified with table name."""
        mcp_server._shared_model_id = "m1"

        mock_query_runner.run.return_value = MagicMock(
            rows=[], fields=[], row_count=0, truncated=False,
        )

        with patch.object(mcp_server, "_resolve_table_name", return_value="my_table"):
            result = json.loads(mcp_server.query_data(
                table="my_table",
                fields=["my_table.revenue"],
                sorts=[{"column_name": "revenue", "sort_descending": True}],
            ))

        assert "error" not in result
        # The sort column should have been auto-qualified
        call_args = mock_query_runner.run.call_args[0][0]
        sort_cols = [s["column_name"] for s in call_args.sorts]
        assert "my_table.revenue" in sort_cols


class TestEmptyDocIdGuard:
    """Reimport returning empty document_id should not delete originals."""

    def test_create_with_vis_configs_raises_on_empty_id(self, mock_doc_svc):
        """If reimport returns empty doc_id, skeleton should be preserved."""
        mock_doc_svc.create_dashboard.return_value = MagicMock(document_id="skel-1")
        mock_doc_svc.export_dashboard.return_value = {
            "document": {"name": "Test", "modelId": "m1"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Tile1",
                                "visConfig": {},
                                "query": {"queryJson": {}},
                            }
                        }
                    ]
                },
                "metadata": {"layouts": {"lg": []}},
            },
            "workbookModel": {"base_model_id": "m1"},
        }
        # Reimport returns empty document_id
        mock_doc_svc.import_dashboard.return_value = MagicMock(
            document_id="", name="Test"
        )

        payload = {
            "queryPresentations": [
                {
                    "name": "Tile1",
                    "query": {"table": "t", "fields": ["t.a"]},
                    "visConfig": {"visType": "basic", "chartType": "line"},
                }
            ],
            "modelId": "m1",
        }

        with pytest.raises(Exception, match="no document_id"):
            mcp_server._create_with_vis_configs(
                payload, name="Test", folder_id=None,
            )

        # Skeleton should NOT have been deleted
        delete_calls = [
            c[0][0] for c in mock_doc_svc.delete_dashboard.call_args_list
        ]
        assert "skel-1" not in delete_calls

    def test_add_tiles_preserves_original_on_empty_reimport_id(self, mock_doc_svc):
        """add_tiles should not delete original if reimport returns empty id."""
        mcp_server._shared_model_id = "m1"

        orig_export = {
            "document": {"name": "Test", "folderId": "f1", "modelId": "m1"},
            "dashboard": {
                "ephemeral": "1:abc",
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {"queryPresentation": {"name": "Existing", "miniUuid": "abc", "query": {"queryJson": {"fields": ["t.a"]}}}}
                    ],
                },
                "metadata": {"layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 12, "h": 6}]}},
            },
            "workbookModel": {"base_model_id": "m1"},
        }
        temp_export = {
            "document": {"name": "tmp", "modelId": "m1"},
            "dashboard": {
                "ephemeral": "1:def",
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {"queryPresentation": {"name": "New", "miniUuid": "def", "query": {"queryJson": {"fields": ["t.b"]}}}}
                    ],
                },
                "metadata": {"layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 6, "h": 6}]}},
            },
        }
        mock_doc_svc.export_dashboard.side_effect = [orig_export, temp_export]
        mock_doc_svc.import_dashboard.return_value = MagicMock(document_id="", name="Test")

        tiles = [{"name": "New", "chart_type": "line", "query": {"table": "t", "fields": ["t.b"]}}]

        with (
            patch.object(mcp_server, "_validate_tile_fields", return_value=[]),
            patch.object(mcp_server, "_create_with_vis_configs", return_value=("temp-id", "tmp")),
        ):
            result = json.loads(mcp_server.add_tiles_to_dashboard("orig-id", tiles))

        assert "error" in result
        delete_calls = [c[0][0] for c in mock_doc_svc.delete_dashboard.call_args_list]
        assert "orig-id" not in delete_calls, "Original should not be deleted on empty reimport ID"


class TestImportFallbackLayout:
    """Import fallback should produce valid full-width stacked layout."""

    def test_tiles_dont_overflow_grid(self, mock_doc_svc):
        """All tiles should have x=0 and w=24 (full width in 24-col grid)."""
        payload = {
            "queryPresentations": [
                {"name": "Tile1", "query": {"table": "t", "fields": ["t.a"]}},
                {"name": "Tile2", "query": {"table": "t", "fields": ["t.b"]}},
                {"name": "Tile3", "query": {"table": "t", "fields": ["t.c"]}},
            ],
            "modelId": "m1",
        }

        mock_doc_svc.import_dashboard.return_value = MagicMock(
            document_id="new-id", name="Test"
        )

        mcp_server._create_via_import_fallback(
            mock_doc_svc, payload, name="Test", folder_id=None,
        )

        call_args = mock_doc_svc.import_dashboard.call_args[0][0]
        layouts = call_args["dashboard"]["metadata"]["layouts"]["lg"]

        for layout in layouts:
            assert layout["x"] == 0, f"Tile at x={layout['x']} overflows grid"
            assert layout["w"] == 24, f"Tile w={layout['w']} should be full-width (24-col)"
            assert layout["x"] + layout["w"] <= 24, "Layout overflows 24-column grid"


class TestUpdateTilePartialStatus:
    """update_tile should report partial status if delete fails."""

    def test_returns_partial_on_delete_failure(self, mock_doc_svc):
        """Should return status=partial, not status=updated, when delete fails."""
        mcp_server._shared_model_id = "m1"

        mock_doc_svc.export_dashboard.return_value = {
            "document": {"name": "Test", "folderId": "f1", "modelId": "m1"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "My Tile",
                                "query": {"queryJson": {"fields": ["t.a"]}},
                                "visConfig": {"chartType": "line", "visType": "basic"},
                            }
                        }
                    ]
                },
                "metadata": {"layouts": {"lg": []}},
            },
            "workbookModel": {"base_model_id": "m1"},
        }
        mock_doc_svc.import_dashboard.return_value = MagicMock(
            document_id="new-id", name="Test"
        )
        mock_doc_svc.delete_dashboard.side_effect = Exception("delete boom")

        result = json.loads(mcp_server.update_tile(
            dashboard_id="old-id",
            tile_name="My Tile",
            chart_type="bar",
        ))

        assert result["status"] == "partial"
        assert "warning" in result
        assert result["dashboard_id"] == "new-id"
        assert result["old_dashboard_id"] == "old-id"


class TestFilterOverrideAlwaysApplied:
    """Tile query overrides should always be applied, not gated on emptiness."""

    def test_filters_override_existing_export_value(self, mock_doc_svc):
        """User-specified filters should replace Omni's placeholder values."""
        # Set up a skeleton creation that will return an export with pre-existing filters
        mock_doc_svc.create_dashboard.return_value = MagicMock(document_id="skel-1")
        mock_doc_svc.export_dashboard.return_value = {
            "document": {"name": "Test", "modelId": "m1"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Tile1",
                                "query": {
                                    "queryJson": {
                                        "fields": ["t.a"],
                                        "filters": {"t.date": {"kind": "PLACEHOLDER"}},
                                    }
                                },
                                "visConfig": {},
                            }
                        }
                    ]
                },
                "metadata": {"layouts": {"lg": []}},
            },
            "workbookModel": {"base_model_id": "m1"},
        }
        mock_doc_svc.import_dashboard.return_value = MagicMock(
            document_id="final-id", name="Test"
        )

        user_filters = {"t.date": {"kind": "TIME", "type": "date", "values": ["90 days ago"]}}
        payload = {
            "queryPresentations": [
                {
                    "name": "Tile1",
                    "query": {
                        "table": "t",
                        "fields": ["t.a"],
                        "filters": user_filters,
                    },
                    "visConfig": {"visType": "basic", "chartType": "line"},
                }
            ],
            "modelId": "m1",
        }

        mcp_server._create_with_vis_configs(
            payload, name="Test", folder_id=None,
        )

        # Check the reimported payload has user's filters, not the placeholder
        import_call = mock_doc_svc.import_dashboard.call_args[0][0]
        qp = import_call["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        actual_filters = qp["query"]["queryJson"]["filters"]
        assert actual_filters == user_filters


class TestSharedModelIdCache:
    """_get_shared_model_id should cache its result."""

    def test_caches_env_var_result(self):
        """Once resolved from env var, should not re-read settings."""
        with patch.object(mcp_server, "get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(omni_shared_model_id="m1")

            # First call
            result1 = mcp_server._get_shared_model_id()
            assert result1 == "m1"

            # Second call should use cache (settings not re-read)
            mock_settings.reset_mock()
            result2 = mcp_server._get_shared_model_id()
            assert result2 == "m1"
            mock_settings.assert_not_called()

    def test_caches_discovered_model_id(self, mock_model_svc):
        """Once discovered via API, should not call list_models again."""
        with patch.object(mcp_server, "get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(omni_shared_model_id="")
            mock_model_svc.list_models.return_value = [
                MagicMock(id="found-id", model_kind="shared"),
            ]

            result1 = mcp_server._get_shared_model_id()
            assert result1 == "found-id"

            # Second call should use cache
            mock_model_svc.list_models.reset_mock()
            result2 = mcp_server._get_shared_model_id()
            assert result2 == "found-id"
            mock_model_svc.list_models.assert_not_called()


class TestMoveDashboardDeleteCatch:
    """move_dashboard should catch all exceptions on delete, not just OmniDashError."""

    def test_catches_runtime_error_on_delete(self, mock_doc_svc):
        """Non-OmniDashError exceptions should still produce partial status."""
        mock_doc_svc.export_dashboard.return_value = {
            "document": {"name": "Test", "modelId": "m1"},
            "dashboard": {},
            "workbookModel": {"base_model_id": "m1"},
        }
        mock_doc_svc.import_dashboard.return_value = MagicMock(
            document_id="new-id", name="Test"
        )
        mock_doc_svc.delete_dashboard.side_effect = RuntimeError("connection reset")

        result = json.loads(mcp_server.move_dashboard("old-id", "folder-1"))

        assert result["status"] == "partial"
        assert "new_dashboard_id" in result
        assert result["new_dashboard_id"] == "new-id"


class TestPayloadNotMutated:
    """_create_with_vis_configs should not mutate the caller's payload."""

    def test_vis_config_preserved_in_caller_payload(self, mock_doc_svc):
        """After _create_with_vis_configs, original payload should still have visConfig."""
        mock_doc_svc.create_dashboard.return_value = MagicMock(document_id="skel-1")
        mock_doc_svc.export_dashboard.return_value = {
            "document": {"name": "Test", "modelId": "m1"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Tile1",
                                "visConfig": {},
                                "query": {"queryJson": {}},
                            }
                        }
                    ]
                },
                "metadata": {"layouts": {"lg": []}},
            },
            "workbookModel": {"base_model_id": "m1"},
        }
        mock_doc_svc.import_dashboard.return_value = MagicMock(
            document_id="final-id", name="Test"
        )

        # Keep a reference to the original vis config
        original_vc = {"visType": "basic", "chartType": "line"}
        payload = {
            "queryPresentations": [
                {
                    "name": "Tile1",
                    "query": {"table": "t", "fields": ["t.a"]},
                    "visConfig": dict(original_vc),  # copy
                }
            ],
            "modelId": "m1",
        }
        # Store a reference to the ORIGINAL qp list before the call
        original_qps = payload["queryPresentations"]

        mcp_server._create_with_vis_configs(
            payload, name="Test", folder_id=None,
        )

        # The payload's queryPresentations should now be a different list
        # (deep-copied), so the original list should still have visConfig
        assert "visConfig" in original_qps[0], (
            "Original payload's visConfig was mutated by _create_with_vis_configs"
        )


# ---------------------------------------------------------------------------
# add_tiles: temp membership IDs are cleaned before merge
# ---------------------------------------------------------------------------


class TestAddTilesMembershipCleaning:
    """add_tiles_to_dashboard cleans temp membership IDs before merge."""

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch("omni_dash.mcp.server.get_settings")
    def test_temp_memberships_get_collection_id_updated(
        self, mock_settings, _, mock_doc_svc,
    ):
        from omni_dash.api.documents import ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://test.omniapp.co")
        svc = mock_doc_svc

        # Original export with 1 tile
        orig_export = {
            "document": {"name": "Dashboard", "folderId": "f1"},
            "dashboard": {
                "queryPresentationCollection": {
                    "id": "orig-collection-id",
                    "queryPresentationCollectionMemberships": [
                        {
                            "id": "orig-membership-1",
                            "queryPresentationCollectionId": "orig-collection-id",
                            "queryPresentation": {
                                "id": "orig-qp-1",
                                "name": "Existing",
                                "miniUuid": "AAA11111",
                                "queryIdentifierMapKey": "1",
                                "query": {"queryJson": {"table": "t", "fields": ["t.a"]}},
                                "visConfig": {"visType": "basic", "chartType": "line"},
                            },
                        }
                    ],
                },
                "metadata": {"layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 24, "h": 51}]}},
                "ephemeral": "1:AAA11111",
            },
            "exportVersion": "0.1",
        }

        # Temp skeleton export with 1 new tile
        temp_export = {
            "document": {"name": "__add_tiles_tmp__"},
            "dashboard": {
                "queryPresentationCollection": {
                    "id": "temp-collection-id",
                    "queryPresentationCollectionMemberships": [
                        {
                            "id": "temp-membership-1",
                            "queryPresentationCollectionId": "temp-collection-id",
                            "queryPresentation": {
                                "id": "temp-qp-1",
                                "name": "New Tile",
                                "miniUuid": "BBB22222",
                                "queryIdentifierMapKey": "1",
                                "query": {"queryJson": {"table": "t", "fields": ["t.b"]}},
                                "visConfig": {"visType": "basic", "chartType": "bar"},
                            },
                        }
                    ],
                },
                "metadata": {"layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 24, "h": 51}]}},
                "ephemeral": "1:BBB22222",
            },
            "exportVersion": "0.1",
        }

        # Mock: export original, create temp, export temp, import merged
        svc.export_dashboard.side_effect = [orig_export, temp_export]
        svc.import_dashboard.return_value = ImportResponse(
            document_id="new-dash-id", name="Dashboard",
        )

        # Capture the import payload
        import_calls = []
        orig_import = svc.import_dashboard
        def capture_import(*args, **kwargs):
            import_calls.append(args[0] if args else kwargs.get("export_data"))
            return orig_import(*args, **kwargs)
        svc.import_dashboard = capture_import

        with patch.object(mcp_server, "_create_with_vis_configs", return_value=("temp-id", "__add_tiles_tmp__")):
            result = json.loads(mcp_server.add_tiles_to_dashboard(
                "orig-id",
                [{"name": "New Tile", "chart_type": "bar", "query": {"table": "t", "fields": ["t.b"]}}],
            ))

        assert result["status"] == "updated"
        assert result["new_tile_count"] == 2

        # Verify the import payload
        assert len(import_calls) == 1
        imported = import_calls[0]
        memberships = (
            imported["dashboard"]["queryPresentationCollection"]
            ["queryPresentationCollectionMemberships"]
        )
        assert len(memberships) == 2

        # Original membership keeps its collection ID
        assert memberships[0]["queryPresentationCollectionId"] == "orig-collection-id"

        # Temp membership: collectionId removed, membership-level ID removed,
        # QP-level ID regenerated as fresh UUID
        assert "queryPresentationCollectionId" not in memberships[1]
        assert "id" not in memberships[1]  # membership-level ID removed
        assert memberships[1]["queryPresentation"]["id"] != "temp-qp-1"  # fresh UUID
        assert memberships[1]["queryPresentation"]["miniUuid"] == "BBB22222"

        # queryIdentifierMapKey removed from temp memberships (Omni
        # assigns new keys on import when absent)
        assert "queryIdentifierMapKey" not in memberships[1]["queryPresentation"]
        # Original membership keeps its queryIdentifierMapKey
        assert memberships[0]["queryPresentation"]["queryIdentifierMapKey"] == "1"

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    @patch("omni_dash.mcp.server.get_settings")
    def test_temp_memberships_strip_all_dedup_ids(
        self, mock_settings, _, mock_doc_svc,
    ):
        """Omni deduplicates tiles by visConfigId, queryId, query.id,
        visConfig.id, and jsonHash. All must be stripped from temp
        memberships to prevent silent tile loss on import."""
        from omni_dash.api.documents import ImportResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://test.omniapp.co")
        svc = mock_doc_svc

        orig_export = {
            "document": {"name": "Dashboard", "folderId": "f1"},
            "dashboard": {
                "queryPresentationCollection": {
                    "id": "orig-coll",
                    "queryPresentationCollectionMemberships": [
                        {
                            "id": "m1",
                            "queryPresentationCollectionId": "orig-coll",
                            "queryPresentation": {
                                "id": "qp1",
                                "name": "Existing",
                                "miniUuid": "AAA11111",
                                "queryIdentifierMapKey": "1",
                                "visConfigId": "vc-orig-1",
                                "queryId": "q-orig-1",
                                "query": {
                                    "id": "q-orig-1",
                                    "jsonHash": "hash-q1",
                                    "queryJson": {"table": "t", "fields": ["t.a"]},
                                },
                                "visConfig": {
                                    "id": "vc-orig-1",
                                    "jsonHash": "hash-vc1",
                                    "visType": "basic",
                                    "chartType": "line",
                                },
                            },
                        }
                    ],
                },
                "metadata": {"layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 24, "h": 51}]}},
                "ephemeral": "1:AAA11111",
            },
            "exportVersion": "0.1",
        }

        temp_export = {
            "document": {"name": "__add_tiles_tmp__"},
            "dashboard": {
                "queryPresentationCollection": {
                    "id": "temp-coll",
                    "queryPresentationCollectionMemberships": [
                        {
                            "id": "tm1",
                            "queryPresentationCollectionId": "temp-coll",
                            "queryPresentation": {
                                "id": "tqp1",
                                "name": "New Tile",
                                "miniUuid": "BBB22222",
                                "queryIdentifierMapKey": "1",
                                "visConfigId": "vc-temp-1",
                                "queryId": "q-temp-1",
                                "query": {
                                    "id": "q-temp-1",
                                    "jsonHash": "hash-tq1",
                                    "queryJson": {"table": "t", "fields": ["t.b"]},
                                },
                                "visConfig": {
                                    "id": "vc-temp-1",
                                    "jsonHash": "hash-tvc1",
                                    "visType": "basic",
                                    "chartType": "bar",
                                },
                            },
                        }
                    ],
                },
                "metadata": {"layouts": {"lg": [{"i": "1", "x": 0, "y": 0, "w": 24, "h": 51}]}},
                "ephemeral": "1:BBB22222",
            },
            "exportVersion": "0.1",
        }

        svc.export_dashboard.side_effect = [orig_export, temp_export]
        svc.import_dashboard.return_value = ImportResponse(
            document_id="new-dash", name="Dashboard",
        )

        import_calls = []
        orig_import = svc.import_dashboard
        def capture_import(*args, **kwargs):
            import_calls.append(args[0] if args else kwargs.get("export_data"))
            return orig_import(*args, **kwargs)
        svc.import_dashboard = capture_import

        with patch.object(mcp_server, "_create_with_vis_configs", return_value=("temp-id", "__add_tiles_tmp__")):
            result = json.loads(mcp_server.add_tiles_to_dashboard(
                "orig-id",
                [{"name": "New Tile", "chart_type": "bar", "query": {"table": "t", "fields": ["t.b"]}}],
            ))

        assert result["status"] == "updated"

        imported = import_calls[0]
        memberships = (
            imported["dashboard"]["queryPresentationCollection"]
            ["queryPresentationCollectionMemberships"]
        )
        assert len(memberships) == 2

        # Original membership should keep its IDs (not touched)
        orig_qp = memberships[0]["queryPresentation"]
        assert orig_qp.get("visConfigId") == "vc-orig-1"
        assert orig_qp.get("queryId") == "q-orig-1"
        assert orig_qp["query"]["id"] == "q-orig-1"
        assert orig_qp["visConfig"]["id"] == "vc-orig-1"

        # Temp membership: fresh UUIDs, collectionId set, membership ID removed
        temp_qp = memberships[1]["queryPresentation"]
        assert "id" not in memberships[1]            # membership-level ID popped
        assert "queryPresentationCollectionId" not in memberships[1]  # removed from temp
        assert temp_qp["id"] != "tqp1"              # QP ID regenerated
        assert temp_qp["visConfigId"] != "vc-temp-1"  # fresh UUID
        assert temp_qp["queryId"] != "q-temp-1"      # fresh UUID
        # visConfig.id matches visConfigId (linked)
        assert temp_qp["visConfig"]["id"] == temp_qp["visConfigId"]
        # query.id matches queryId (linked)
        assert temp_qp["query"]["id"] == temp_qp["queryId"]
        # queryIdentifierMapKey removed (Omni assigns on import)
        assert "queryIdentifierMapKey" not in temp_qp

        # Data fields should still be present
        assert temp_qp["query"]["queryJson"]["fields"] == ["t.b"]
        assert temp_qp["visConfig"]["chartType"] == "bar"


# ---------------------------------------------------------------------------
# _create_with_vis_configs: skeleton cleanup on failure
# ---------------------------------------------------------------------------


class TestSkeletonCleanup:
    """_create_with_vis_configs cleans up skeleton on export/reimport failure."""

    @patch.object(mcp_server, "_get_doc_svc")
    def test_skeleton_deleted_on_export_failure(self, mock_get_doc_svc):
        from omni_dash.api.documents import DashboardResponse
        from omni_dash.exceptions import OmniDashError

        svc = MagicMock()
        mock_get_doc_svc.return_value = svc
        svc.create_dashboard.return_value = DashboardResponse(
            document_id="skel-123", name="Test",
        )
        svc.export_dashboard.side_effect = OmniDashError("Export failed")

        payload = {
            "name": "Test",
            "queryPresentations": [
                {
                    "name": "T1",
                    "query": {"table": "t", "fields": ["t.a"]},
                    "visConfig": {"visType": "basic", "chartType": "line"},
                }
            ],
            "modelId": "m1",
        }

        with pytest.raises(OmniDashError, match="Export failed"):
            mcp_server._create_with_vis_configs(payload, name="Test", folder_id=None)

        # Skeleton should have been deleted
        svc.delete_dashboard.assert_called_with("skel-123")

    @patch.object(mcp_server, "_get_doc_svc")
    def test_skeleton_deleted_on_reimport_failure(self, mock_get_doc_svc):
        from omni_dash.api.documents import DashboardResponse
        from omni_dash.exceptions import OmniDashError

        svc = MagicMock()
        mock_get_doc_svc.return_value = svc
        svc.create_dashboard.return_value = DashboardResponse(
            document_id="skel-456", name="Test",
        )
        svc.export_dashboard.return_value = {
            "document": {"name": "Test"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "T1",
                                "visConfig": {"visType": "basic", "chartType": "line"},
                                "query": {"queryJson": {}},
                            }
                        }
                    ]
                }
            },
            "exportVersion": "0.1",
        }
        svc.import_dashboard.side_effect = OmniDashError("Import failed")

        payload = {
            "name": "Test",
            "queryPresentations": [
                {
                    "name": "T1",
                    "query": {"table": "t", "fields": ["t.a"]},
                    "visConfig": {"visType": "basic", "chartType": "line"},
                }
            ],
            "modelId": "m1",
        }

        with pytest.raises(OmniDashError, match="Import failed"):
            mcp_server._create_with_vis_configs(payload, name="Test", folder_id=None)

        # Skeleton should have been deleted
        svc.delete_dashboard.assert_called_with("skel-456")


# ---------------------------------------------------------------------------
# _to_omni_filter: date normalization for tile-level filters
# ---------------------------------------------------------------------------


class TestTileFilterDateNormalization:
    """_to_omni_filter normalizes date values through _normalize_date_to_days."""

    def test_date_range_with_weeks(self):
        from omni_dash.dashboard.definition import FilterSpec
        from omni_dash.dashboard.serializer import _to_omni_filter

        f = FilterSpec(field="t.date", operator="date_range", value="12 complete weeks ago")
        result = _to_omni_filter(f)
        assert result["left_side"] == "84 days ago"
        assert result["right_side"] == "84 days"

    def test_past_with_months(self):
        from omni_dash.dashboard.definition import FilterSpec
        from omni_dash.dashboard.serializer import _to_omni_filter

        f = FilterSpec(field="t.date", operator="past", value="3 months ago")
        result = _to_omni_filter(f)
        assert result["left_side"] == "90 days ago"
        assert result["right_side"] == "90 days"

    def test_before_with_weeks(self):
        from omni_dash.dashboard.definition import FilterSpec
        from omni_dash.dashboard.serializer import _to_omni_filter

        f = FilterSpec(field="t.date", operator="before", value="2 weeks ago")
        result = _to_omni_filter(f)
        assert result["right_side"] == "14 days ago"


class TestEmptyIDGuards:
    """BUG-1: update_dashboard and move_dashboard must guard against empty IDs."""

    @patch("omni_dash.mcp.server._get_shared_model_id", return_value="m1")
    @patch("omni_dash.mcp.server._get_doc_svc")
    def test_update_dashboard_empty_id_preserves_original(self, mock_get_doc, mock_mid):
        mock_doc = MagicMock()
        mock_get_doc.return_value = mock_doc
        mock_doc.export_dashboard.return_value = {
            "document": {"name": "Old", "modelId": "m1"},
            "dashboard": {"queryPresentationCollection": {}},
            "workbookModel": {"id": "wm1"},
            "exportVersion": "0.1",
        }
        mock_doc.import_dashboard.return_value = MagicMock(
            document_id="", name="Old"
        )

        result = json.loads(mcp_server.update_dashboard(
            dashboard_id="orig-123", name="New Name"
        ))
        assert "error" in result
        assert "preserved" in result["error"].lower()
        mock_doc.delete_dashboard.assert_not_called()

    @patch("omni_dash.mcp.server._get_shared_model_id", return_value="m1")
    @patch("omni_dash.mcp.server._get_doc_svc")
    def test_move_dashboard_empty_id_preserves_original(self, mock_get_doc, mock_mid):
        mock_doc = MagicMock()
        mock_get_doc.return_value = mock_doc
        mock_doc.export_dashboard.return_value = {
            "document": {"name": "D", "modelId": "m1"},
            "dashboard": {},
            "workbookModel": {"id": "wm1"},
            "exportVersion": "0.1",
        }
        mock_doc.import_dashboard.return_value = MagicMock(
            document_id="", name="D"
        )

        result = json.loads(mcp_server.move_dashboard(
            dashboard_id="orig-456", target_folder_id="f1"
        ))
        assert "error" in result
        assert "preserved" in result["error"].lower()
        mock_doc.delete_dashboard.assert_not_called()


class TestUpdateDashboardFiltersMetadataOnly:
    """BUG-3: Metadata-only update_dashboard should inject filters."""

    @patch("omni_dash.mcp.server._get_shared_model_id", return_value="m1")
    @patch("omni_dash.mcp.server._get_doc_svc")
    def test_filters_injected_in_metadata_only_path(self, mock_get_doc, mock_mid):
        mock_doc = MagicMock()
        mock_get_doc.return_value = mock_doc
        mock_doc.export_dashboard.return_value = {
            "document": {"name": "D", "modelId": "m1"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [],
                },
            },
            "workbookModel": {"id": "wm1"},
            "exportVersion": "0.1",
        }
        mock_doc.import_dashboard.return_value = MagicMock(
            document_id="new-id", name="D"
        )

        mcp_server.update_dashboard(
            dashboard_id="orig-789",
            filters=[{"field": "t.date", "filter_type": "date_range", "default_value": "30 days ago"}],
        )

        call_args = mock_doc.import_dashboard.call_args[0][0]
        qpc = call_args["dashboard"]["queryPresentationCollection"]
        assert "filterConfig" in qpc
        assert len(qpc["filterConfig"]) == 1
        assert len(qpc["filterOrder"]) == 1


class TestImportFallback24ColGrid:
    """BUG-6: _create_via_import_fallback layout uses 24-col grid."""

    @patch("omni_dash.mcp.server._get_doc_svc")
    def test_half_width_tile_gets_12_col(self, mock_get_doc):
        mock_doc = MagicMock()
        mock_get_doc.return_value = mock_doc
        mock_doc.import_dashboard.return_value = MagicMock(
            document_id="new-id", name="Test"
        )

        payload = {
            "queryPresentations": [
                {
                    "name": "Half",
                    "query": {"table": "t", "fields": ["t.a"]},
                    "position": {"x": 0, "y": 0, "w": 6, "h": 6},
                },
            ],
            "modelId": "m1",
        }

        mcp_server._create_via_import_fallback(
            mock_doc, payload, name="Test", folder_id=None,
        )

        call_args = mock_doc.import_dashboard.call_args[0][0]
        layout = call_args["dashboard"]["metadata"]["layouts"]["lg"][0]
        assert layout["w"] == 12, "6-col SDK width should be 12 in 24-col grid"
        assert layout["x"] == 0


class TestDuplicateTileNamesVisConfig:
    """BUG-7: Duplicate tile names should each get their own vis config."""

    @patch("omni_dash.mcp.server._get_doc_svc")
    def test_duplicate_names_get_correct_vis_configs(self, mock_get_doc):
        mock_doc = MagicMock()
        mock_get_doc.return_value = mock_doc

        mock_doc.create_dashboard.return_value = MagicMock(document_id="sk1")
        mock_doc.export_dashboard.return_value = {
            "document": {"name": "Test", "modelId": "m1"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {"queryPresentation": {"name": "Revenue", "visConfig": {}, "query": {"queryJson": {}}}},
                        {"queryPresentation": {"name": "Revenue", "visConfig": {}, "query": {"queryJson": {}}}},
                    ],
                },
            },
            "workbookModel": {"id": "wm1"},
            "exportVersion": "0.1",
        }
        mock_doc.import_dashboard.return_value = MagicMock(
            document_id="new-id", name="Test"
        )

        payload = {
            "queryPresentations": [
                {
                    "name": "Revenue",
                    "query": {"table": "t", "fields": ["t.a"]},
                    "visConfig": {"visType": "omni-kpi", "chartType": "kpi"},
                },
                {
                    "name": "Revenue",
                    "query": {"table": "t", "fields": ["t.a", "t.b"]},
                    "visConfig": {"visType": "basic", "chartType": "line", "spec": {"version": 0}},
                },
            ],
            "modelId": "m1",
        }

        mcp_server._create_with_vis_configs(payload, name="Test", folder_id=None)

        call_args = mock_doc.import_dashboard.call_args[0][0]
        memberships = call_args["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ]
        vc0 = memberships[0]["queryPresentation"]["visConfig"]
        vc1 = memberships[1]["queryPresentation"]["visConfig"]

        assert vc0["visType"] == "omni-kpi"
        assert vc0["chartType"] == "kpi"
        assert vc1["visType"] == "basic"
        assert vc1["chartType"] == "line"


# ---------------------------------------------------------------------------
# update_tile: chart_type updates config.mark and series marks
# ---------------------------------------------------------------------------


class TestUpdateTileChartTypeMarkSync:
    """update_tile chart_type change must also update config.mark.type
    and series[].mark.type to prevent Omni from wiping the cartesian spec."""

    @patch.object(mcp_server, "_get_doc_svc")
    def test_chart_type_updates_mark_and_series(self, mock_doc_svc_fn):
        """Changing chart_type from line to bar should update mark.type and
        series[].mark.type in the cartesian config."""
        mock_svc = MagicMock()
        mock_doc_svc_fn.return_value = mock_svc

        # Build a realistic export with a line chart
        export_data = {
            "document": {"name": "Test", "folderId": "f1", "modelId": "m1"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "Visits",
                                "visConfig": {
                                    "visType": "basic",
                                    "chartType": "line",
                                    "config": {
                                        "configType": "cartesian",
                                        "mark": {"type": "line"},
                                        "series": [
                                            {
                                                "field": {"name": "t.visits"},
                                                "yAxis": "y",
                                                "mark": {"type": "line"},
                                            }
                                        ],
                                        "x": {"field": {"name": "t.date"}},
                                    },
                                },
                                "query": {
                                    "queryJson": {
                                        "table": "t",
                                        "fields": ["t.date", "t.visits"],
                                    }
                                },
                            }
                        }
                    ]
                }
            },
            "exportVersion": "0.1",
        }
        mock_svc.export_dashboard.return_value = export_data

        # Import returns a result
        import_result = MagicMock()
        import_result.document_id = "new-id"
        mock_svc.import_dashboard.return_value = import_result

        result = json.loads(
            mcp_server.update_tile(
                dashboard_id="old-id",
                tile_name="Visits",
                chart_type="bar",
            )
        )
        assert result["status"] == "updated"
        assert result["dashboard_id"] == "new-id"

        # Check the patched payload
        call_args = mock_svc.import_dashboard.call_args[0][0]
        qp = call_args["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]

        vc = qp["visConfig"]
        assert vc["chartType"] == "bar"
        assert vc["visType"] == "basic"

        # config.mark.type should be updated to "bar"
        config = vc["config"]
        assert config["mark"]["type"] == "bar"

        # series[0].mark.type should also be "bar"
        assert config["series"][0]["mark"]["type"] == "bar"

    @patch.object(mcp_server, "_get_doc_svc")
    def test_chart_type_area_maps_mark_correctly(self, mock_doc_svc_fn):
        """area chart type should set mark.type to 'area'."""
        mock_svc = MagicMock()
        mock_doc_svc_fn.return_value = mock_svc

        export_data = {
            "document": {"name": "Test", "folderId": "f1", "modelId": "m1"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "T1",
                                "visConfig": {
                                    "visType": "basic",
                                    "chartType": "line",
                                    "config": {
                                        "mark": {"type": "line"},
                                        "series": [
                                            {"mark": {"type": "line"}, "field": {"name": "t.x"}}
                                        ],
                                    },
                                },
                                "query": {"queryJson": {"table": "t", "fields": ["t.x"]}},
                            }
                        }
                    ]
                }
            },
            "exportVersion": "0.1",
        }
        mock_svc.export_dashboard.return_value = export_data

        import_result = MagicMock()
        import_result.document_id = "new-2"
        mock_svc.import_dashboard.return_value = import_result

        mcp_server.update_tile(
            dashboard_id="old-2", tile_name="T1", chart_type="area"
        )

        call_args = mock_svc.import_dashboard.call_args[0][0]
        qp = call_args["dashboard"]["queryPresentationCollection"][
            "queryPresentationCollectionMemberships"
        ][0]["queryPresentation"]
        config = qp["visConfig"]["config"]
        assert config["mark"]["type"] == "area"
        assert config["series"][0]["mark"]["type"] == "area"

    @patch.object(mcp_server, "_get_doc_svc")
    def test_no_config_does_not_crash(self, mock_doc_svc_fn):
        """If config is empty, chart_type change should still work."""
        mock_svc = MagicMock()
        mock_doc_svc_fn.return_value = mock_svc

        export_data = {
            "document": {"name": "Test", "folderId": "f1", "modelId": "m1"},
            "dashboard": {
                "queryPresentationCollection": {
                    "queryPresentationCollectionMemberships": [
                        {
                            "queryPresentation": {
                                "name": "T1",
                                "visConfig": {
                                    "visType": "basic",
                                    "chartType": "line",
                                    "config": {},
                                },
                                "query": {"queryJson": {"table": "t", "fields": ["t.x"]}},
                            }
                        }
                    ]
                }
            },
            "exportVersion": "0.1",
        }
        mock_svc.export_dashboard.return_value = export_data

        import_result = MagicMock()
        import_result.document_id = "new-3"
        mock_svc.import_dashboard.return_value = import_result

        result = json.loads(
            mcp_server.update_tile(
                dashboard_id="old-3", tile_name="T1", chart_type="scatter"
            )
        )
        assert result["status"] == "updated"


# ---------------------------------------------------------------------------
# get_dashboard: tiles key
# ---------------------------------------------------------------------------


class TestGetDashboardTilesKey:
    """get_dashboard should return a 'tiles' key with simplified tile info."""

    @patch.object(mcp_server, "_get_doc_svc")
    def test_tiles_key_present(self, mock_doc_svc_fn):
        from omni_dash.api.documents import DashboardResponse

        mock_svc = MagicMock()
        mock_doc_svc_fn.return_value = mock_svc
        mock_svc.get_dashboard.return_value = DashboardResponse(
            document_id="abc",
            name="Test",
            query_presentations=[
                {
                    "name": "My Tile",
                    "chartType": "bar",
                    "query": {
                        "table": "t",
                        "fields": ["t.a", "t.b"],
                    },
                }
            ],
        )
        result = json.loads(mcp_server.get_dashboard("abc"))
        assert "tiles" in result
        assert len(result["tiles"]) == 1
        tile = result["tiles"][0]
        assert tile["name"] == "My Tile"
        assert tile["chart_type"] == "bar"
        assert tile["fields"] == ["t.a", "t.b"]
        assert tile["table"] == "t"

    @patch.object(mcp_server, "_get_doc_svc")
    def test_sql_tile_flag(self, mock_doc_svc_fn):
        from omni_dash.api.documents import DashboardResponse

        mock_svc = MagicMock()
        mock_doc_svc_fn.return_value = mock_svc
        mock_svc.get_dashboard.return_value = DashboardResponse(
            document_id="abc",
            name="Test",
            query_presentations=[
                {"name": "SQL Tile", "chartType": "line", "isSql": True, "query": {}},
            ],
        )
        result = json.loads(mcp_server.get_dashboard("abc"))
        tile = result["tiles"][0]
        assert tile["is_sql"] is True
