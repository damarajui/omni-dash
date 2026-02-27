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
                    "lg": [{"i": 1, "x": 0, "y": 0, "w": 12, "h": 40}]
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
                    "lg": [{"i": 1, "x": 0, "y": 0, "w": 6, "h": 40}]
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
        }
        assert expected == names

    def test_tool_count(self):
        import asyncio

        async def check():
            return len(await mcp_server.mcp.list_tools())

        assert asyncio.run(check()) == 19


# ---------------------------------------------------------------------------
# Bug 4: profile_data fallback for non-topic views
# ---------------------------------------------------------------------------


class TestProfileDataFallback:
    """profile_data should fall back to query discovery for non-topic views."""

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_profile_data_topic_success(self, _mock_mid, mock_model_svc, mock_query_runner):
        from omni_dash.api.models import TopicDetail
        from omni_dash.api.queries import QueryResult

        mock_model_svc.get_topic.return_value = TopicDetail(
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
    def test_profile_data_non_topic_fallback(self, _mock_mid, mock_model_svc, mock_query_runner):
        from omni_dash.api.queries import QueryResult

        # get_topic raises — simulates non-topic Snowflake view
        mock_model_svc.get_topic.side_effect = Exception("Topic not found")
        # Both the discovery query and profiling query return the same result
        mock_query_runner.run.return_value = QueryResult(
            fields=["mart_seo.week_start", "mart_seo.visits"],
            rows=[
                {"mart_seo.week_start": "2026-01-01", "mart_seo.visits": 100},
            ],
            row_count=1,
        )

        result = json.loads(mcp_server.profile_data("mart_seo"))
        # Fallback should discover fields and profile them successfully
        assert "error" not in result
        assert "fields" in result
        assert "mart_seo.week_start" in result["fields"]
        assert "mart_seo.visits" in result["fields"]
        # query_runner.run called twice: once for discovery, once for profiling
        assert mock_query_runner.run.call_count == 2

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_profile_data_fallback_query_also_fails(self, _mock_mid, mock_model_svc, mock_query_runner):
        """When both get_topic and the fallback query fail, return a clear error."""
        # get_topic fails (non-topic view)
        mock_model_svc.get_topic.side_effect = Exception("Topic not found")
        # Fallback query also fails (e.g. view doesn't exist in Snowflake)
        mock_query_runner.run.side_effect = Exception("Query execution failed")

        result = json.loads(mcp_server.profile_data("nonexistent_view"))
        assert "error" in result
        assert "nonexistent_view" in result["error"]
        assert "not queryable" in result["error"]


# ---------------------------------------------------------------------------
# suggest_chart
# ---------------------------------------------------------------------------


class TestSuggestChart:
    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_date_and_measure_recommends_line(self, _, mock_model_svc):
        from omni_dash.api.models import TopicDetail

        mock_model_svc.get_topic.return_value = TopicDetail(
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

        mock_model_svc.get_topic.return_value = TopicDetail(
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
    def test_handles_topic_error(self, _, mock_model_svc):
        from omni_dash.exceptions import OmniAPIError

        mock_model_svc.get_topic.side_effect = OmniAPIError(404, "Topic not found")

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

        mock_model_svc.get_topic.return_value = TopicDetail(
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
    def test_invalid_chart_type_raises(self, _):
        """Tile with chart_type not in ChartType enum raises Pydantic ValidationError.

        validate_dashboard only catches OmniDashError, so Pydantic
        ValidationError from the Tile constructor propagates.
        """
        from pydantic import ValidationError

        tiles = [{
            "name": "Bad Chart",
            "chart_type": "nonexistent_type",
            "query": {"table": "t", "fields": ["t.x"]},
        }]
        with pytest.raises(ValidationError, match="chart_type"):
            mcp_server.validate_dashboard(tiles=tiles)

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_missing_fields_raises(self, _):
        """Tile with empty fields list raises Pydantic ValidationError.

        The TileQuery field_validator rejects empty fields lists before
        validate_definition runs, and this error is not caught.
        """
        from pydantic import ValidationError

        tiles = [{
            "name": "Empty Fields",
            "chart_type": "line",
            "query": {"table": "t", "fields": []},
        }]
        with pytest.raises(ValidationError, match="At least one field"):
            mcp_server.validate_dashboard(tiles=tiles)

    @patch.object(mcp_server, "_get_shared_model_id", return_value="model-123")
    def test_sort_field_not_in_query_warns(self, _, mock_model_svc):
        """Sort field not in query fields should produce a warning."""
        mock_model_svc.get_topic.side_effect = Exception("skip field check")

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
        mock_serializer.to_omni_create_payload.return_value = {"name": "SEO Trends"}

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
             patch("omni_dash.ai.service.DashboardAI", mock_ai_cls):
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
        mock_serializer.to_omni_create_payload.return_value = {"name": "D"}
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
             patch("omni_dash.ai.service.DashboardAI", mock_ai_cls):
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

        mock_model_svc.get_topic.return_value = TopicDetail(
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

        mock_model_svc.get_topic.return_value = TopicDetail(
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

        mock_model_svc.get_topic.return_value = TopicDetail(
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
                "lg": [{"i": 1, "x": 0, "y": 0, "w": 12, "h": 40}]
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
                "metadata": {"layouts": {"lg": [{"i": 1, "x": 0, "y": 0, "w": 12, "h": 40}]}},
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
                "metadata": {"layouts": {"lg": [{"i": 1, "x": 0, "y": 0, "w": 6, "h": 40}]}},
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

        model_svc.get_topic.side_effect = get_topic
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
