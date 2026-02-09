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
        from omni_dash.api.documents import DashboardResponse

        mock_settings.return_value = MagicMock(omni_base_url="https://org.omniapp.co")
        svc = MagicMock()
        svc.create_dashboard.return_value = DashboardResponse(
            document_id="new123",
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
        assert result["dashboard_id"] == "new123"
        assert "url" in result

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
            TopicSummary(name="mart_seo", label="SEO", description="SEO data"),
            TopicSummary(name="mart_paid", label="Paid", description="Paid data"),
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
            "delete_dashboard",
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

        assert asyncio.run(check()) == 9
