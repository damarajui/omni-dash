"""Tests for omni_dash.api.ai — Omni native AI service."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from omni_dash.api.ai import (
    AIJobResult,
    AIJobStatus,
    GeneratedQuery,
    OmniAIService,
)
from omni_dash.exceptions import OmniAPIError


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def ai_svc(mock_client):
    return OmniAIService(mock_client)


class TestGeneratedQuery:
    def test_from_omni_response_full(self):
        data = {
            "query": {
                "model_job": {
                    "table": "orders",
                    "fields": ["orders.total", "orders.date"],
                    "sorts": [{"column_name": "orders.date", "sort_descending": True}],
                    "filters": {"orders.status": {"operator": "is", "value": "active"}},
                    "limit": 50,
                    "pivots": [],
                    "calculations": [{"name": "growth", "expression": "..."}],
                }
            }
        }
        q = GeneratedQuery.from_omni_response(data)
        assert q.table == "orders"
        assert q.fields == ["orders.total", "orders.date"]
        assert len(q.sorts) == 1
        assert q.limit == 50
        assert len(q.calculations) == 1

    def test_from_omni_response_minimal(self):
        data = {"query": {"model_job": {"table": "t", "fields": ["t.a"]}}}
        q = GeneratedQuery.from_omni_response(data)
        assert q.table == "t"
        assert q.fields == ["t.a"]
        assert q.limit == 200  # default

    def test_from_omni_response_empty(self):
        q = GeneratedQuery.from_omni_response({})
        assert q.table == ""
        assert q.fields == []

    def test_from_omni_response_flat_query(self):
        """Some responses have query fields directly, not nested in model_job."""
        data = {"query": {"table": "t", "fields": ["t.x"]}}
        q = GeneratedQuery.from_omni_response(data)
        assert q.table == "t"
        assert q.fields == ["t.x"]


class TestOmniAIServiceGenerateQuery:
    def test_basic_generate(self, ai_svc, mock_client):
        mock_client.post.return_value = {
            "query": {
                "model_job": {
                    "table": "revenue",
                    "fields": ["revenue.month", "revenue.total"],
                    "limit": 100,
                }
            }
        }
        result = ai_svc.generate_query("model-1", "Show me revenue by month")
        assert result.table == "revenue"
        assert result.fields == ["revenue.month", "revenue.total"]
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        body = call_args.kwargs.get("json") or call_args[1].get("json")
        assert body["modelId"] == "model-1"
        assert body["prompt"] == "Show me revenue by month"
        assert body["structured"] is True

    def test_generate_with_topic(self, ai_svc, mock_client):
        mock_client.post.return_value = {
            "query": {"model_job": {"table": "orders", "fields": ["orders.id"]}}
        }
        ai_svc.generate_query("m1", "top orders", topic_name="orders")
        body = mock_client.post.call_args.kwargs.get("json") or mock_client.post.call_args[1].get("json")
        assert body["currentTopicName"] == "orders"

    def test_generate_api_error(self, ai_svc, mock_client):
        mock_client.post.side_effect = OmniAPIError(403, "AI not enabled")
        with pytest.raises(OmniAPIError, match="AI not enabled"):
            ai_svc.generate_query("m1", "test")

    def test_generate_unexpected_response_type(self, ai_svc, mock_client):
        mock_client.post.return_value = []
        with pytest.raises(OmniAPIError, match="Unexpected response type"):
            ai_svc.generate_query("m1", "test")


class TestOmniAIServicePickTopic:
    def test_basic_pick(self, ai_svc, mock_client):
        mock_client.post.return_value = {"topicId": "customers"}
        result = ai_svc.pick_topic("m1", "Who are our biggest customers?")
        assert result == "customers"

    def test_pick_with_constraints(self, ai_svc, mock_client):
        mock_client.post.return_value = {"topicId": "orders"}
        ai_svc.pick_topic("m1", "test", topic_names=["orders", "customers"])
        body = mock_client.post.call_args.kwargs.get("json") or mock_client.post.call_args[1].get("json")
        assert body["potentialTopicNames"] == ["orders", "customers"]

    def test_pick_empty_response(self, ai_svc, mock_client):
        mock_client.post.return_value = {}
        result = ai_svc.pick_topic("m1", "test")
        assert result == ""


class TestOmniAIServiceJobs:
    def test_create_job(self, ai_svc, mock_client):
        mock_client.post.return_value = {
            "jobId": "job-123",
            "conversationId": "conv-456",
            "omniChatUrl": "https://omni.co/chat/123",
        }
        status = ai_svc.create_job("m1", "Analyze churn")
        assert status.job_id == "job-123"
        assert status.conversation_id == "conv-456"
        assert status.status == "QUEUED"

    def test_get_job_status_complete(self, ai_svc, mock_client):
        mock_client.get.return_value = {
            "status": "COMPLETE",
            "resultSummary": "Analysis complete",
        }
        status = ai_svc.get_job_status("job-1")
        assert status.status == "COMPLETE"
        assert status.result_summary == "Analysis complete"

    def test_get_job_status_failed(self, ai_svc, mock_client):
        mock_client.get.return_value = {
            "status": "FAILED",
            "error": "Model not found",
        }
        status = ai_svc.get_job_status("job-1")
        assert status.status == "FAILED"
        assert status.error == "Model not found"

    def test_get_job_result(self, ai_svc, mock_client):
        mock_client.get.return_value = {
            "message": "Here is the analysis...",
            "resultSummary": "Churn is at 5%",
            "topic": "customers",
            "omniChatUrl": "https://omni.co/chat/1",
            "actions": [{"type": "query", "message": "Ran query"}],
        }
        result = ai_svc.get_job_result("job-1")
        assert result.message == "Here is the analysis..."
        assert result.topic == "customers"
        assert len(result.actions) == 1

    def test_cancel_job(self, ai_svc, mock_client):
        ai_svc.cancel_job("job-1")
        mock_client.post.assert_called_once_with(
            "/api/v1/ai/jobs/job-1/cancel", json={}
        )

    def test_wait_for_job_immediate_complete(self, ai_svc, mock_client):
        mock_client.get.side_effect = [
            {"status": "COMPLETE"},  # get_job_status
            {  # get_job_result
                "message": "Done",
                "resultSummary": "All good",
                "topic": "t",
                "actions": [],
            },
        ]
        result = ai_svc.wait_for_job("job-1")
        assert result.message == "Done"

    def test_wait_for_job_polls_then_completes(self, ai_svc, mock_client):
        mock_client.get.side_effect = [
            {"status": "EXECUTING", "progress": 0.5},  # poll 1
            {"status": "COMPLETE"},  # poll 2
            {"message": "Result", "resultSummary": "ok", "topic": "", "actions": []},
        ]
        with patch("omni_dash.api.ai.time") as mock_time:
            mock_time.monotonic.side_effect = [0, 1, 2, 3]
            mock_time.sleep = MagicMock()
            result = ai_svc.wait_for_job("job-1", poll_interval=1.0)
        assert result.result_summary == "ok"

    def test_wait_for_job_failed_raises(self, ai_svc, mock_client):
        mock_client.get.return_value = {"status": "FAILED", "error": "bad query"}
        with patch("omni_dash.api.ai.time") as mock_time:
            mock_time.monotonic.side_effect = [0, 1]
            with pytest.raises(OmniAPIError, match="FAILED"):
                ai_svc.wait_for_job("job-1")

    def test_wait_for_job_timeout(self, ai_svc, mock_client):
        mock_client.get.return_value = {"status": "EXECUTING"}
        with patch("omni_dash.api.ai.time") as mock_time:
            mock_time.monotonic.side_effect = [0, 999]  # immediately past deadline
            mock_time.sleep = MagicMock()
            with pytest.raises(OmniAPIError, match="timed out"):
                ai_svc.wait_for_job("job-1", timeout=10.0)
