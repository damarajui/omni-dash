"""Omni native AI API integration.

Provides access to Omni's AI endpoints:
- generate-query: Convert natural language to structured Omni queries
- pick-topic: AI-powered topic selection for a prompt
- jobs: Asynchronous AI analysis with full result streaming
"""

from __future__ import annotations

import logging
import time
from typing import Any

from pydantic import BaseModel, Field

from omni_dash.api.client import OmniClient
from omni_dash.exceptions import OmniAPIError

logger = logging.getLogger(__name__)


class GeneratedQuery(BaseModel):
    """Result from Omni's AI query generation."""

    table: str = ""
    fields: list[str] = Field(default_factory=list)
    sorts: list[dict[str, Any]] = Field(default_factory=list)
    filters: dict[str, Any] = Field(default_factory=dict)
    limit: int = 200
    pivots: list[str] = Field(default_factory=list)
    calculations: list[dict[str, Any]] = Field(default_factory=list)
    raw_response: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_omni_response(cls, data: dict[str, Any]) -> GeneratedQuery:
        """Parse Omni's generate-query response into a GeneratedQuery."""
        query = data.get("query", {})
        model_job = query.get("model_job", query)

        return cls(
            table=model_job.get("table", ""),
            fields=model_job.get("fields", []),
            sorts=model_job.get("sorts", []),
            filters=model_job.get("filters", {}),
            limit=model_job.get("limit", 200),
            pivots=model_job.get("pivots", []),
            calculations=model_job.get("calculations", []),
            raw_response=data,
        )


class AIJobStatus(BaseModel):
    """Status of an async Omni AI job."""

    job_id: str
    conversation_id: str = ""
    status: str = "QUEUED"
    progress: dict[str, Any] | float | None = None
    result_summary: str | None = None
    error: str | None = None
    omni_chat_url: str = ""


class AIJobResult(BaseModel):
    """Full result from a completed Omni AI job."""

    message: str = ""
    result_summary: str = ""
    topic: str = ""
    omni_chat_url: str = ""
    actions: list[dict[str, Any]] = Field(default_factory=list)
    raw_response: dict[str, Any] = Field(default_factory=dict)


class OmniAIService:
    """Client for Omni's native AI endpoints.

    Usage:
        client = OmniClient()
        ai = OmniAIService(client)

        # Generate a query from natural language
        query = ai.generate_query("model-id", "Show me revenue by month")
        print(query.table, query.fields)

        # Pick the best topic for a prompt
        topic = ai.pick_topic("model-id", "What are our top customers?")

        # Run an async AI analysis
        job = ai.create_job("model-id", "Analyze churn patterns")
        result = ai.wait_for_job(job.job_id)
        print(result.message)
    """

    def __init__(self, client: OmniClient):
        self._client = client

    def generate_query(
        self,
        model_id: str,
        prompt: str,
        *,
        topic_name: str | None = None,
        branch_id: str | None = None,
        context_query: dict[str, Any] | None = None,
        structured: bool = True,
    ) -> GeneratedQuery:
        """Convert natural language to a structured Omni query.

        Uses Omni's native AI with full semantic model context — knows
        about joins, measures, dimensions, and relationships.

        Args:
            model_id: Omni model UUID.
            prompt: Natural language query description.
            topic_name: Optional base topic to scope the query.
            branch_id: Optional model branch ID.
            context_query: Optional previous query for follow-up context.
            structured: Request structured output format.

        Returns:
            GeneratedQuery with table, fields, sorts, filters, etc.

        Raises:
            OmniAPIError: On API errors (403 if AI not enabled, 429 rate limit).
        """
        body: dict[str, Any] = {
            "modelId": model_id,
            "prompt": prompt,
        }
        if topic_name:
            body["currentTopicName"] = topic_name
        if branch_id:
            body["branchId"] = branch_id
        if context_query:
            body["contextQuery"] = context_query
        # Note: Omni's "structured" param is undocumented and expects a string,
        # not a boolean. Omit it — the default response format is sufficient.

        logger.info("Omni AI generate_query: prompt=%r, topic=%s", prompt[:80], topic_name)
        result = self._client.post("/api/v1/ai/generate-query", json=body)
        if not isinstance(result, dict):
            raise OmniAPIError(0, f"Unexpected response type: {type(result)}")

        return GeneratedQuery.from_omni_response(result)

    def pick_topic(
        self,
        model_id: str,
        prompt: str,
        *,
        topic_names: list[str] | None = None,
        branch_id: str | None = None,
    ) -> str:
        """Use Omni AI to pick the best topic for a prompt.

        Args:
            model_id: Omni model UUID.
            prompt: Natural language description.
            topic_names: Optional list to constrain selection.
            branch_id: Optional model branch ID.

        Returns:
            Topic name string.
        """
        body: dict[str, Any] = {
            "modelId": model_id,
            "prompt": prompt,
        }
        if topic_names:
            body["potentialTopicNames"] = topic_names
        if branch_id:
            body["branchId"] = branch_id

        result = self._client.post("/api/v1/ai/pick-topic", json=body)
        if not isinstance(result, dict):
            raise OmniAPIError(0, f"Unexpected response type: {type(result)}")

        return result.get("topicId", "")

    def create_job(
        self,
        model_id: str,
        prompt: str,
        *,
        conversation_id: str | None = None,
        topic_name: str | None = None,
        branch_id: str | None = None,
        webhook_url: str | None = None,
    ) -> AIJobStatus:
        """Create an async AI analysis job.

        Args:
            model_id: Omni model UUID.
            prompt: Natural language instruction.
            conversation_id: Continue an existing conversation.
            topic_name: Scope to a specific topic.
            branch_id: Optional model branch.
            webhook_url: URL to notify on completion.

        Returns:
            AIJobStatus with job_id and conversation_id.
        """
        body: dict[str, Any] = {
            "modelId": model_id,
            "prompt": prompt,
        }
        if conversation_id:
            body["conversationId"] = conversation_id
        if topic_name:
            body["topicName"] = topic_name
        if branch_id:
            body["branchId"] = branch_id
        if webhook_url:
            body["webhookUrl"] = webhook_url

        result = self._client.post("/api/v1/ai/jobs", json=body)
        if not isinstance(result, dict):
            raise OmniAPIError(0, f"Unexpected response type: {type(result)}")

        return AIJobStatus(
            job_id=result.get("jobId", ""),
            conversation_id=result.get("conversationId", ""),
            status="QUEUED",
            omni_chat_url=result.get("omniChatUrl", ""),
        )

    def get_job_status(self, job_id: str) -> AIJobStatus:
        """Poll the status of an AI job.

        Args:
            job_id: Job UUID from create_job.

        Returns:
            AIJobStatus with current state.
        """
        result = self._client.get(f"/api/v1/ai/jobs/{job_id}")
        if not isinstance(result, dict):
            raise OmniAPIError(0, f"Unexpected response type: {type(result)}")

        return AIJobStatus(
            job_id=job_id,
            status=result.get("state") or result.get("status", "UNKNOWN"),
            progress=result.get("progress"),
            result_summary=result.get("resultSummary"),
            error=result.get("error"),
            omni_chat_url=result.get("omniChatUrl", ""),
        )

    def get_job_result(self, job_id: str) -> AIJobResult:
        """Get full results from a completed AI job.

        Args:
            job_id: Job UUID from create_job.

        Returns:
            AIJobResult with message, actions, and data.
        """
        result = self._client.get(f"/api/v1/ai/jobs/{job_id}/result")
        if not isinstance(result, dict):
            raise OmniAPIError(0, f"Unexpected response type: {type(result)}")

        return AIJobResult(
            message=result.get("message", ""),
            result_summary=result.get("resultSummary", ""),
            topic=result.get("topic", ""),
            omni_chat_url=result.get("omniChatUrl", ""),
            actions=result.get("actions", []),
            raw_response=result,
        )

    def cancel_job(self, job_id: str) -> None:
        """Cancel a running AI job."""
        self._client.post(f"/api/v1/ai/jobs/{job_id}/cancel", json={})

    def wait_for_job(
        self,
        job_id: str,
        *,
        poll_interval: float = 3.0,
        timeout: float = 300.0,
    ) -> AIJobResult:
        """Wait for an AI job to complete and return its result.

        Args:
            job_id: Job UUID.
            poll_interval: Seconds between status checks.
            timeout: Max seconds to wait.

        Returns:
            AIJobResult on success.

        Raises:
            OmniAPIError: If job fails or times out.
        """
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            status = self.get_job_status(job_id)
            logger.debug("AI job %s: status=%s", job_id, status.status)

            if status.status == "COMPLETE":
                return self.get_job_result(job_id)
            if status.status in ("FAILED", "CANCELLED"):
                raise OmniAPIError(
                    0,
                    f"AI job {status.status}: {status.error or 'unknown error'}",
                )

            time.sleep(poll_interval)

        raise OmniAPIError(0, f"AI job {job_id} timed out after {timeout}s")
