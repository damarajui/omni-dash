"""Adapter bridging Omni ModelService to the ModelRegistry interface.

DashboardAI expects a ModelRegistry (dbt-based) for tool execution.
This adapter lets DashboardAI work against Omni's topic/field API
instead, enabling the generate_dashboard MCP tool.
"""

from __future__ import annotations

from typing import Any

from omni_dash.api.models import ModelService, TopicDetail, TopicSummary
from omni_dash.dbt.manifest_reader import DbtColumnMetadata, DbtModelMetadata


class OmniModelAdapter:
    """Adapts Omni's ModelService to the ModelRegistry interface.

    Implements the three methods that ToolExecutor uses:
    - list_models(layer=None)
    - get_model(name)
    - search_models(keyword) [via _all_model_names]
    """

    def __init__(self, model_svc: ModelService, model_id: str):
        self._model_svc = model_svc
        self._model_id = model_id
        self._topics_cache: list[TopicSummary] | None = None

    def _get_topics(self) -> list[TopicSummary]:
        if self._topics_cache is None:
            self._topics_cache = self._model_svc.list_topics(self._model_id)
        return self._topics_cache

    def list_models(self, *, layer: str | None = None) -> list[DbtModelMetadata]:
        """List Omni topics as DbtModelMetadata."""
        topics = self._get_topics()
        return [
            DbtModelMetadata(
                name=t.name,
                description=t.description or t.label or t.name,
                columns=[],
            )
            for t in topics
        ]

    def get_model(self, name: str) -> DbtModelMetadata:
        """Get topic detail as DbtModelMetadata with columns."""
        detail: TopicDetail = self._model_svc.get_topic_fields(self._model_id, name)
        columns = [
            DbtColumnMetadata(
                name=f.get("name", ""),
                description=f.get("description", ""),
                data_type=f.get("type"),
            )
            for f in detail.fields
        ]
        return DbtModelMetadata(
            name=detail.name,
            description=detail.description or detail.label or detail.name,
            columns=columns,
        )

    def search_models(self, keyword: str) -> list[DbtModelMetadata]:
        """Search topics by keyword."""
        keyword_lower = keyword.lower()
        matches = []
        for topic in self._get_topics():
            text = f"{topic.name} {topic.label} {topic.description}".lower()
            if keyword_lower in text:
                matches.append(
                    DbtModelMetadata(
                        name=topic.name,
                        description=topic.description or topic.label or topic.name,
                        columns=[],
                    )
                )
        return matches

    @property
    def manifest(self) -> OmniModelAdapter:
        """Compatibility shim — return self (ToolExecutor accesses registry.manifest)."""
        return self

    @property
    def schema(self) -> OmniModelAdapter:
        """Compatibility shim — return self."""
        return self

    def _all_model_names(self) -> list[str]:
        """Return all topic names (used by ToolExecutor for error messages)."""
        return [t.name for t in self._get_topics()]
