"""Omni model and topic introspection."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from omni_dash.api.client import OmniClient
from omni_dash.exceptions import ModelNotFoundError, OmniAPIError

logger = logging.getLogger(__name__)

CACHE_FILE = ".omni-dash-cache.json"


class OmniModel(BaseModel):
    """An Omni model (semantic layer instance)."""

    id: str
    name: str
    connection_id: str = ""
    database: str = ""
    schema_name: str = ""


class TopicSummary(BaseModel):
    """Brief topic info within a model."""

    name: str
    label: str = ""
    description: str = ""


class TopicDetail(BaseModel):
    """Full topic definition including views and fields."""

    name: str
    label: str = ""
    description: str = ""
    views: list[dict[str, Any]] = Field(default_factory=list)
    fields: list[dict[str, Any]] = Field(default_factory=list)


class ModelService:
    """Introspect Omni models, topics, and views.

    Provides methods to discover which Omni model corresponds to a
    given Snowflake database/schema, and to enumerate topics and
    fields within a model. Results are cached locally.
    """

    def __init__(self, client: OmniClient, cache_ttl: int = 3600):
        self._client = client
        self._cache_ttl = cache_ttl
        self._cache: dict[str, Any] = {}
        self._cache_ts: float = 0.0

    def list_models(self) -> list[OmniModel]:
        """List all models in the organization."""
        result = self._client.get("/api/v1/models")
        if not result:
            return []

        models_data = result if isinstance(result, list) else result.get("models", [])

        return [
            OmniModel(
                id=m.get("id", ""),
                name=m.get("name", ""),
                connection_id=m.get("connectionId", ""),
                database=m.get("database", ""),
                schema_name=m.get("schemaName", m.get("schema", "")),
            )
            for m in models_data
        ]

    def get_model(self, model_id: str) -> OmniModel:
        """Get a specific model by ID."""
        result = self._client.get(f"/api/v1/models/{model_id}")
        if not result or not isinstance(result, dict):
            raise ModelNotFoundError(model_id)

        return OmniModel(
            id=result.get("id", model_id),
            name=result.get("name", ""),
            connection_id=result.get("connectionId", ""),
            database=result.get("database", ""),
            schema_name=result.get("schemaName", result.get("schema", "")),
        )

    def find_model_for_connection(
        self, database: str, schema: str = ""
    ) -> OmniModel:
        """Find the Omni model connected to a specific database/schema.

        This is the key bridge between dbt and Omni: given the Snowflake
        database that dbt writes to, find the Omni model that reads from it.

        Args:
            database: Snowflake database name (e.g., "TRAINING_DATABASE").
            schema: Optional schema name filter.

        Returns:
            The matching OmniModel.

        Raises:
            ModelNotFoundError: If no model matches.
        """
        # Check cache first
        cache_key = f"model:{database}:{schema}"
        cached = self._get_cache(cache_key)
        if cached:
            return OmniModel(**cached)

        models = self.list_models()
        db_upper = database.upper()
        schema_upper = schema.upper() if schema else ""

        for model in models:
            model_db = (model.database or "").upper()
            model_schema = (model.schema_name or "").upper()

            if model_db == db_upper:
                if not schema_upper or model_schema == schema_upper:
                    self._set_cache(cache_key, model.model_dump())
                    return model

        raise ModelNotFoundError(
            f"database={database}, schema={schema}. "
            f"Available models: {[m.name for m in models]}"
        )

    def list_topics(self, model_id: str) -> list[TopicSummary]:
        """List all topics in a model."""
        result = self._client.get(f"/api/v1/models/{model_id}/topics")
        if not result:
            return []

        topics_data = result if isinstance(result, list) else result.get("topics", [])

        return [
            TopicSummary(
                name=t.get("name", ""),
                label=t.get("label", ""),
                description=t.get("description", ""),
            )
            for t in topics_data
        ]

    def get_topic(self, model_id: str, topic_name: str) -> TopicDetail:
        """Get full topic details including views and fields."""
        result = self._client.get(
            f"/api/v1/models/{model_id}/topics/{topic_name}"
        )
        if not result or not isinstance(result, dict):
            raise OmniAPIError(404, f"Topic not found: {topic_name}")

        return TopicDetail(
            name=result.get("name", topic_name),
            label=result.get("label", ""),
            description=result.get("description", ""),
            views=result.get("views", []),
            fields=result.get("fields", []),
        )

    def find_view_for_table(
        self, model_id: str, table_name: str
    ) -> str | None:
        """Find the Omni view name that corresponds to a dbt table name.

        Searches through all topics in the model to find a view whose
        name matches the given table (dbt model) name.

        Returns:
            The Omni view name, or None if not found.
        """
        cache_key = f"view:{model_id}:{table_name}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached.get("view_name")

        topics = self.list_topics(model_id)
        table_lower = table_name.lower()

        for topic in topics:
            try:
                detail = self.get_topic(model_id, topic.name)
                for view in detail.views:
                    view_name = view.get("name", "")
                    if view_name.lower() == table_lower:
                        self._set_cache(cache_key, {"view_name": view_name})
                        return view_name
            except OmniAPIError:
                continue

        return None

    # -- Cache helpers --

    def _get_cache(self, key: str) -> dict | None:
        """Get a value from the in-memory cache (with TTL check)."""
        if self._cache_ts and time.monotonic() - self._cache_ts > self._cache_ttl:
            self._cache.clear()
            self._cache_ts = 0.0
            return None
        return self._cache.get(key)

    def _set_cache(self, key: str, value: dict) -> None:
        """Set a value in the in-memory cache."""
        self._cache[key] = value
        if not self._cache_ts:
            self._cache_ts = time.monotonic()

    def save_cache(self, path: Path | None = None) -> None:
        """Persist cache to disk."""
        path = path or Path(CACHE_FILE)
        data = {
            "timestamp": time.time(),
            "ttl": self._cache_ttl,
            "entries": self._cache,
        }
        path.write_text(json.dumps(data, indent=2))

    def load_cache(self, path: Path | None = None) -> None:
        """Load cache from disk if it exists and is not expired."""
        path = path or Path(CACHE_FILE)
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text())
            ts = data.get("timestamp", 0)
            if time.time() - ts < self._cache_ttl:
                self._cache = data.get("entries", {})
                self._cache_ts = time.monotonic()
        except (json.JSONDecodeError, KeyError):
            logger.warning("Corrupt cache file, ignoring: %s", path)

    def clear_cache(self, path: Path | None = None) -> None:
        """Clear in-memory and on-disk cache."""
        self._cache.clear()
        self._cache_ts = 0.0
        path = path or Path(CACHE_FILE)
        if path.exists():
            path.unlink()
