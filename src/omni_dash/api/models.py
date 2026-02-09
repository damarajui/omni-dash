"""Omni model and topic introspection.

Uses the model YAML endpoint (/api/v1/models/{id}/yaml) to discover
topics, views, and fields. This replaces the non-existent /topics API.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from omni_dash.api.client import OmniClient
from omni_dash.api.documents import _extract_records
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
    model_kind: str = ""
    base_model_id: str = ""


class TopicSummary(BaseModel):
    """Brief topic info within a model."""

    name: str
    label: str = ""
    description: str = ""
    base_view: str = ""


class TopicDetail(BaseModel):
    """Full topic definition including views and fields."""

    name: str
    label: str = ""
    description: str = ""
    base_view: str = ""
    views: list[dict[str, Any]] = Field(default_factory=list)
    fields: list[dict[str, Any]] = Field(default_factory=list)


class ModelService:
    """Introspect Omni models, topics, and views.

    Provides methods to discover which Omni model corresponds to a
    given Snowflake database/schema, and to enumerate topics and
    fields within a model.

    Topic and field discovery uses the model YAML endpoint which
    returns all .topic and .view file definitions.
    """

    def __init__(self, client: OmniClient, cache_ttl: int = 3600):
        self._client = client
        self._cache_ttl = cache_ttl
        self._cache: dict[str, Any] = {}
        self._cache_ts: float = 0.0

    def list_models(self) -> list[OmniModel]:
        """List all models in the organization.

        Handles the paginated ``{pageInfo, records}`` response format.
        """
        params: dict[str, str] = {"pageSize": "100"}
        all_models: list[OmniModel] = []
        while True:
            result = self._client.get("/api/v1/models", params=params)
            if not result:
                break

            models_data = _extract_records(result)
            for m in models_data:
                all_models.append(
                    OmniModel(
                        id=m.get("id", ""),
                        name=m.get("name") or "",
                        connection_id=m.get("connectionId", ""),
                        database=m.get("database") or "",
                        schema_name=m.get("schemaName") or m.get("schema") or "",
                        model_kind=m.get("modelKind") or "",
                        base_model_id=m.get("baseModelId") or "",
                    )
                )

            page_info = result.get("pageInfo", {}) if isinstance(result, dict) else {}
            if page_info.get("hasNextPage") and page_info.get("nextCursor"):
                params["cursor"] = page_info["nextCursor"]
            else:
                break

        return all_models

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

    # -- YAML-based topic/view discovery --

    def _fetch_model_yaml(self, model_id: str) -> dict[str, Any]:
        """Fetch the model YAML definition, with caching.

        Returns the full response: {files: {name: content}, viewNames: {file: name}, version: int}
        """
        cache_key = f"yaml:{model_id}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        result = self._client.get(f"/api/v1/models/{model_id}/yaml")
        if not result or not isinstance(result, dict):
            raise OmniAPIError(0, f"Failed to fetch model YAML for {model_id}")

        self._set_cache(cache_key, result)
        return result

    def _parse_yaml_content(self, content: str) -> dict[str, Any]:
        """Parse a YAML string into a dict, handling empty/invalid content."""
        if not content or not content.strip():
            return {}
        try:
            parsed = yaml.safe_load(content)
            return parsed if isinstance(parsed, dict) else {}
        except yaml.YAMLError:
            return {}

    def list_topics(self, model_id: str) -> list[TopicSummary]:
        """List all topics in a model by parsing .topic files from model YAML."""
        model_yaml = self._fetch_model_yaml(model_id)
        files = model_yaml.get("files", {})

        topics: list[TopicSummary] = []
        for filename, content in files.items():
            if not filename.endswith(".topic"):
                continue

            topic_name = filename.removesuffix(".topic")
            parsed = self._parse_yaml_content(content)

            topics.append(
                TopicSummary(
                    name=topic_name,
                    label=parsed.get("label", ""),
                    description=parsed.get("description", ""),
                    base_view=parsed.get("base_view", ""),
                )
            )

        return sorted(topics, key=lambda t: t.name)

    def get_topic(self, model_id: str, topic_name: str) -> TopicDetail:
        """Get full topic details including resolved views and fields.

        Parses the topic's .topic file to find its base_view and joins,
        then resolves each view's .view file to extract dimensions and measures.
        """
        model_yaml = self._fetch_model_yaml(model_id)
        files = model_yaml.get("files", {})

        # Find the topic file
        topic_file = f"{topic_name}.topic"
        if topic_file not in files:
            available = [f.removesuffix(".topic") for f in files if f.endswith(".topic")]
            raise OmniAPIError(
                404,
                f"Topic not found: {topic_name}. "
                f"Available topics: {available}",
            )

        parsed = self._parse_yaml_content(files[topic_file])
        base_view = parsed.get("base_view", "")
        joins = parsed.get("joins", {}) or {}
        topic_label = parsed.get("label", "")
        topic_desc = parsed.get("description", "")

        # Collect all view names: base_view + joined views
        view_names = []
        if base_view:
            view_names.append(base_view)
        for join_name in joins:
            if join_name and isinstance(join_name, str):
                view_names.append(join_name)

        # Resolve each view's fields from .view files
        views_info: list[dict[str, Any]] = []
        all_fields: list[dict[str, Any]] = []

        for view_name in view_names:
            view_data = self._find_view_file(files, view_name)
            if not view_data:
                views_info.append({"name": view_name, "fields": []})
                continue

            dimensions = view_data.get("dimensions", {}) or {}
            measures = view_data.get("measures", {}) or {}
            view_schema = view_data.get("schema", "")
            table_name = view_data.get("table_name", "")

            view_fields: list[dict[str, Any]] = []

            for dim_name, dim_def in dimensions.items():
                if isinstance(dim_def, dict) and dim_def.get("hidden"):
                    continue
                field_info: dict[str, Any] = {
                    "name": dim_name,
                    "qualified_name": f"{topic_name}.{dim_name}",
                    "view": view_name,
                    "type": "dimension",
                }
                if isinstance(dim_def, dict):
                    field_info["label"] = dim_def.get("label", "")
                    field_info["format"] = dim_def.get("format", "")
                    field_info["sql"] = dim_def.get("sql", "")
                view_fields.append(field_info)

            for measure_name, measure_def in measures.items():
                if isinstance(measure_def, dict) and measure_def.get("hidden"):
                    continue
                field_info = {
                    "name": measure_name,
                    "qualified_name": f"{topic_name}.{measure_name}",
                    "view": view_name,
                    "type": "measure",
                }
                if isinstance(measure_def, dict):
                    field_info["label"] = measure_def.get("label", "")
                    field_info["aggregate_type"] = measure_def.get("aggregate_type", "")
                    field_info["sql"] = measure_def.get("sql", "")
                view_fields.append(field_info)

            views_info.append({
                "name": view_name,
                "schema": view_schema,
                "table_name": table_name,
                "field_count": len(view_fields),
            })
            all_fields.extend(view_fields)

        return TopicDetail(
            name=topic_name,
            label=topic_label,
            description=topic_desc,
            base_view=base_view,
            views=views_info,
            fields=all_fields,
        )

    def _find_view_file(
        self, files: dict[str, str], view_name: str
    ) -> dict[str, Any]:
        """Find and parse a .view file by view name.

        View files may be at root level (e.g., "view_name.view") or
        under a schema prefix (e.g., "PUBLIC/view_name.view").
        """
        # Try exact match first
        candidates = [
            f"{view_name}.view",
            f"{view_name}.query.view",
        ]
        # Also try with schema prefixes
        for filename in files:
            if filename.endswith(".view"):
                # Extract view name from path like "PUBLIC/mart_foo.view"
                bare = filename.rsplit("/", 1)[-1].removesuffix(".view").removesuffix(".query")
                if bare == view_name:
                    candidates.insert(0, filename)

        for candidate in candidates:
            if candidate in files:
                return self._parse_yaml_content(files[candidate])

        return {}

    def list_views(self, model_id: str) -> list[dict[str, str]]:
        """List all views (queryable tables) in a model.

        Returns view names from the model YAML viewNames mapping.
        """
        model_yaml = self._fetch_model_yaml(model_id)
        view_names = model_yaml.get("viewNames", {})

        views = []
        for file_path, view_name in view_names.items():
            schema = ""
            if "/" in file_path:
                schema = file_path.split("/")[0]
            views.append({
                "name": view_name,
                "file": file_path,
                "schema": schema,
            })
        return sorted(views, key=lambda v: v["name"])

    def find_view_for_table(
        self, model_id: str, table_name: str
    ) -> str | None:
        """Find the Omni view name that corresponds to a dbt table name.

        Searches through viewNames in the model YAML to find a view whose
        name matches the given table (dbt model) name.

        Returns:
            The Omni view name, or None if not found.
        """
        cache_key = f"view:{model_id}:{table_name}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached.get("view_name")

        model_yaml = self._fetch_model_yaml(model_id)
        view_names = model_yaml.get("viewNames", {})
        table_lower = table_name.lower()

        for _file_path, view_name in view_names.items():
            if view_name.lower() == table_lower:
                self._set_cache(cache_key, {"view_name": view_name})
                return view_name

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
