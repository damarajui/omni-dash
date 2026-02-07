"""Parse dbt manifest.json for model metadata."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from omni_dash.exceptions import DbtMetadataError, DbtModelNotFoundError

logger = logging.getLogger(__name__)


class DbtColumnMetadata(BaseModel):
    """Column metadata from dbt manifest or schema.yml."""

    name: str
    description: str = ""
    data_type: str | None = None
    tests: list[str] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)


class DbtModelMetadata(BaseModel):
    """Comprehensive metadata for a single dbt model."""

    name: str
    unique_id: str = ""
    description: str = ""
    database: str = ""
    schema_name: str = ""
    materialization: str = ""
    columns: list[DbtColumnMetadata] = Field(default_factory=list)
    depends_on: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    path: str = ""
    raw_code: str = ""
    has_omni_grant: bool = False
    meta: dict[str, Any] = Field(default_factory=dict)
    layer: str = ""  # staging, intermediate, mart, etc.

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def qualified_name(self) -> str:
        """Database-qualified table name (e.g., TRAINING_DATABASE.PUBLIC.mart_seo_weekly_funnel)."""
        parts = [p for p in [self.database, self.schema_name, self.name] if p]
        return ".".join(parts)


class ManifestReader:
    """Parse dbt's target/manifest.json for model metadata.

    The manifest is dbt's compiled artifact containing resolved references,
    full column lists, materializations, and dependency graphs. It may be
    stale if `dbt compile` hasn't been run recently.
    """

    def __init__(self, project_path: str | Path):
        self._project_path = Path(project_path).expanduser()
        self._manifest: dict[str, Any] | None = None

    @property
    def manifest_path(self) -> Path:
        return self._project_path / "target" / "manifest.json"

    @property
    def manifest(self) -> dict[str, Any]:
        """Lazy-load and cache the manifest."""
        if self._manifest is None:
            if not self.manifest_path.exists():
                raise DbtMetadataError(
                    f"manifest.json not found at {self.manifest_path}. "
                    "Run `uv run dbt compile` to generate it."
                )

            # Check staleness
            mtime = os.path.getmtime(self.manifest_path)
            import time

            age_hours = (time.time() - mtime) / 3600
            if age_hours > 24:
                logger.warning(
                    "manifest.json is %.1f hours old. Consider running `uv run dbt compile`.",
                    age_hours,
                )

            with open(self.manifest_path) as f:
                self._manifest = json.load(f)

        return self._manifest

    def _infer_layer(self, path: str) -> str:
        """Infer the model layer from its file path."""
        path_lower = path.lower()
        for layer in ("staging", "intermediate", "mart", "archive", "ad_hoc", "report", "datasets"):
            if f"/{layer}/" in path_lower or path_lower.startswith(f"{layer}/"):
                return layer
        return "other"

    def _detect_omni_grant(self, raw_code: str) -> bool:
        """Check if the model SQL contains the OMNATA_SYNC_ENGINE grant post-hook."""
        return "OMNATA_SYNC_ENGINE" in raw_code.upper()

    def _parse_model_node(self, node: dict[str, Any]) -> DbtModelMetadata:
        """Parse a single model node from the manifest."""
        columns = []
        for col_name, col_data in node.get("columns", {}).items():
            tests = []
            # Extract test names from the manifest's test metadata
            for test in col_data.get("tests", []):
                if isinstance(test, str):
                    tests.append(test)
                elif isinstance(test, dict):
                    tests.extend(test.keys())
            columns.append(
                DbtColumnMetadata(
                    name=col_name,
                    description=col_data.get("description", ""),
                    data_type=col_data.get("data_type"),
                    tests=tests,
                    meta=col_data.get("meta", {}),
                )
            )

        raw_code = node.get("raw_code", node.get("raw_sql", ""))
        path = node.get("path", node.get("original_file_path", ""))

        config = node.get("config", {})
        materialization = config.get("materialized", "")

        # Check for post_hook containing OMNATA
        has_grant = self._detect_omni_grant(raw_code)
        if not has_grant:
            post_hooks = config.get("post-hook", config.get("post_hook", []))
            if isinstance(post_hooks, list):
                has_grant = any(
                    "OMNATA_SYNC_ENGINE" in str(h).upper() for h in post_hooks
                )
            elif isinstance(post_hooks, str):
                has_grant = "OMNATA_SYNC_ENGINE" in post_hooks.upper()

        depends_on = node.get("depends_on", {}).get("nodes", [])

        return DbtModelMetadata(
            name=node.get("name", ""),
            unique_id=node.get("unique_id", ""),
            description=node.get("description", ""),
            database=node.get("database", ""),
            schema_name=node.get("schema", ""),
            materialization=materialization,
            columns=columns,
            depends_on=depends_on,
            tags=node.get("tags", []),
            path=path,
            raw_code=raw_code,
            has_omni_grant=has_grant,
            meta=node.get("meta", {}),
            layer=self._infer_layer(path),
        )

    def get_model(self, name: str) -> DbtModelMetadata:
        """Get metadata for a specific model by name.

        Args:
            name: Model name (e.g., "mart_seo_weekly_funnel").

        Raises:
            DbtModelNotFoundError: If the model doesn't exist.
        """
        nodes = self.manifest.get("nodes", {})

        # Try exact unique_id match first
        for uid, node in nodes.items():
            if node.get("resource_type") == "model" and node.get("name") == name:
                return self._parse_model_node(node)

        available = self.list_model_names()
        raise DbtModelNotFoundError(name, available)

    def list_models(self, *, layer: str | None = None) -> list[DbtModelMetadata]:
        """List all models, optionally filtered by layer.

        Args:
            layer: Filter by layer name (e.g., "mart", "staging", "intermediate").
        """
        nodes = self.manifest.get("nodes", {})
        models = []

        for uid, node in nodes.items():
            if node.get("resource_type") != "model":
                continue

            model = self._parse_model_node(node)

            if layer and model.layer != layer:
                continue

            models.append(model)

        return sorted(models, key=lambda m: m.name)

    def list_model_names(self, *, layer: str | None = None) -> list[str]:
        """Get just model names (faster than full metadata)."""
        nodes = self.manifest.get("nodes", {})
        names = []

        for uid, node in nodes.items():
            if node.get("resource_type") != "model":
                continue
            if layer:
                path = node.get("path", "")
                if self._infer_layer(path) != layer:
                    continue
            names.append(node.get("name", ""))

        return sorted(names)

    def search_models(self, keyword: str) -> list[DbtModelMetadata]:
        """Fuzzy search models by name or description.

        Matches if keyword appears (case-insensitive) in the model name,
        description, or any column description.
        """
        keyword_lower = keyword.lower()
        results = []

        for model in self.list_models():
            if keyword_lower in model.name.lower():
                results.append(model)
                continue
            if keyword_lower in model.description.lower():
                results.append(model)
                continue
            if any(keyword_lower in c.description.lower() for c in model.columns):
                results.append(model)

        return results

    def get_model_columns(self, name: str) -> list[DbtColumnMetadata]:
        """Get column metadata for a model."""
        return self.get_model(name).columns

    def get_model_dependencies(self, name: str) -> list[str]:
        """Get upstream model dependencies."""
        model = self.get_model(name)
        return [
            dep.split(".")[-1]
            for dep in model.depends_on
            if dep.startswith("model.")
        ]
