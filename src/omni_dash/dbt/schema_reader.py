"""Parse dbt schema.yml files directly for column documentation.

The schema.yml files are the source of truth for column descriptions
and are always current (unlike manifest.json which can be stale).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from omni_dash.dbt.manifest_reader import DbtColumnMetadata

logger = logging.getLogger(__name__)


class SchemaModelEntry(dict):
    """A model entry from a schema.yml file with convenience accessors."""

    @property
    def name(self) -> str:
        return self.get("name", "")

    @property
    def description(self) -> str:
        return self.get("description", "").strip()

    @property
    def columns(self) -> list[dict[str, Any]]:
        return self.get("columns", [])

    def get_column_metadata(self) -> list[DbtColumnMetadata]:
        """Parse columns into DbtColumnMetadata objects."""
        result = []
        for col in self.columns:
            tests = []
            for test in col.get("tests", []):
                if isinstance(test, str):
                    tests.append(test)
                elif isinstance(test, dict):
                    tests.extend(test.keys())

            result.append(
                DbtColumnMetadata(
                    name=col.get("name", ""),
                    description=col.get("description", "").strip(),
                    data_type=col.get("data_type"),
                    tests=tests,
                    meta=col.get("meta", {}),
                )
            )
        return result


class SchemaReader:
    """Read and parse schema.yml files from a dbt project.

    Scans all `schema.yml` and `_schema.yml` files under the models/
    directory, parsing model definitions with their column documentation.
    This complements the ManifestReader by providing always-current
    column descriptions.
    """

    def __init__(self, project_path: str | Path):
        self._project_path = Path(project_path).expanduser()
        self._models_dir = self._project_path / "models"
        self._cache: dict[str, SchemaModelEntry] | None = None
        self._schema_files: list[Path] | None = None

    def _find_schema_files(self) -> list[Path]:
        """Find all schema.yml files in the models directory."""
        if self._schema_files is not None:
            return self._schema_files

        if not self._models_dir.exists():
            self._schema_files = []
            return self._schema_files

        patterns = ["**/schema.yml", "**/_schema.yml", "**/*_schema.yml"]
        files: set[Path] = set()
        for pattern in patterns:
            files.update(self._models_dir.glob(pattern))

        self._schema_files = sorted(f for f in files if f.is_file())

        logger.debug("Found %d schema files", len(self._schema_files))
        return self._schema_files

    def _load_all(self) -> dict[str, SchemaModelEntry]:
        """Load and merge all schema files into a model name → entry map."""
        if self._cache is not None:
            return self._cache

        self._cache = {}
        for schema_file in self._find_schema_files():
            try:
                with open(schema_file) as f:
                    content = yaml.safe_load(f)

                if not content or not isinstance(content, dict):
                    continue

                models = content.get("models", [])
                if not isinstance(models, list):
                    continue

                for model_dict in models:
                    if not isinstance(model_dict, dict):
                        continue
                    name = model_dict.get("name", "")
                    if name:
                        entry = SchemaModelEntry(model_dict)
                        # If model already exists, merge column info
                        if name in self._cache:
                            existing = self._cache[name]
                            existing_cols = {
                                c.get("name"): c for c in existing.get("columns", [])
                            }
                            for col in model_dict.get("columns", []):
                                col_name = col.get("name", "")
                                if col_name and col_name not in existing_cols:
                                    existing.setdefault("columns", []).append(col)
                            # Prefer longer description
                            if len(model_dict.get("description", "")) > len(
                                existing.get("description", "")
                            ):
                                existing["description"] = model_dict["description"]
                        else:
                            self._cache[name] = entry

            except yaml.YAMLError as e:
                logger.warning("Failed to parse %s: %s", schema_file, e)
            except OSError as e:
                logger.warning("Failed to read %s: %s", schema_file, e)

        return self._cache

    def get_model_schema(self, name: str) -> SchemaModelEntry | None:
        """Get the schema.yml entry for a model by name.

        Returns None if the model is not documented in any schema.yml file.
        """
        return self._load_all().get(name)

    def list_documented_models(self) -> list[str]:
        """List all model names that have schema.yml entries."""
        return sorted(self._load_all().keys())

    def get_column_docs(self, model_name: str) -> list[DbtColumnMetadata]:
        """Get column documentation for a model from schema.yml.

        Returns an empty list if the model or columns are not documented.
        """
        entry = self.get_model_schema(model_name)
        if not entry:
            return []
        return entry.get_column_metadata()

    def get_all_column_docs(self) -> dict[str, list[DbtColumnMetadata]]:
        """Get column documentation for all documented models."""
        result = {}
        for name, entry in self._load_all().items():
            cols = entry.get_column_metadata()
            if cols:
                result[name] = cols
        return result

    def get_model_description(self, model_name: str) -> str:
        """Get the model-level description from schema.yml."""
        entry = self.get_model_schema(model_name)
        return entry.description if entry else ""

    def search_by_column(self, column_name: str) -> list[str]:
        """Find models that contain a specific column name."""
        column_lower = column_name.lower()
        results = []
        for name, entry in self._load_all().items():
            for col in entry.get("columns", []):
                if col.get("name", "").lower() == column_lower:
                    results.append(name)
                    break
        return sorted(results)

    def invalidate_cache(self) -> None:
        """Clear cached schema data (e.g., after schema.yml changes)."""
        self._cache = None
        self._schema_files = None
