"""Unified dbt model registry merging manifest and schema.yml sources."""

from __future__ import annotations

import logging
from pathlib import Path

from omni_dash.dbt.manifest_reader import DbtColumnMetadata, DbtModelMetadata, ManifestReader
from omni_dash.dbt.schema_reader import SchemaReader
from omni_dash.exceptions import DbtMetadataError, DbtModelNotFoundError

logger = logging.getLogger(__name__)


class ModelRegistry:
    """Unified registry of dbt models combining manifest and schema.yml sources.

    The manifest provides comprehensive metadata (materializations, dependencies,
    compiled SQL) while schema.yml files provide always-current column descriptions.
    This registry merges both, preferring schema.yml for descriptions and manifest
    for structural metadata.
    """

    def __init__(self, project_path: str | Path):
        self._project_path = Path(project_path).expanduser()
        self._manifest_reader: ManifestReader | None = None
        self._schema_reader: SchemaReader | None = None

    @property
    def manifest(self) -> ManifestReader:
        if self._manifest_reader is None:
            self._manifest_reader = ManifestReader(self._project_path)
        return self._manifest_reader

    @property
    def schema(self) -> SchemaReader:
        if self._schema_reader is None:
            self._schema_reader = SchemaReader(self._project_path)
        return self._schema_reader

    def _merge_columns(
        self,
        manifest_cols: list[DbtColumnMetadata],
        schema_cols: list[DbtColumnMetadata],
    ) -> list[DbtColumnMetadata]:
        """Merge column metadata from manifest and schema.yml.

        Schema.yml descriptions take priority (they're the source of truth).
        Manifest provides data_type and any columns not in schema.yml.
        """
        schema_map = {c.name: c for c in schema_cols}
        manifest_map = {c.name: c for c in manifest_cols}

        # Start with all schema columns (they have the best descriptions)
        merged: dict[str, DbtColumnMetadata] = {}

        for name, schema_col in schema_map.items():
            manifest_col = manifest_map.get(name)
            merged[name] = DbtColumnMetadata(
                name=name,
                description=schema_col.description or (manifest_col.description if manifest_col else ""),
                data_type=schema_col.data_type or (manifest_col.data_type if manifest_col else None),
                tests=schema_col.tests or (manifest_col.tests if manifest_col else []),
                meta={**(manifest_col.meta if manifest_col else {}), **schema_col.meta},
            )

        # Add manifest-only columns
        for name, manifest_col in manifest_map.items():
            if name not in merged:
                merged[name] = manifest_col

        return sorted(merged.values(), key=lambda c: c.name)

    def get_model(self, name: str) -> DbtModelMetadata:
        """Get comprehensive metadata for a model, merging both sources.

        Args:
            name: Model name (e.g., "mart_seo_weekly_funnel").

        Raises:
            DbtModelNotFoundError: If the model doesn't exist in either source.
        """
        manifest_model: DbtModelMetadata | None = None
        try:
            manifest_model = self.manifest.get_model(name)
        except DbtModelNotFoundError:
            pass
        except DbtMetadataError as e:
            logger.warning("Could not read manifest: %s", e)

        schema_cols = self.schema.get_column_docs(name)
        schema_desc = self.schema.get_model_description(name)

        if manifest_model is None and not schema_cols and not schema_desc:
            all_names = self._all_model_names()
            raise DbtModelNotFoundError(name, all_names)

        if manifest_model is None:
            # Schema-only model (manifest might be stale)
            return DbtModelMetadata(
                name=name,
                description=schema_desc,
                columns=schema_cols,
            )

        # Merge both sources
        merged_cols = self._merge_columns(manifest_model.columns, schema_cols)
        merged_desc = schema_desc if schema_desc else manifest_model.description

        return manifest_model.model_copy(
            update={
                "columns": merged_cols,
                "description": merged_desc,
            }
        )

    def list_models(self, *, layer: str | None = None) -> list[DbtModelMetadata]:
        """List all models with merged metadata."""
        try:
            manifest_models = self.manifest.list_models(layer=layer)
        except DbtMetadataError:
            logger.warning("Manifest unavailable, using schema.yml only")
            manifest_models = []

        # Merge schema descriptions into manifest models
        result = []
        seen_names: set[str] = set()

        for model in manifest_models:
            schema_cols = self.schema.get_column_docs(model.name)
            schema_desc = self.schema.get_model_description(model.name)

            if schema_cols or schema_desc:
                merged_cols = self._merge_columns(model.columns, schema_cols)
                model = model.model_copy(
                    update={
                        "columns": merged_cols,
                        "description": schema_desc if schema_desc else model.description,
                    }
                )
            result.append(model)
            seen_names.add(model.name)

        # Add schema-only models (not in manifest)
        for name in self.schema.list_documented_models():
            if name not in seen_names:
                schema_entry = self.schema.get_model_schema(name)
                if schema_entry:
                    result.append(
                        DbtModelMetadata(
                            name=name,
                            description=schema_entry.description,
                            columns=schema_entry.get_column_metadata(),
                        )
                    )

        return sorted(result, key=lambda m: m.name)

    def list_mart_models(self) -> list[DbtModelMetadata]:
        """List only models in the mart layer."""
        return self.list_models(layer="mart")

    def list_omni_eligible_models(self) -> list[DbtModelMetadata]:
        """List models with OMNATA_SYNC_ENGINE grants (Omni-ready models).

        These are the models that have post-hooks granting access to
        the OMNATA sync engine, making them queryable from Omni.
        """
        all_models = self.list_models()
        return [m for m in all_models if m.has_omni_grant]

    def suggest_dashboard_models(self) -> list[DbtModelMetadata]:
        """Suggest models that are good candidates for dashboarding.

        Criteria:
        - In the mart layer
        - Has documented columns (schema.yml)
        - Materialized as table or incremental (not views)
        - Bonus: has OMNATA_SYNC_ENGINE grant
        """
        marts = self.list_mart_models()
        candidates = []

        for model in marts:
            # Must have at least some column documentation
            documented_cols = [c for c in model.columns if c.description]
            if not documented_cols:
                continue

            # Prefer materialized tables (not ephemeral or views for dashboards)
            if model.materialization in ("ephemeral",):
                continue

            candidates.append(model)

        # Sort: Omni-eligible first, then by number of documented columns
        return sorted(
            candidates,
            key=lambda m: (not m.has_omni_grant, -len([c for c in m.columns if c.description])),
        )

    def search_models(self, keyword: str) -> list[DbtModelMetadata]:
        """Search models by keyword across names, descriptions, and columns."""
        keyword_lower = keyword.lower()
        results = []

        for model in self.list_models():
            if keyword_lower in model.name.lower():
                results.append(model)
                continue
            if keyword_lower in model.description.lower():
                results.append(model)
                continue
            if any(keyword_lower in c.name.lower() or keyword_lower in c.description.lower() for c in model.columns):
                results.append(model)

        return results

    def _all_model_names(self) -> list[str]:
        """Get all known model names from both sources."""
        names: set[str] = set()
        try:
            names.update(self.manifest.list_model_names())
        except DbtMetadataError:
            pass
        names.update(self.schema.list_documented_models())
        return sorted(names)
