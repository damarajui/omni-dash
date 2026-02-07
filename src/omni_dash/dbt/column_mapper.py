"""Map dbt column names to Omni field references.

The critical bridge between dbt model metadata and Omni query specifications.
Handles the naming convention differences and resolves Omni model/view IDs.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field

from omni_dash.api.models import ModelService
from omni_dash.dbt.model_registry import ModelRegistry
from omni_dash.exceptions import DbtModelNotFoundError, ModelNotFoundError

logger = logging.getLogger(__name__)


class OmniFieldMapping(BaseModel):
    """Complete mapping from a dbt model to Omni field references."""

    dbt_model_name: str
    omni_model_id: str
    omni_view_name: str
    field_map: dict[str, str] = Field(default_factory=dict)

    def omni_field(self, dbt_column: str) -> str:
        """Get the Omni field reference for a dbt column name."""
        return self.field_map.get(
            dbt_column, f"{self.omni_view_name}.{dbt_column}"
        )

    def omni_fields(self, dbt_columns: list[str]) -> list[str]:
        """Get Omni field references for multiple dbt columns."""
        return [self.omni_field(col) for col in dbt_columns]


class ColumnMapper:
    """Resolve dbt columns to Omni field references.

    This mapper:
    1. Looks up the dbt model in the ModelRegistry to get column names
    2. Finds the corresponding Omni model via the ModelService API
    3. Maps each dbt column to its qualified Omni field reference
       (format: "view_name.field_name")
    4. Caches results to avoid repeated API calls
    """

    def __init__(
        self,
        model_service: ModelService,
        registry: ModelRegistry,
    ):
        self._model_service = model_service
        self._registry = registry
        self._mapping_cache: dict[str, OmniFieldMapping] = {}

    def resolve_omni_references(
        self,
        dbt_model_name: str,
        *,
        omni_model_id: str | None = None,
    ) -> OmniFieldMapping:
        """Resolve all columns of a dbt model to Omni field references.

        Args:
            dbt_model_name: Name of the dbt model (e.g., "mart_seo_weekly_funnel").
            omni_model_id: Optional Omni model ID override (skips auto-discovery).

        Returns:
            OmniFieldMapping with the complete field map.

        Raises:
            DbtModelNotFoundError: If the dbt model doesn't exist.
            ModelNotFoundError: If no Omni model matches the dbt model's database.
        """
        # Check cache
        cache_key = f"{dbt_model_name}:{omni_model_id or 'auto'}"
        if cache_key in self._mapping_cache:
            return self._mapping_cache[cache_key]

        # Get dbt model metadata
        dbt_model = self._registry.get_model(dbt_model_name)

        # Find Omni model
        if omni_model_id:
            model_id = omni_model_id
        else:
            try:
                omni_model = self._model_service.find_model_for_connection(
                    database=dbt_model.database or "TRAINING_DATABASE",
                    schema=dbt_model.schema_name or "PUBLIC",
                )
                model_id = omni_model.id
            except ModelNotFoundError:
                # Fall back: try without schema filter
                omni_model = self._model_service.find_model_for_connection(
                    database=dbt_model.database or "TRAINING_DATABASE",
                )
                model_id = omni_model.id

        # Try to find the exact view name in Omni
        view_name = self._model_service.find_view_for_table(
            model_id, dbt_model_name
        )
        if not view_name:
            # Default: use the dbt model name as the view name
            view_name = dbt_model_name
            logger.info(
                "No exact view match for '%s' in Omni model %s, using dbt name as view name",
                dbt_model_name,
                model_id,
            )

        # Build field map: dbt_column -> "view_name.column_name"
        field_map: dict[str, str] = {}
        for col in dbt_model.columns:
            field_map[col.name] = f"{view_name}.{col.name}"

        mapping = OmniFieldMapping(
            dbt_model_name=dbt_model_name,
            omni_model_id=model_id,
            omni_view_name=view_name,
            field_map=field_map,
        )

        self._mapping_cache[cache_key] = mapping
        return mapping

    def map_column(
        self,
        dbt_model_name: str,
        dbt_column: str,
        *,
        omni_model_id: str | None = None,
    ) -> str:
        """Map a single dbt column to its Omni field reference.

        Returns a string like "mart_seo_weekly_funnel.week_start".
        """
        mapping = self.resolve_omni_references(
            dbt_model_name, omni_model_id=omni_model_id
        )
        return mapping.omni_field(dbt_column)

    def map_columns(
        self,
        dbt_model_name: str,
        dbt_columns: list[str],
        *,
        omni_model_id: str | None = None,
    ) -> list[str]:
        """Map multiple dbt columns to Omni field references."""
        mapping = self.resolve_omni_references(
            dbt_model_name, omni_model_id=omni_model_id
        )
        return mapping.omni_fields(dbt_columns)

    def clear_cache(self) -> None:
        """Clear the mapping cache."""
        self._mapping_cache.clear()
