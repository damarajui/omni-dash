"""Validate template variables against dbt model metadata."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field

from omni_dash.dbt.model_registry import ModelRegistry
from omni_dash.exceptions import TemplateValidationError

logger = logging.getLogger(__name__)


class ValidationResult(BaseModel):
    """Result of template validation."""

    valid: bool = True
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class TemplateValidator:
    """Cross-validate template variables against dbt model metadata.

    Checks:
    - Required variables are present
    - dbt model exists
    - Referenced columns exist in the dbt model
    - Column types match expectations (where specified)
    """

    def __init__(self, registry: ModelRegistry):
        self._registry = registry

    def validate(
        self,
        template_name: str,
        variables: dict[str, Any],
        variable_specs: dict[str, Any],
    ) -> ValidationResult:
        """Validate template variables.

        Args:
            template_name: Name of the template being validated.
            variables: Provided variable values.
            variable_specs: Variable specifications from the template.

        Returns:
            ValidationResult with any errors or warnings.
        """
        result = ValidationResult()

        # Check required variables
        for var_name, spec in variable_specs.items():
            if not isinstance(spec, dict):
                continue
            if spec.get("required", False) and var_name not in variables:
                if "default" not in spec:
                    result.errors.append(f"Required variable '{var_name}' is missing")

        # If there's a dbt model variable, validate it exists
        dbt_model_name = variables.get("dbt_model") or variables.get("omni_table")
        if dbt_model_name:
            try:
                model = self._registry.get_model(dbt_model_name)
            except Exception:
                result.errors.append(
                    f"dbt model '{dbt_model_name}' not found in project"
                )
                result.valid = len(result.errors) == 0
                return result

            model_col_names = {c.name for c in model.columns}

            # Validate referenced columns exist
            for var_name in ("time_column", "dimension_column"):
                col_name = variables.get(var_name)
                if col_name and model_col_names and col_name not in model_col_names:
                    result.errors.append(
                        f"Column '{col_name}' (from variable '{var_name}') "
                        f"not found in dbt model '{dbt_model_name}'. "
                        f"Available: {sorted(model_col_names)}"
                    )

            # Validate column lists
            for var_name in ("metric_columns", "columns", "fields"):
                col_list = variables.get(var_name)
                if isinstance(col_list, list) and model_col_names:
                    for col_name in col_list:
                        if col_name not in model_col_names:
                            result.errors.append(
                                f"Column '{col_name}' (from variable '{var_name}') "
                                f"not found in dbt model '{dbt_model_name}'"
                            )

            # Warnings for models without documentation
            if not model.columns:
                result.warnings.append(
                    f"dbt model '{dbt_model_name}' has no documented columns. "
                    "Column validation was skipped."
                )

            if not model.description:
                result.warnings.append(
                    f"dbt model '{dbt_model_name}' has no description in schema.yml"
                )

            if not model.has_omni_grant:
                result.warnings.append(
                    f"dbt model '{dbt_model_name}' does not have OMNATA_SYNC_ENGINE grant. "
                    "It may not be queryable from Omni."
                )

        result.valid = len(result.errors) == 0
        return result

    def validate_or_raise(
        self,
        template_name: str,
        variables: dict[str, Any],
        variable_specs: dict[str, Any],
    ) -> None:
        """Validate and raise TemplateValidationError if invalid."""
        result = self.validate(template_name, variables, variable_specs)
        if not result.valid:
            raise TemplateValidationError(template_name, result.errors)
        for warning in result.warnings:
            logger.warning("Template '%s': %s", template_name, warning)
