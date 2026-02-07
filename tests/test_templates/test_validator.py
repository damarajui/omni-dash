"""Tests for omni_dash.templates.validator — cross-validation against dbt."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from omni_dash.dbt.manifest_reader import DbtColumnMetadata, DbtModelMetadata
from omni_dash.exceptions import TemplateValidationError
from omni_dash.templates.validator import TemplateValidator, ValidationResult


@pytest.fixture
def mock_registry():
    return MagicMock()


@pytest.fixture
def validator(mock_registry):
    return TemplateValidator(mock_registry)


def _model_with_cols(cols: list[str]) -> DbtModelMetadata:
    return DbtModelMetadata(
        name="test_model",
        description="A test model",
        columns=[DbtColumnMetadata(name=c, description=f"col {c}") for c in cols],
        has_omni_grant=True,
    )


class TestValidate:
    def test_missing_required_variable(self, validator):
        result = validator.validate(
            "t1",
            variables={},
            variable_specs={"dashboard_name": {"type": "string", "required": True}},
        )
        assert not result.valid
        assert any("dashboard_name" in e for e in result.errors)

    def test_required_with_default_ok(self, validator):
        result = validator.validate(
            "t1",
            variables={},
            variable_specs={"x": {"type": "string", "required": True, "default": "val"}},
        )
        assert result.valid

    def test_column_not_in_model(self, validator, mock_registry):
        mock_registry.get_model.return_value = _model_with_cols(["a", "b"])
        result = validator.validate(
            "t1",
            variables={"omni_table": "test_model", "time_column": "nonexistent"},
            variable_specs={},
        )
        assert not result.valid
        assert any("nonexistent" in e for e in result.errors)

    def test_metric_columns_validated(self, validator, mock_registry):
        mock_registry.get_model.return_value = _model_with_cols(["a", "b"])
        result = validator.validate(
            "t1",
            variables={"omni_table": "test_model", "metric_columns": ["a", "missing"]},
            variable_specs={},
        )
        assert not result.valid
        assert any("missing" in e for e in result.errors)

    def test_model_not_found_errors(self, validator, mock_registry):
        mock_registry.get_model.side_effect = Exception("not found")
        result = validator.validate(
            "t1",
            variables={"omni_table": "nope"},
            variable_specs={},
        )
        assert not result.valid

    def test_warnings_for_no_columns(self, validator, mock_registry):
        mock_registry.get_model.return_value = DbtModelMetadata(
            name="m", description="", columns=[], has_omni_grant=False,
        )
        result = validator.validate(
            "t1",
            variables={"omni_table": "m"},
            variable_specs={},
        )
        assert result.valid  # No errors, but warnings
        assert len(result.warnings) >= 1

    def test_valid_passes(self, validator, mock_registry):
        mock_registry.get_model.return_value = _model_with_cols(["week_start", "visits"])
        result = validator.validate(
            "t1",
            variables={"omni_table": "test_model", "time_column": "week_start", "metric_columns": ["visits"]},
            variable_specs={"dashboard_name": {"type": "string", "required": True, "default": "Test"}},
        )
        assert result.valid


class TestValidateOrRaise:
    def test_raises_on_errors(self, validator):
        with pytest.raises(TemplateValidationError, match="validation failed"):
            validator.validate_or_raise(
                "t1",
                variables={},
                variable_specs={"x": {"type": "string", "required": True}},
            )

    def test_passes_on_valid(self, validator):
        validator.validate_or_raise(
            "t1",
            variables={"x": "val"},
            variable_specs={"x": {"type": "string", "required": True}},
        )
