"""Pre-flight validation for dashboard definitions.

Checks that fields exist, chart types are valid, axis fields are in
query fields, and formats are known Omni codes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from omni_dash.dashboard.definition import ChartType, DashboardDefinition, Tile

# Known Omni format codes
KNOWN_FORMATS = frozenset({
    "BIGNUMBER_0", "BIGNUMBER_1", "BIGNUMBER_2",
    "USDCURRENCY_0", "USDCURRENCY_1", "USDCURRENCY_2",
    "PERCENT_0", "PERCENT_1", "PERCENT_2",
    "NUMBER_0", "NUMBER_1", "NUMBER_2",
    "DECIMAL_0", "DECIMAL_1", "DECIMAL_2",
})


@dataclass
class ValidationResult:
    """Result of dashboard validation."""

    valid: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)
        self.valid = False

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)


def validate_definition(
    definition: DashboardDefinition,
    available_fields: dict[str, set[str]] | None = None,
) -> ValidationResult:
    """Validate a DashboardDefinition before sending to Omni.

    Args:
        definition: The dashboard to validate.
        available_fields: Optional mapping of table → set of known field names.
            If provided, field existence is checked.

    Returns:
        ValidationResult with errors and warnings.
    """
    result = ValidationResult()

    if not definition.name:
        result.add_error("Dashboard name is required.")

    if not definition.model_id:
        result.add_error("model_id is required for Omni API.")

    if not definition.tiles and not definition.text_tiles:
        result.add_error("Dashboard must have at least one tile.")

    for tile in definition.tiles:
        _validate_tile(tile, result, available_fields)

    return result


def _validate_tile(
    tile: Tile,
    result: ValidationResult,
    available_fields: dict[str, set[str]] | None,
) -> None:
    """Validate a single tile."""
    prefix = f"Tile '{tile.name}'"

    # Chart type
    valid_types = {ct.value for ct in ChartType}
    if tile.chart_type not in valid_types:
        result.add_error(f"{prefix}: Invalid chart_type '{tile.chart_type}'.")

    # Query fields
    if not tile.query.fields:
        result.add_error(f"{prefix}: Query must have at least one field.")

    # Table
    if not tile.query.table:
        result.add_error(f"{prefix}: Query table is required.")

    # Field existence check
    if available_fields and tile.query.table:
        known = available_fields.get(tile.query.table, set())
        if known:
            for field_name in tile.query.fields:
                if field_name not in known:
                    result.add_error(f"{prefix}: Field '{field_name}' not found in topic '{tile.query.table}'.")

    # x_axis in fields
    vc = tile.vis_config
    if vc.x_axis and vc.x_axis not in tile.query.fields:
        result.add_warning(f"{prefix}: x_axis '{vc.x_axis}' is not in query fields.")

    # y_axis fields in query
    for y_field in vc.y_axis:
        if y_field not in tile.query.fields:
            result.add_warning(f"{prefix}: y_axis field '{y_field}' is not in query fields.")

    # Sort fields in query
    for sort in tile.query.sorts:
        if sort.column_name not in tile.query.fields:
            result.add_warning(f"{prefix}: Sort field '{sort.column_name}' is not in query fields (will be auto-added).")

    # KPI tiles should have limit=1
    if tile.chart_type == "number" and tile.query.limit > 1:
        result.add_warning(f"{prefix}: KPI tiles should have limit=1 (currently {tile.query.limit}).")

    # Format validation
    if vc.value_format and vc.value_format not in KNOWN_FORMATS:
        result.add_warning(f"{prefix}: value_format '{vc.value_format}' is not a known Omni format code.")
    if vc.y_axis_format and vc.y_axis_format not in KNOWN_FORMATS:
        result.add_warning(f"{prefix}: y_axis_format '{vc.y_axis_format}' is not a known Omni format code.")
