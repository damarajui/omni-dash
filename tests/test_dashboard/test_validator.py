"""Tests for the dashboard validator."""

from omni_dash.dashboard.definition import (
    DashboardDefinition,
    SortSpec,
    Tile,
    TileQuery,
    TileVisConfig,
)
from omni_dash.dashboard.validator import validate_definition


def _make_def(**overrides) -> DashboardDefinition:
    defaults = {
        "name": "Test",
        "model_id": "m-1",
        "tiles": [
            Tile(
                name="Chart",
                chart_type="line",
                query=TileQuery(table="t", fields=["t.date", "t.v"]),
                vis_config=TileVisConfig(x_axis="t.date"),
            ),
        ],
    }
    defaults.update(overrides)
    return DashboardDefinition(**defaults)


class TestValidateDefinition:
    def test_valid_definition_passes(self):
        result = validate_definition(_make_def())
        assert result.valid is True
        assert result.errors == []

    def test_missing_name(self):
        result = validate_definition(_make_def(name=""))
        assert result.valid is False
        assert any("name" in e.lower() for e in result.errors)

    def test_missing_model_id(self):
        result = validate_definition(_make_def(model_id=""))
        assert result.valid is False
        assert any("model_id" in e.lower() for e in result.errors)

    def test_no_tiles(self):
        result = validate_definition(_make_def(tiles=[]))
        assert result.valid is False
        assert any("tile" in e.lower() for e in result.errors)

    def test_invalid_chart_type(self):
        tile = Tile(
            name="Bad", chart_type="line",
            query=TileQuery(table="t", fields=["t.v"]),
        )
        # Bypass Pydantic validation by setting after creation
        object.__setattr__(tile, "chart_type", "invalid_type")
        result = validate_definition(_make_def(tiles=[tile]))
        assert result.valid is False
        assert any("chart_type" in e for e in result.errors)

    def test_empty_query_fields(self):
        tile = Tile(
            name="Empty", chart_type="line",
            query=TileQuery(table="t", fields=["t.v"]),
        )
        tile.query.fields = []
        result = validate_definition(_make_def(tiles=[tile]))
        assert result.valid is False

    def test_x_axis_not_in_fields_warning(self):
        tile = Tile(
            name="Bad Axis", chart_type="line",
            query=TileQuery(table="t", fields=["t.v"]),
            vis_config=TileVisConfig(x_axis="t.missing"),
        )
        result = validate_definition(_make_def(tiles=[tile]))
        assert any("x_axis" in w for w in result.warnings)

    def test_sort_field_not_in_query_warning(self):
        tile = Tile(
            name="Bad Sort", chart_type="line",
            query=TileQuery(
                table="t",
                fields=["t.v"],
                sorts=[SortSpec(column_name="t.missing")],
            ),
        )
        result = validate_definition(_make_def(tiles=[tile]))
        assert any("sort" in w.lower() for w in result.warnings)

    def test_kpi_limit_warning(self):
        tile = Tile(
            name="KPI", chart_type="number",
            query=TileQuery(table="t", fields=["t.v"], limit=1000),
        )
        result = validate_definition(_make_def(tiles=[tile]))
        assert any("limit" in w.lower() for w in result.warnings)

    def test_unknown_format_warning(self):
        tile = Tile(
            name="Fmt", chart_type="line",
            query=TileQuery(table="t", fields=["t.v"]),
            vis_config=TileVisConfig(value_format="UNKNOWN_FORMAT"),
        )
        result = validate_definition(_make_def(tiles=[tile]))
        assert any("format" in w.lower() for w in result.warnings)

    def test_known_format_no_warning(self):
        tile = Tile(
            name="Fmt", chart_type="line",
            query=TileQuery(table="t", fields=["t.v"]),
            vis_config=TileVisConfig(value_format="USDCURRENCY_0"),
        )
        result = validate_definition(_make_def(tiles=[tile]))
        assert not any("format" in w.lower() for w in result.warnings)

    def test_field_existence_with_available_fields(self):
        tile = Tile(
            name="Missing", chart_type="line",
            query=TileQuery(table="t", fields=["t.nonexistent"]),
        )
        available = {"t": {"t.date", "t.value"}}
        result = validate_definition(_make_def(tiles=[tile]), available)
        assert result.valid is False
        assert any("nonexistent" in e for e in result.errors)

    def test_field_existence_passes_for_known_fields(self):
        tile = Tile(
            name="Good", chart_type="line",
            query=TileQuery(table="t", fields=["t.date", "t.value"]),
        )
        available = {"t": {"t.date", "t.value"}}
        result = validate_definition(_make_def(tiles=[tile]), available)
        assert result.valid is True
