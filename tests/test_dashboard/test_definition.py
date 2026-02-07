"""Tests for omni_dash.dashboard.definition — models, validators, enums."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from omni_dash.dashboard.definition import (
    CHART_TYPE_DEFAULTS,
    TILE_SIZE_WIDTHS,
    ChartType,
    DashboardDefinition,
    DashboardFilter,
    Tile,
    TilePosition,
    TileQuery,
    TileSize,
    TileVisConfig,
)


# ── TilePosition ──


class TestTilePosition:
    def test_defaults(self):
        p = TilePosition()
        assert p.x == 0 and p.y == 0 and p.w == 6 and p.h == 4

    def test_valid_position(self):
        p = TilePosition(x=6, y=2, w=6, h=3)
        assert p.x == 6

    def test_x_too_low(self):
        with pytest.raises(ValidationError, match="x must be 0-11"):
            TilePosition(x=-1)

    def test_x_too_high(self):
        with pytest.raises(ValidationError, match="x must be 0-11"):
            TilePosition(x=12)

    def test_w_zero(self):
        with pytest.raises(ValidationError, match="w must be 1-12"):
            TilePosition(w=0)

    def test_w_too_large(self):
        with pytest.raises(ValidationError, match="w must be 1-12"):
            TilePosition(w=13)

    def test_h_zero(self):
        with pytest.raises(ValidationError, match="h must be >= 1"):
            TilePosition(h=0)

    def test_bounds_overflow(self):
        with pytest.raises(ValidationError, match="extends beyond grid"):
            TilePosition(x=7, w=6)

    def test_bounds_at_edge(self):
        p = TilePosition(x=6, w=6)
        assert p.x + p.w == 12


# ── Enums ──


class TestEnums:
    def test_chart_type_values(self):
        assert ChartType.LINE.value == "line"
        assert ChartType.TABLE.value == "table"
        assert ChartType.STACKED_BAR.value == "stacked_bar"
        assert len(ChartType) == 16

    def test_tile_size_values(self):
        assert TileSize.FULL.value == "full"
        assert TileSize.QUARTER.value == "quarter"
        assert len(TileSize) == 5

    def test_tile_size_widths_coverage(self):
        for size in TileSize:
            assert size in TILE_SIZE_WIDTHS
        assert TILE_SIZE_WIDTHS[TileSize.FULL] == 12
        assert TILE_SIZE_WIDTHS[TileSize.HALF] == 6
        assert TILE_SIZE_WIDTHS[TileSize.QUARTER] == 3

    def test_chart_type_defaults_coverage(self):
        for ct in ChartType:
            if ct != ChartType.TEXT:
                assert ct.value in CHART_TYPE_DEFAULTS, f"Missing default for {ct.value}"


# ── TileQuery ──


class TestTileQuery:
    def test_no_fields_raises(self):
        with pytest.raises(ValidationError, match="At least one field"):
            TileQuery(table="t", fields=[])

    def test_valid_query(self):
        q = TileQuery(table="t", fields=["t.a", "t.b"])
        assert q.limit == 200


# ── Tile ──


class TestTile:
    def _make_query(self):
        return TileQuery(table="t", fields=["t.x"])

    def test_valid_tile(self):
        t = Tile(name="T1", query=self._make_query(), chart_type="bar")
        assert t.chart_type == "bar"

    def test_invalid_chart_type(self):
        with pytest.raises(ValidationError, match="Invalid chart_type"):
            Tile(name="T1", query=self._make_query(), chart_type="invalid_type")

    def test_all_chart_types_accepted(self):
        for ct in ChartType:
            Tile(name="T", query=self._make_query(), chart_type=ct.value)


# ── DashboardDefinition ──


class TestDashboardDefinition:
    def _tile(self, name: str, table: str = "t") -> Tile:
        return Tile(
            name=name,
            query=TileQuery(table=table, fields=[f"{table}.a"]),
        )

    def test_tile_count(self):
        d = DashboardDefinition(name="D", tiles=[self._tile("T1"), self._tile("T2")])
        assert d.tile_count == 2

    def test_get_tile_found(self):
        d = DashboardDefinition(name="D", tiles=[self._tile("Alpha")])
        assert d.get_tile("Alpha") is not None

    def test_get_tile_not_found(self):
        d = DashboardDefinition(name="D", tiles=[self._tile("Alpha")])
        assert d.get_tile("Missing") is None

    def test_all_fields(self):
        d = DashboardDefinition(
            name="D",
            tiles=[
                self._tile("T1", "a"),
                self._tile("T2", "b"),
            ],
        )
        assert d.all_fields() == {"a.a", "b.a"}

    def test_all_tables(self):
        d = DashboardDefinition(
            name="D",
            tiles=[
                self._tile("T1", "x"),
                self._tile("T2", "y"),
                self._tile("T3", "x"),
            ],
        )
        assert d.all_tables() == {"x", "y"}

    def test_empty_definition(self):
        d = DashboardDefinition(name="Empty")
        assert d.tile_count == 0
        assert d.all_fields() == set()
        assert d.all_tables() == set()
