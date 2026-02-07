"""Tests for omni_dash.dashboard.layout — grid auto-positioning and compaction."""

from __future__ import annotations

import pytest

from omni_dash.dashboard.definition import Tile, TilePosition, TileQuery
from omni_dash.dashboard.layout import LayoutManager


def _tile(name: str, chart_type: str = "line", size: str = "half", position: TilePosition | None = None) -> Tile:
    return Tile(
        name=name,
        query=TileQuery(table="t", fields=["t.a"]),
        chart_type=chart_type,
        size=size,
        position=position,
    )


class TestAutoPosition:
    def test_single_tile(self):
        tiles = LayoutManager.auto_position([_tile("A")])
        assert tiles[0].position is not None
        assert tiles[0].position.x == 0
        assert tiles[0].position.y == 0

    def test_two_half_tiles_side_by_side(self):
        tiles = LayoutManager.auto_position([_tile("A"), _tile("B")])
        assert tiles[0].position.x == 0
        assert tiles[1].position.x == 6
        assert tiles[0].position.y == tiles[1].position.y == 0

    def test_full_width_pushes_next_down(self):
        tiles = LayoutManager.auto_position([
            _tile("A", chart_type="table", size="full"),
            _tile("B"),
        ])
        assert tiles[0].position.w == 12
        assert tiles[1].position.y > 0

    def test_preserves_existing_positions(self):
        pos = TilePosition(x=6, y=0, w=6, h=4)
        tiles = LayoutManager.auto_position([
            _tile("Pre", position=pos),
            _tile("Auto"),
        ])
        assert tiles[0].position == pos
        assert tiles[1].position.y >= 0  # Auto gets positioned around it

    def test_quarter_tiles_fit_four_across(self):
        tiles = LayoutManager.auto_position([
            _tile("A", chart_type="number", size="quarter"),
            _tile("B", chart_type="number", size="quarter"),
            _tile("C", chart_type="number", size="quarter"),
            _tile("D", chart_type="number", size="quarter"),
        ])
        xs = [t.position.x for t in tiles]
        assert xs == [0, 3, 6, 9]
        assert all(t.position.y == 0 for t in tiles)

    def test_empty_list(self):
        assert LayoutManager.auto_position([]) == []


class TestCompact:
    def test_removes_vertical_gap(self):
        tiles = [
            _tile("A", position=TilePosition(x=0, y=0, w=6, h=2)),
            _tile("B", position=TilePosition(x=0, y=5, w=6, h=2)),  # gap at y=2-4
        ]
        compacted = LayoutManager.compact(tiles)
        assert compacted[1].position.y == 2  # moved up to fill gap

    def test_no_overlap_after_compact(self):
        tiles = [
            _tile("A", position=TilePosition(x=0, y=0, w=6, h=4)),
            _tile("B", position=TilePosition(x=0, y=10, w=6, h=4)),
        ]
        compacted = LayoutManager.compact(tiles)
        a_end = compacted[0].position.y + compacted[0].position.h
        assert compacted[1].position.y >= a_end

    def test_empty_list(self):
        assert LayoutManager.compact([]) == []

    def test_tile_without_position_preserved(self):
        tiles = [_tile("A")]  # no position
        result = LayoutManager.compact(tiles)
        assert result[0].position is None


class TestTileDimensions:
    def test_explicit_size_overrides(self):
        tile = _tile("A", chart_type="line", size="full")
        w, h = LayoutManager._tile_dimensions(tile)
        assert w == 12

    def test_chart_type_defaults_used(self):
        tile = _tile("A", chart_type="number", size="invalid")
        w, h = LayoutManager._tile_dimensions(tile)
        assert w == 3 and h == 2

    def test_table_defaults(self):
        tile = _tile("A", chart_type="table", size="invalid")
        w, h = LayoutManager._tile_dimensions(tile)
        assert w == 12 and h == 6
