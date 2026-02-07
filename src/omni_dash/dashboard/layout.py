"""Grid layout manager for auto-positioning dashboard tiles.

Uses a 12-column grid (standard responsive layout). Tiles are placed
left-to-right, top-to-bottom, avoiding overlap.
"""

from __future__ import annotations

from omni_dash.dashboard.definition import (
    CHART_TYPE_DEFAULTS,
    TILE_SIZE_WIDTHS,
    Tile,
    TilePosition,
    TileSize,
)

GRID_COLS = 12


class LayoutManager:
    """Position tiles on a 12-column grid automatically.

    Algorithm:
    1. For each tile, determine its width and height from chart type or explicit size.
    2. Scan the grid row by row, left to right, looking for a spot where the tile fits.
    3. Place the tile and mark the occupied cells.
    4. Continue until all tiles are placed.
    """

    @staticmethod
    def auto_position(tiles: list[Tile]) -> list[Tile]:
        """Assign grid positions to all tiles that don't have explicit positions.

        Tiles with existing positions are respected. Only tiles with
        position=None get auto-positioned.
        """
        # Track occupied cells: set of (x, y) tuples
        occupied: set[tuple[int, int]] = set()

        # First pass: register pre-positioned tiles
        for tile in tiles:
            if tile.position is not None:
                for dx in range(tile.position.w):
                    for dy in range(tile.position.h):
                        occupied.add((tile.position.x + dx, tile.position.y + dy))

        # Second pass: position remaining tiles
        result = []
        for tile in tiles:
            if tile.position is not None:
                result.append(tile)
                continue

            w, h = LayoutManager._tile_dimensions(tile)

            # Find the first available position
            pos = LayoutManager._find_position(occupied, w, h)

            # Mark as occupied
            for dx in range(w):
                for dy in range(h):
                    occupied.add((pos.x + dx, pos.y + dy))

            result.append(
                tile.model_copy(update={"position": pos})
            )

        return result

    @staticmethod
    def _tile_dimensions(tile: Tile) -> tuple[int, int]:
        """Determine tile width and height."""
        # Explicit size takes priority
        try:
            size_enum = TileSize(tile.size)
            w = TILE_SIZE_WIDTHS[size_enum]
        except (ValueError, KeyError):
            w = None

        # Chart type defaults
        chart_defaults = CHART_TYPE_DEFAULTS.get(tile.chart_type, (6, 4))

        if w is None:
            w = chart_defaults[0]

        h = chart_defaults[1]

        return w, h

    @staticmethod
    def _find_position(
        occupied: set[tuple[int, int]], w: int, h: int
    ) -> TilePosition:
        """Find the first available grid position for a tile of size (w, h).

        Scans top-to-bottom, left-to-right looking for a contiguous
        rectangular space.
        """
        max_y = 0
        if occupied:
            max_y = max(y for _, y in occupied) + 1

        # Search up to max_y + h rows beyond current content
        for y in range(max_y + h + 10):
            for x in range(GRID_COLS - w + 1):
                # Check if all cells in the rectangle are free
                if all(
                    (x + dx, y + dy) not in occupied
                    for dx in range(w)
                    for dy in range(h)
                ):
                    return TilePosition(x=x, y=y, w=w, h=h)

        # Fallback: place at the bottom
        return TilePosition(x=0, y=max_y + 1, w=w, h=h)

    @staticmethod
    def compact(tiles: list[Tile]) -> list[Tile]:
        """Re-compact tiles to remove vertical gaps.

        Moves each tile as far up as possible without overlapping
        other tiles. Preserves horizontal positions.
        """
        if not tiles:
            return tiles

        # Sort by current y position
        sorted_tiles = sorted(
            tiles,
            key=lambda t: (t.position.y if t.position else 0, t.position.x if t.position else 0),
        )

        occupied: set[tuple[int, int]] = set()
        result = []

        for tile in sorted_tiles:
            if tile.position is None:
                result.append(tile)
                continue

            w, h = tile.position.w, tile.position.h
            x = tile.position.x

            # Find the lowest y where this tile fits at its current x
            best_y = 0
            for y in range(tile.position.y + 1):
                if all(
                    (x + dx, y + dy) not in occupied
                    for dx in range(w)
                    for dy in range(h)
                ):
                    best_y = y
                    break
            else:
                best_y = tile.position.y

            new_pos = TilePosition(x=x, y=best_y, w=w, h=h)
            for dx in range(w):
                for dy in range(h):
                    occupied.add((x + dx, best_y + dy))

            result.append(tile.model_copy(update={"position": new_pos}))

        return result
