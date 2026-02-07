"""Template registry for discovering and managing dashboard templates."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from omni_dash.templates.engine import TemplateEngine

logger = logging.getLogger(__name__)


class TemplateRegistry:
    """Discover and manage dashboard templates from multiple directories.

    Provides a high-level interface for listing, searching, and
    retrieving template metadata without rendering them.
    """

    def __init__(self, template_dirs: list[Path] | None = None):
        self._engine = TemplateEngine(template_dirs)
        self._cache: list[dict[str, Any]] | None = None

    @property
    def templates(self) -> list[dict[str, Any]]:
        """Get all available templates (cached)."""
        if self._cache is None:
            self._cache = self._engine.list_templates()
        return self._cache

    def list_names(self) -> list[str]:
        """Get just template names."""
        return [t["name"] for t in self.templates]

    def get_info(self, name: str) -> dict[str, Any] | None:
        """Get metadata for a specific template."""
        for t in self.templates:
            if t["name"] == name:
                return t
        return None

    def search(self, keyword: str) -> list[dict[str, Any]]:
        """Search templates by keyword in name, description, or tags."""
        keyword_lower = keyword.lower()
        results = []
        for t in self.templates:
            if keyword_lower in t["name"].lower():
                results.append(t)
            elif keyword_lower in t.get("description", "").lower():
                results.append(t)
            elif any(keyword_lower in tag.lower() for tag in t.get("tags", [])):
                results.append(t)
        return results

    def get_required_variables(self, name: str) -> dict[str, Any]:
        """Get variable specifications for a template."""
        return self._engine.get_required_variables(name)

    def invalidate_cache(self) -> None:
        """Clear cached template list."""
        self._cache = None
