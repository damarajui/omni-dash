"""Registry of Omni tools for the Anthropic SDK agent loop.

Maps each MCP server function to an Anthropic-compatible tool definition
(name, description, input_schema, callable).  The agent loop calls
``get_definitions()`` to build the ``tools=`` list for
``messages.create()``, then ``get()`` to look up the callable at dispatch
time.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class RegisteredTool:
    """A tool registered for use by the agent loop."""

    name: str
    description: str
    input_schema: dict[str, Any]
    callable: Callable[..., str]


class ToolRegistry:
    """Registers all 25 tools (24 Omni MCP + save_learning) with Anthropic-format schemas."""

    def __init__(self) -> None:
        self._tools: dict[str, RegisteredTool] = {}
        self._register_all()

    def get_definitions(self) -> list[dict[str, Any]]:
        """Return Anthropic-format tool definitions for ``messages.create(tools=...)``."""
        return [
            {
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_schema,
            }
            for t in self._tools.values()
        ]

    def get(self, name: str) -> RegisteredTool | None:
        """Look up a tool by name."""
        return self._tools.get(name)

    @property
    def tool_count(self) -> int:
        return len(self._tools)

    def _register(
        self,
        name: str,
        description: str,
        input_schema: dict[str, Any],
        func: Callable[..., str],
    ) -> None:
        self._tools[name] = RegisteredTool(
            name=name,
            description=description,
            input_schema=input_schema,
            callable=func,
        )

    def _register_all(self) -> None:
        from omni_dash.mcp import server as srv

        # --- Data Discovery ---
        self._register(
            "list_topics",
            "List available data topics (tables/views) in the Omni model.",
            {
                "type": "object",
                "properties": {
                    "model_id": {
                        "type": "string",
                        "description": "Omni model ID. Auto-discovered if omitted.",
                    },
                },
                "required": [],
            },
            srv.list_topics,
        )
        self._register(
            "get_topic_fields",
            "Get all fields (columns) for a specific topic.",
            {
                "type": "object",
                "properties": {
                    "topic_name": {"type": "string", "description": "Topic name from list_topics."},
                    "model_id": {"type": "string"},
                },
                "required": ["topic_name"],
            },
            srv.get_topic_fields,
        )
        self._register(
            "query_data",
            "Run a query against Omni and return data rows.",
            {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Topic/table name."},
                    "fields": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Qualified field names.",
                    },
                    "sorts": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Sort specs.",
                    },
                    "filters": {"type": "object", "description": "Filter map."},
                    "limit": {"type": "integer", "description": "Max rows (default 25, max 1000)."},
                    "model_id": {"type": "string"},
                },
                "required": ["table", "fields"],
            },
            srv.query_data,
        )
        self._register(
            "profile_data",
            "Profile data in a table — field distributions, types, min/max.",
            {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "fields": {"type": "array", "items": {"type": "string"}},
                    "sample_size": {"type": "integer"},
                    "model_id": {"type": "string"},
                },
                "required": ["table"],
            },
            srv.profile_data,
        )
        self._register(
            "list_folders",
            "List all folders in the Omni org.",
            {"type": "object", "properties": {}, "required": []},
            srv.list_folders,
        )

        # --- Dashboard Building ---
        self._register(
            "create_dashboard",
            "Create a new Omni dashboard from a tile specification.",
            {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Dashboard display name."},
                    "tiles": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Tile specs.",
                    },
                    "description": {"type": "string"},
                    "folder_id": {"type": "string"},
                    "filters": {"type": "array", "items": {"type": "object"}},
                    "model_id": {"type": "string"},
                },
                "required": ["name", "tiles"],
            },
            srv.create_dashboard,
        )
        self._register(
            "generate_dashboard",
            "Generate a complete dashboard from natural language using AI.",
            {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string", "description": "Natural language description."},
                    "folder_id": {"type": "string"},
                    "model_id": {"type": "string"},
                },
                "required": ["prompt"],
            },
            srv.generate_dashboard,
        )
        self._register(
            "suggest_chart",
            "Suggest the best chart type for given table and fields.",
            {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "fields": {"type": "array", "items": {"type": "string"}},
                    "model_id": {"type": "string"},
                },
                "required": ["table"],
            },
            srv.suggest_chart,
        )
        self._register(
            "validate_dashboard",
            "Pre-flight validation for a dashboard spec.",
            {
                "type": "object",
                "properties": {
                    "tiles": {"type": "array", "items": {"type": "object"}},
                    "model_id": {"type": "string"},
                },
                "required": ["tiles"],
            },
            srv.validate_dashboard,
        )

        # --- Dashboard Management ---
        self._register(
            "list_dashboards",
            "List dashboards in the Omni org.",
            {
                "type": "object",
                "properties": {
                    "folder_id": {"type": "string"},
                },
                "required": [],
            },
            srv.list_dashboards,
        )
        self._register(
            "get_dashboard",
            "Get a dashboard's details by ID.",
            {
                "type": "object",
                "properties": {
                    "dashboard_id": {"type": "string"},
                },
                "required": ["dashboard_id"],
            },
            srv.get_dashboard,
        )
        self._register(
            "update_dashboard",
            "Update an existing dashboard (replace tiles or metadata).",
            {
                "type": "object",
                "properties": {
                    "dashboard_id": {"type": "string"},
                    "name": {"type": "string"},
                    "tiles": {"type": "array", "items": {"type": "object"}},
                    "description": {"type": "string"},
                    "filters": {"type": "array", "items": {"type": "object"}},
                    "folder_id": {"type": "string"},
                    "model_id": {"type": "string"},
                },
                "required": ["dashboard_id"],
            },
            srv.update_dashboard,
        )
        self._register(
            "add_tiles_to_dashboard",
            "Append new tiles to an existing dashboard without replacing existing ones.",
            {
                "type": "object",
                "properties": {
                    "dashboard_id": {"type": "string"},
                    "tiles": {"type": "array", "items": {"type": "object"}},
                    "model_id": {"type": "string"},
                },
                "required": ["dashboard_id", "tiles"],
            },
            srv.add_tiles_to_dashboard,
        )
        self._register(
            "update_tile",
            "Update a single tile in-place (SQL, fields, filters, chart type).",
            {
                "type": "object",
                "properties": {
                    "dashboard_id": {"type": "string"},
                    "tile_name": {"type": "string"},
                    "sql": {"type": "string"},
                    "fields": {"type": "array", "items": {"type": "string"}},
                    "filters": {"type": "object"},
                    "chart_type": {"type": "string"},
                    "title": {"type": "string"},
                },
                "required": ["dashboard_id", "tile_name"],
            },
            srv.update_tile,
        )
        self._register(
            "delete_dashboard",
            "Delete a dashboard by ID.",
            {
                "type": "object",
                "properties": {
                    "dashboard_id": {"type": "string"},
                },
                "required": ["dashboard_id"],
            },
            srv.delete_dashboard,
        )
        self._register(
            "clone_dashboard",
            "Clone a dashboard with a new name.",
            {
                "type": "object",
                "properties": {
                    "dashboard_id": {"type": "string"},
                    "new_name": {"type": "string"},
                    "folder_id": {"type": "string"},
                    "model_id": {"type": "string"},
                },
                "required": ["dashboard_id", "new_name"],
            },
            srv.clone_dashboard,
        )
        self._register(
            "move_dashboard",
            "Move a dashboard to a different folder.",
            {
                "type": "object",
                "properties": {
                    "dashboard_id": {"type": "string"},
                    "target_folder_id": {"type": "string"},
                    "model_id": {"type": "string"},
                },
                "required": ["dashboard_id", "target_folder_id"],
            },
            srv.move_dashboard,
        )
        self._register(
            "export_dashboard",
            "Export a dashboard's full definition (for backup or cloning).",
            {
                "type": "object",
                "properties": {
                    "dashboard_id": {"type": "string"},
                },
                "required": ["dashboard_id"],
            },
            srv.export_dashboard,
        )
        self._register(
            "import_dashboard",
            "Import a dashboard from an export payload.",
            {
                "type": "object",
                "properties": {
                    "export_data": {"type": "object"},
                    "name": {"type": "string"},
                    "folder_id": {"type": "string"},
                    "model_id": {"type": "string"},
                },
                "required": ["export_data"],
            },
            srv.import_dashboard,
        )
        self._register(
            "get_dashboard_filters",
            "Get the filter configuration for a dashboard.",
            {
                "type": "object",
                "properties": {
                    "dashboard_id": {"type": "string"},
                },
                "required": ["dashboard_id"],
            },
            srv.get_dashboard_filters,
        )
        self._register(
            "update_dashboard_filters",
            "Update existing filter values on a dashboard.",
            {
                "type": "object",
                "properties": {
                    "dashboard_id": {"type": "string"},
                    "filters": {"type": "object"},
                    "filter_order": {"type": "array", "items": {"type": "string"}},
                    "clear_existing_draft": {"type": "boolean"},
                },
                "required": ["dashboard_id"],
            },
            srv.update_dashboard_filters,
        )

        # --- Omni AI ---
        self._register(
            "ai_generate_query",
            "Convert natural language to a structured Omni query.",
            {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string"},
                    "topic_name": {"type": "string"},
                    "model_id": {"type": "string"},
                },
                "required": ["prompt"],
            },
            srv.ai_generate_query,
        )
        self._register(
            "ai_pick_topic",
            "Use Omni AI to pick the best data topic for a question.",
            {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string"},
                    "model_id": {"type": "string"},
                },
                "required": ["prompt"],
            },
            srv.ai_pick_topic,
        )
        self._register(
            "ai_analyze",
            "Run an AI-powered data analysis using Omni's native AI.",
            {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string"},
                    "topic_name": {"type": "string"},
                    "model_id": {"type": "string"},
                },
                "required": ["prompt"],
            },
            srv.ai_analyze,
        )

        # --- Feedback / Self-Improvement ---
        self._register(
            "save_learning",
            (
                "Save a learning or correction to LEARNINGS.md and push to GitHub. "
                "Use this when the user gives feedback, corrects a mistake, or "
                "says 'remember this' / 'next time do X'. The learning should be "
                "a concise, actionable rule."
            ),
            {
                "type": "object",
                "properties": {
                    "learning": {
                        "type": "string",
                        "description": (
                            "Concise actionable rule to remember, "
                            "e.g. 'Always use stacked_bar for composition data'"
                        ),
                    },
                },
                "required": ["learning"],
            },
            self._save_learning,
        )

    @staticmethod
    def _save_learning(learning: str) -> str:
        """Persist a learning to LEARNINGS.md via GitHub API.

        Imports ``add_learning`` dynamically — works whether ``scripts/``
        is on ``sys.path`` (local dev) or not (Docker).
        """
        import json
        import sys
        from pathlib import Path

        # Ensure scripts/ is importable (Docker may not have it on sys.path)
        scripts_dir = str(Path(__file__).resolve().parents[3] / "scripts")
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)

        try:
            from github_utils import add_learning

            success = add_learning(learning)
            if success:
                return json.dumps({"status": "ok", "message": f"Learning saved: {learning}"})
            return json.dumps({"error": "Failed to save learning — check GITHUB_TOKEN"})
        except ImportError:
            logger.warning("github_utils not available — cannot save learning")
            return json.dumps({"error": "Learning persistence not available in this environment"})
