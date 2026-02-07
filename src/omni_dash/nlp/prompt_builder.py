"""Build structured prompts for Claude Code integration.

Generates context-rich prompts that include dbt model metadata,
available templates, and Omni configuration so that Claude can
intelligently compose dashboard definitions.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from omni_dash.dbt.model_registry import ModelRegistry
from omni_dash.templates.registry import TemplateRegistry


class PromptBuilder:
    """Build prompts with dbt model context for Claude Code.

    This is the key integration point: when a user asks Claude Code
    to "create an SEO dashboard", this builder provides Claude with
    all the context it needs to compose the right omni-dash commands.
    """

    def __init__(
        self,
        registry: ModelRegistry,
        template_registry: TemplateRegistry | None = None,
        omni_model_id: str | None = None,
    ):
        self._registry = registry
        self._template_registry = template_registry or TemplateRegistry()
        self._omni_model_id = omni_model_id

    def build_context_prompt(
        self,
        *,
        layer: str | None = "mart",
        max_models: int = 20,
        include_columns: bool = True,
    ) -> str:
        """Build a comprehensive context prompt for Claude.

        Returns a formatted string that can be injected as system
        context or user context for Claude Code sessions.
        """
        sections = []

        # Section 1: Available dbt models
        models = self._registry.list_models(layer=layer)[:max_models]
        sections.append("## Available dbt Models\n")

        for model in models:
            omni_flag = " [Omni-ready]" if model.has_omni_grant else ""
            sections.append(f"### {model.name}{omni_flag}")
            if model.description:
                sections.append(f"  {model.description.strip()}")
            sections.append(f"  - Materialization: {model.materialization}")
            sections.append(f"  - Database: {model.database}.{model.schema_name}")

            if include_columns and model.columns:
                sections.append("  - Columns:")
                for col in model.columns:
                    desc = f" - {col.description}" if col.description else ""
                    dtype = f" ({col.data_type})" if col.data_type else ""
                    sections.append(f"    - `{col.name}`{dtype}{desc}")
            sections.append("")

        # Section 2: Available templates
        templates = self._template_registry.templates
        if templates:
            sections.append("## Available Dashboard Templates\n")
            for t in templates:
                sections.append(f"### {t['name']}")
                if t.get("description"):
                    sections.append(f"  {t['description']}")
                if t.get("variables"):
                    sections.append(f"  Variables: {', '.join(t['variables'])}")
                if t.get("tags"):
                    sections.append(f"  Tags: {', '.join(t['tags'])}")
                sections.append("")

        # Section 3: Omni configuration
        sections.append("## Omni Configuration\n")
        if self._omni_model_id:
            sections.append(f"- Model ID: `{self._omni_model_id}`")
        sections.append("- CLI: `omni-dash` (installed)")
        sections.append("- API: Omni REST API via `OMNI_API_KEY`")
        sections.append("")

        # Section 4: Usage instructions
        sections.append("## How to Create a Dashboard\n")
        sections.append("### Option 1: From template")
        sections.append("```bash")
        sections.append('omni-dash create --template <name> --dbt-model <model> --var key=value')
        sections.append("```\n")
        sections.append("### Option 2: From YAML definition file")
        sections.append("```bash")
        sections.append("omni-dash create --from-file dashboards/my_dashboard.yml")
        sections.append("```\n")
        sections.append("### Option 3: Preview first (dry-run)")
        sections.append("```bash")
        sections.append("omni-dash preview --template <name> --dbt-model <model> --var key=value")
        sections.append("```\n")
        sections.append("### Useful commands")
        sections.append("```bash")
        sections.append("omni-dash dbt inspect <model>   # See model columns")
        sections.append("omni-dash dbt suggest <model>   # Get template suggestions")
        sections.append("omni-dash list templates         # See all templates")
        sections.append("omni-dash export <id>            # Export existing dashboard")
        sections.append("```")

        return "\n".join(sections)

    def build_model_prompt(self, model_name: str) -> str:
        """Build a focused prompt for a specific dbt model.

        Useful when the user has already identified which model to
        dashboard — provides deep column context.
        """
        model = self._registry.get_model(model_name)

        lines = [
            f"## dbt Model: {model.name}",
            "",
            model.description or "No description available.",
            "",
            f"- Database: {model.database}.{model.schema_name}",
            f"- Materialization: {model.materialization}",
            f"- Omni-ready: {'Yes' if model.has_omni_grant else 'No'}",
            "",
        ]

        if model.columns:
            lines.append("### Columns")
            for col in model.columns:
                tests = f" [tests: {', '.join(col.tests)}]" if col.tests else ""
                desc = col.description or "No description"
                lines.append(f"- **{col.name}**: {desc}{tests}")
            lines.append("")

        # Suggest templates
        col_names = {c.name for c in model.columns}
        has_time = any(n in col_names for n in ("week_start", "month_start", "day_start"))
        has_dimension = any(n in col_names for n in ("channel", "source", "type", "category", "page_type", "llm_source"))
        has_page = any("page" in n or "path" in n for n in col_names)

        lines.append("### Suggested templates")
        if has_time and len(model.columns) >= 4:
            lines.append("- `weekly_funnel` - funnel metrics over time")
        if has_time:
            lines.append("- `time_series_kpi` - line charts over time")
        if has_time and has_dimension:
            lines.append("- `channel_breakdown` - breakdown by category")
        if has_page:
            lines.append("- `page_performance` - page-level metrics")

        return "\n".join(lines)

    def build_creation_prompt(self, description: str) -> str:
        """Build a prompt that asks Claude to generate a dashboard definition.

        This is the "natural language to dashboard" pathway: the user
        provides a plain English description, and this prompt gives
        Claude everything it needs to produce a valid YAML definition.
        """
        context = self.build_context_prompt(include_columns=True)

        return f"""You are a dashboard architect. Given the user's description and the available
dbt models and templates below, generate a dashboard definition YAML file.

{context}

## User's Request
{description}

## Instructions
1. Select the most appropriate dbt model(s) based on the description.
2. Choose the best template OR design custom tiles.
3. Generate a complete dashboard YAML in omni-dash format.
4. Include appropriate filters (usually a date range).
5. Choose chart types that best visualize each metric.

Output ONLY the YAML content, wrapped in ```yaml ... ``` code fences.
The YAML must be valid omni-dash format (parseable by `DashboardSerializer.from_yaml`).
"""
