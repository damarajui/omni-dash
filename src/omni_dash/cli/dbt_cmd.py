"""omni-dash dbt: Inspect dbt models and suggest dashboard configurations."""

from __future__ import annotations

import json
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from omni_dash.config import get_settings
from omni_dash.exceptions import OmniDashError

console = Console()


def models(
    layer: Annotated[str | None, typer.Option("--layer", "-l", help="Filter by layer (mart, staging, etc.)")] = None,
    omni_only: Annotated[bool, typer.Option("--omni-only", help="Only show Omni-eligible models")] = False,
    fmt: Annotated[str, typer.Option("--format", "-f", help="Output format: table or json")] = "table",
) -> None:
    """List dbt models available for dashboarding."""
    try:
        settings = get_settings()
        dbt_path = settings.require_dbt()

        from omni_dash.dbt.model_registry import ModelRegistry

        registry = ModelRegistry(dbt_path)

        if omni_only:
            model_list = registry.list_omni_eligible_models()
        else:
            model_list = registry.list_models(layer=layer)

        if fmt == "json":
            console.print_json(json.dumps(
                [m.model_dump(include={"name", "layer", "materialization", "description", "has_omni_grant"})
                 for m in model_list],
                indent=2,
                default=str,
            ))
            return

        title = "dbt Models"
        if layer:
            title += f" ({layer})"
        if omni_only:
            title += " (Omni-eligible)"

        table = Table(title=title)
        table.add_column("Name", style="cyan")
        table.add_column("Layer", style="dim")
        table.add_column("Mat.", style="dim")
        table.add_column("Documented Cols", justify="right")
        table.add_column("Omni", style="green")

        for m in model_list:
            doc_cols = len([c for c in m.columns if c.description])
            table.add_row(
                m.name,
                m.layer,
                m.materialization[:5] if m.materialization else "",
                str(doc_cols),
                "Yes" if m.has_omni_grant else "",
            )

        console.print(table)
        console.print(f"\n  {len(model_list)} models")

    except OmniDashError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e


def inspect(
    model_name: Annotated[str, typer.Argument(help="dbt model name to inspect")],
    fmt: Annotated[str, typer.Option("--format", "-f", help="Output format: table or json")] = "table",
) -> None:
    """Show detailed metadata for a dbt model."""
    try:
        settings = get_settings()
        dbt_path = settings.require_dbt()

        from omni_dash.dbt.model_registry import ModelRegistry

        registry = ModelRegistry(dbt_path)
        model = registry.get_model(model_name)

        if fmt == "json":
            console.print_json(json.dumps(model.model_dump(), indent=2, default=str))
            return

        # Header
        console.print(Panel(
            f"[bold]{model.name}[/bold]\n\n"
            f"{model.description}\n\n"
            f"  Layer: {model.layer}\n"
            f"  Materialization: {model.materialization}\n"
            f"  Database: {model.database}.{model.schema_name}\n"
            f"  Omni grant: {'Yes' if model.has_omni_grant else 'No'}\n"
            f"  Path: {model.path}",
            title="Model Details",
        ))

        # Columns table
        if model.columns:
            table = Table(title="Columns")
            table.add_column("Name", style="cyan")
            table.add_column("Type", style="dim")
            table.add_column("Tests", style="green")
            table.add_column("Description")

            for col in model.columns:
                table.add_row(
                    col.name,
                    col.data_type or "",
                    ", ".join(col.tests) if col.tests else "",
                    col.description[:70] if col.description else "[dim]undocumented[/dim]",
                )

            console.print(table)
        else:
            console.print("[yellow]No columns documented in schema.yml[/yellow]")

        # Dependencies
        if model.depends_on:
            deps = [d.split(".")[-1] for d in model.depends_on if d.startswith("model.")]
            if deps:
                console.print(f"\n  Depends on: {', '.join(deps)}")

    except OmniDashError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e


def suggest(
    model_name: Annotated[str | None, typer.Argument(help="dbt model name (optional)")] = None,
) -> None:
    """Suggest dashboard templates for dbt models.

    Without a model name, suggests the best models for dashboarding.
    With a model name, suggests which templates fit that model.
    """
    try:
        settings = get_settings()
        dbt_path = settings.require_dbt()

        from omni_dash.dbt.model_registry import ModelRegistry
        from omni_dash.templates.registry import TemplateRegistry

        registry = ModelRegistry(dbt_path)
        template_registry = TemplateRegistry(template_dirs=settings.template_dirs)

        if model_name is None:
            # Suggest models
            candidates = registry.suggest_dashboard_models()
            console.print("[bold]Recommended models for dashboarding:[/bold]\n")

            for i, m in enumerate(candidates[:10], 1):
                doc_cols = len([c for c in m.columns if c.description])
                omni = " [green](Omni-ready)[/green]" if m.has_omni_grant else ""
                console.print(f"  {i}. [cyan]{m.name}[/cyan]{omni}")
                console.print(f"     {m.description[:80]}" if m.description else "")
                console.print(f"     {doc_cols} documented columns, {m.materialization}")
                console.print()
        else:
            # Suggest templates for a specific model
            model = registry.get_model(model_name)
            col_names = {c.name for c in model.columns}

            console.print(f"[bold]Template suggestions for [cyan]{model_name}[/cyan]:[/bold]\n")

            has_time_col = any(
                name in col_names
                for name in ("week_start", "month_start", "day_start", "date", "created_at")
            )
            has_dimension = any(
                name in col_names
                for name in ("channel", "source", "medium", "type", "category", "page_type", "llm_source")
            )
            has_page = any("page" in name or "url" in name or "path" in name for name in col_names)
            numeric_cols = [c.name for c in model.columns if c.name not in (
                "week_start", "month_start", "day_start", "date",
            ) and not c.name.endswith("_id")]

            suggestions = []

            if has_time_col and len(numeric_cols) >= 3:
                suggestions.append(("weekly_funnel", "Weekly funnel with multiple metrics over time"))
            if has_time_col and numeric_cols:
                suggestions.append(("time_series_kpi", "KPI line charts over time"))
            if has_time_col and has_dimension:
                suggestions.append(("channel_breakdown", "Breakdown by dimension with stacked bars"))
            if has_page:
                suggestions.append(("page_performance", "Page-level table and charts"))

            if not suggestions:
                suggestions.append(("time_series_kpi", "Generic KPI dashboard (best default)"))

            for name, reason in suggestions:
                info = template_registry.get_info(name)
                desc = info.get("description", "") if info else ""
                console.print(f"  [green]{name}[/green] - {reason}")
                if desc:
                    console.print(f"    {desc[:80]}")

                # Show example command
                time_col = next((n for n in ("week_start", "month_start", "day_start") if n in col_names), "date")
                example_metrics = numeric_cols[:3]
                metrics_json = json.dumps(example_metrics)
                console.print(
                    f"    [dim]omni-dash create --template {name} --dbt-model {model_name} "
                    f"--var time_column={time_col} --var metric_columns='{metrics_json}'[/dim]"
                )
                console.print()

    except OmniDashError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e
