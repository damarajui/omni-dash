"""omni-dash preview: Dry-run dashboard creation showing the API payload."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table

from omni_dash.config import get_settings
from omni_dash.exceptions import OmniDashError

console = Console()


def preview(
    template: Annotated[str | None, typer.Option("--template", "-t", help="Template name")] = None,
    from_file: Annotated[Path | None, typer.Option("--from-file", "-f", help="Dashboard YAML file")] = None,
    dbt_model: Annotated[str | None, typer.Option("--dbt-model", "-m", help="dbt model name")] = None,
    model_id: Annotated[str | None, typer.Option("--model-id", help="Omni model ID")] = None,
    var: Annotated[list[str] | None, typer.Option("--var", help="Template variables (KEY=VALUE)")] = None,
    output_format: Annotated[str, typer.Option("--format", help="Output: json, yaml, or summary")] = "summary",
) -> None:
    """Preview the API payload without creating a dashboard.

    Shows what would be sent to Omni, useful for debugging templates
    and validating field references.
    """
    try:
        settings = get_settings()

        if from_file:
            from omni_dash.dashboard.serializer import DashboardSerializer

            if not from_file.exists():
                console.print(f"[red]File not found:[/red] {from_file}")
                raise typer.Exit(1)

            yaml_str = from_file.read_text()
            definition = DashboardSerializer.from_yaml(yaml_str)

            if model_id:
                definition = definition.model_copy(update={"model_id": model_id})

        elif template:
            from omni_dash.templates.engine import TemplateEngine

            variables = {}
            for v in var or []:
                if "=" not in v:
                    raise typer.BadParameter(f"Variable must be KEY=VALUE, got: {v}")
                key, _, value = v.partition("=")
                value = value.strip()
                if value.startswith(("[", "{")):
                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        pass
                variables[key.strip()] = value

            if dbt_model:
                variables.setdefault("omni_table", dbt_model)
            if model_id:
                variables.setdefault("omni_model_id", model_id)

            engine = TemplateEngine(template_dirs=settings.template_dirs)
            definition = engine.render(template, variables)
        else:
            console.print("[yellow]Specify --template or --from-file[/yellow]")
            raise typer.Exit(1)

        # Output
        from omni_dash.dashboard.serializer import DashboardSerializer

        if output_format == "json":
            payload = DashboardSerializer.to_omni_create_payload(definition)
            console.print_json(json.dumps(payload, indent=2))

        elif output_format == "yaml":
            yaml_out = DashboardSerializer.to_yaml(definition)
            syntax = Syntax(yaml_out, "yaml", theme="monokai", line_numbers=True)
            console.print(syntax)

        else:
            # Summary view
            console.print(f"\n[bold]{definition.name}[/bold]")
            console.print(f"  Model ID: {definition.model_id or '[dim]not set[/dim]'}")
            console.print(f"  Source: {definition.source_template or definition.dbt_model or 'custom'}")
            console.print()

            table = Table(title="Tiles", show_lines=True)
            table.add_column("#", justify="right", style="dim")
            table.add_column("Name", style="cyan")
            table.add_column("Type", style="green")
            table.add_column("Table")
            table.add_column("Fields")
            table.add_column("Position", style="dim")

            for i, tile in enumerate(definition.tiles, 1):
                fields = ", ".join(f.split(".")[-1] for f in tile.query.fields[:3])
                if len(tile.query.fields) > 3:
                    fields += f" +{len(tile.query.fields) - 3}"
                pos = f"({tile.position.x},{tile.position.y}) {tile.position.w}x{tile.position.h}" if tile.position else "auto"
                table.add_row(str(i), tile.name, tile.chart_type, tile.query.table, fields, pos)

            console.print(table)

            if definition.filters:
                console.print("\n[bold]Filters:[/bold]")
                for f in definition.filters:
                    console.print(f"  - {f.field} ({f.filter_type})")

    except OmniDashError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e
