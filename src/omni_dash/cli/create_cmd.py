"""omni-dash create: Create a dashboard from template, file, or description."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from omni_dash.config import get_settings
from omni_dash.exceptions import OmniDashError

console = Console()


def _parse_var(var: str) -> tuple[str, str]:
    """Parse a KEY=VALUE variable string."""
    if "=" not in var:
        raise typer.BadParameter(f"Variable must be KEY=VALUE format, got: {var}")
    key, _, value = var.partition("=")
    return key.strip(), value.strip()


def _parse_json_or_string(value: str) -> str | list | dict:
    """Try to parse as JSON, fall back to string."""
    value = value.strip()
    if value.startswith(("[", "{")):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            pass
    return value


def create(
    template: Annotated[str | None, typer.Option("--template", "-t", help="Template name from library")] = None,
    from_file: Annotated[Path | None, typer.Option("--from-file", "-f", help="Dashboard definition YAML file")] = None,
    dbt_model: Annotated[str | None, typer.Option("--dbt-model", "-m", help="dbt model to base dashboard on")] = None,
    name: Annotated[str | None, typer.Option("--name", "-n", help="Dashboard name")] = None,
    model_id: Annotated[str | None, typer.Option("--model-id", help="Omni model ID")] = None,
    folder: Annotated[str | None, typer.Option("--folder", help="Omni folder ID to create in")] = None,
    var: Annotated[list[str] | None, typer.Option("--var", help="Template variables (KEY=VALUE)")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Show payload without creating")] = False,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
) -> None:
    """Create a new dashboard in Omni.

    Three modes:
      --template: Render a template with --var variables
      --from-file: Load a dashboard definition YAML
      (default): Use the fluent builder interactively
    """
    try:
        settings = get_settings()

        if from_file:
            _create_from_file(from_file, model_id=model_id, name=name, folder=folder, dry_run=dry_run, yes=yes, settings=settings)
        elif template:
            variables = {}
            for v in var or []:
                key, value = _parse_var(v)
                variables[key] = _parse_json_or_string(value)
            if dbt_model:
                variables["omni_table"] = variables.get("omni_table", dbt_model)
                variables["dbt_model"] = dbt_model
            if model_id:
                variables["omni_model_id"] = model_id
            if name:
                variables["dashboard_name"] = name

            _create_from_template(template, variables, folder=folder, dry_run=dry_run, yes=yes, settings=settings)
        else:
            console.print(
                "[yellow]Specify --template or --from-file. "
                "Run 'omni-dash create --help' for usage.[/yellow]"
            )
            raise typer.Exit(1)

    except OmniDashError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e


def _create_from_file(
    file_path: Path, *, model_id: str | None, name: str | None,
    folder: str | None, dry_run: bool, yes: bool, settings,
) -> None:
    """Create a dashboard from a YAML definition file."""
    from omni_dash.dashboard.serializer import DashboardSerializer

    if not file_path.exists():
        console.print(f"[red]File not found:[/red] {file_path}")
        raise typer.Exit(1)

    yaml_str = file_path.read_text()
    definition = DashboardSerializer.from_yaml(yaml_str)

    if model_id:
        definition = definition.model_copy(update={"model_id": model_id})
    if name:
        definition = definition.model_copy(update={"name": name})
    if folder:
        definition = definition.model_copy(update={"folder_id": folder})

    _deploy_dashboard(definition, dry_run=dry_run, yes=yes, settings=settings)


def _create_from_template(
    template_name: str, variables: dict, *, folder: str | None,
    dry_run: bool, yes: bool, settings,
) -> None:
    """Create a dashboard by rendering a template."""
    from omni_dash.templates.engine import TemplateEngine

    extra_dirs = settings.template_dirs
    engine = TemplateEngine(template_dirs=extra_dirs)

    definition = engine.render(template_name, variables)

    if folder:
        definition = definition.model_copy(update={"folder_id": folder})

    _deploy_dashboard(definition, dry_run=dry_run, yes=yes, settings=settings)


def _deploy_dashboard(definition, *, dry_run: bool, yes: bool, settings) -> None:
    """Show summary and deploy (or dry-run) a dashboard."""
    from omni_dash.dashboard.serializer import DashboardSerializer

    payload = DashboardSerializer.to_omni_create_payload(definition)

    # Show summary
    table = Table(title=f"Dashboard: {definition.name}", show_lines=True)
    table.add_column("Tile", style="cyan")
    table.add_column("Chart Type", style="green")
    table.add_column("Fields", style="dim")
    for tile in definition.tiles:
        table.add_row(
            tile.name,
            tile.chart_type,
            ", ".join(f.split(".")[-1] for f in tile.query.fields[:4]),
        )
    console.print(table)
    console.print(f"  Model ID: [dim]{definition.model_id}[/dim]")
    console.print(f"  Tiles: [bold]{len(definition.tiles)}[/bold]")

    if dry_run:
        console.print("\n[yellow]Dry run — API payload:[/yellow]")
        console.print_json(json.dumps(payload, indent=2))
        return

    if not yes:
        confirm = typer.confirm("Create this dashboard in Omni?")
        if not confirm:
            console.print("[dim]Cancelled.[/dim]")
            raise typer.Exit(0)

    settings.require_api()
    from omni_dash.api.client import OmniClient
    from omni_dash.api.documents import DocumentService

    with OmniClient(settings=settings) as client:
        service = DocumentService(client)
        result = service.create_dashboard(payload, folder_id=definition.folder_id)

    console.print(
        Panel(
            f"[green]Dashboard created![/green]\n"
            f"  ID: {result.document_id}\n"
            f"  Name: {result.name}",
            title="Success",
        )
    )
