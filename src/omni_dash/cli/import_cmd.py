"""omni-dash import: Import a dashboard definition into Omni."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel

from omni_dash.config import get_settings
from omni_dash.exceptions import OmniDashError

console = Console()


def import_dashboard(
    file: Annotated[Path, typer.Argument(help="Dashboard definition file (YAML or JSON)")],
    model_id: Annotated[str, typer.Option("--model-id", help="Target Omni model ID")] = "",
    folder: Annotated[str | None, typer.Option("--folder", help="Destination folder ID")] = None,
    name: Annotated[str | None, typer.Option("--name", "-n", help="Override dashboard name")] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation")] = False,
) -> None:
    """Import a dashboard from a local YAML/JSON file into Omni.

    Supports both:
    - omni-dash YAML format (from 'omni-dash export')
    - Full Omni export JSON (from 'omni-dash export --full')
    """
    try:
        if not file.exists():
            console.print(f"[red]File not found:[/red] {file}")
            raise typer.Exit(1)

        settings = get_settings()
        settings.require_api()

        content = file.read_text()

        # Detect format
        if file.suffix in (".json",):
            data = json.loads(content)
            is_omni_export = "exportVersion" in data
        else:
            import yaml
            data = yaml.safe_load(content)
            is_omni_export = False

        if is_omni_export:
            _import_omni_export(data, model_id=model_id, name=name, folder=folder, yes=yes, settings=settings)
        else:
            _import_yaml_definition(data, content, model_id=model_id, name=name, folder=folder, yes=yes, settings=settings)

    except OmniDashError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e


def _import_omni_export(data: dict, *, model_id: str, name: str | None, folder: str | None, yes: bool, settings) -> None:
    """Import using Omni's native export format."""
    from omni_dash.api.client import OmniClient
    from omni_dash.api.documents import DocumentService

    if not model_id:
        console.print("[red]--model-id is required for Omni export imports[/red]")
        raise typer.Exit(1)

    doc_name = name or data.get("document", {}).get("name", "Imported Dashboard")
    console.print(f"Importing: [cyan]{doc_name}[/cyan]")

    if not yes:
        if not typer.confirm("Import this dashboard?"):
            raise typer.Exit(0)

    with OmniClient(settings=settings) as client:
        service = DocumentService(client)
        result = service.import_dashboard(data, base_model_id=model_id, name=name, folder_id=folder)

    console.print(Panel(
        f"[green]Dashboard imported![/green]\n  ID: {result.document_id}\n  Name: {result.name}",
        title="Success",
    ))


def _import_yaml_definition(data: dict, yaml_str: str, *, model_id: str, name: str | None, folder: str | None, yes: bool, settings) -> None:
    """Import from omni-dash YAML format by creating via API."""
    from omni_dash.api.client import OmniClient
    from omni_dash.api.documents import DocumentService
    from omni_dash.dashboard.serializer import DashboardSerializer

    definition = DashboardSerializer.from_yaml(yaml_str)

    if model_id:
        definition = definition.model_copy(update={"model_id": model_id})
    if name:
        definition = definition.model_copy(update={"name": name})
    if folder:
        definition = definition.model_copy(update={"folder_id": folder})

    if not definition.model_id:
        console.print("[red]model_id is required. Provide --model-id or set it in the YAML file.[/red]")
        raise typer.Exit(1)

    console.print(f"Creating: [cyan]{definition.name}[/cyan] ({len(definition.tiles)} tiles)")

    if not yes:
        if not typer.confirm("Create this dashboard?"):
            raise typer.Exit(0)

    payload = DashboardSerializer.to_omni_create_payload(definition)

    with OmniClient(settings=settings) as client:
        service = DocumentService(client)
        result = service.create_dashboard(payload, folder_id=definition.folder_id)

    console.print(Panel(
        f"[green]Dashboard created![/green]\n  ID: {result.document_id}\n  Name: {result.name}",
        title="Success",
    ))
