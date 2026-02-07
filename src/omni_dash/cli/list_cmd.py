"""omni-dash list: List dashboards, models, topics, and templates."""

from __future__ import annotations

import json
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from omni_dash.config import get_settings
from omni_dash.exceptions import OmniDashError

console = Console()

VALID_RESOURCES = ["dashboards", "models", "topics", "templates", "dbt-models", "folders"]


def list_resources(
    resource: Annotated[str, typer.Argument(help=f"Resource type: {', '.join(VALID_RESOURCES)}")],
    model_id: Annotated[str | None, typer.Option("--model-id", help="Omni model ID (for topics)")] = None,
    folder: Annotated[str | None, typer.Option("--folder", help="Filter by folder ID")] = None,
    fmt: Annotated[str, typer.Option("--format", "-f", help="Output format: table or json")] = "table",
    layer: Annotated[str | None, typer.Option("--layer", help="dbt layer filter (mart, staging, etc.)")] = None,
) -> None:
    """List Omni or dbt resources."""
    try:
        if resource not in VALID_RESOURCES:
            console.print(f"[red]Invalid resource. Choose from: {', '.join(VALID_RESOURCES)}[/red]")
            raise typer.Exit(1)

        valid_formats = ("table", "json")
        if fmt not in valid_formats:
            console.print(f"[red]Invalid format '{fmt}'. Choose from: {', '.join(valid_formats)}[/red]")
            raise typer.Exit(1)

        if resource == "templates":
            _list_templates(fmt)
        elif resource == "dbt-models":
            _list_dbt_models(fmt, layer=layer)
        elif resource == "dashboards":
            _list_dashboards(fmt, folder=folder)
        elif resource == "models":
            _list_omni_models(fmt)
        elif resource == "topics":
            _list_topics(model_id, fmt)
        elif resource == "folders":
            _list_folders(fmt)

    except OmniDashError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e


def _list_templates(fmt: str) -> None:
    """List available dashboard templates."""
    from omni_dash.templates.registry import TemplateRegistry

    settings = get_settings()
    registry = TemplateRegistry(template_dirs=settings.template_dirs)
    templates = registry.templates

    if fmt == "json":
        console.print_json(json.dumps(templates, indent=2))
        return

    table = Table(title="Available Templates")
    table.add_column("Name", style="cyan")
    table.add_column("Description")
    table.add_column("Tags", style="dim")
    table.add_column("Variables", style="green")

    for t in templates:
        table.add_row(
            t["name"],
            t.get("description", "")[:60],
            ", ".join(t.get("tags", [])),
            ", ".join(t.get("variables", [])),
        )

    console.print(table)


def _list_dbt_models(fmt: str, *, layer: str | None) -> None:
    """List dbt models from the project."""
    settings = get_settings()
    dbt_path = settings.require_dbt()

    from omni_dash.dbt.model_registry import ModelRegistry

    registry = ModelRegistry(dbt_path)
    models = registry.list_models(layer=layer)

    if fmt == "json":
        console.print_json(json.dumps([m.model_dump() for m in models], indent=2, default=str))
        return

    table = Table(title=f"dbt Models{f' ({layer})' if layer else ''}")
    table.add_column("Name", style="cyan")
    table.add_column("Layer", style="dim")
    table.add_column("Materialization")
    table.add_column("Columns", justify="right")
    table.add_column("Omni", style="green")
    table.add_column("Description")

    for m in models:
        doc_cols = len([c for c in m.columns if c.description])
        table.add_row(
            m.name,
            m.layer,
            m.materialization,
            str(doc_cols) if doc_cols else "[dim]0[/dim]",
            "[green]Yes[/green]" if m.has_omni_grant else "",
            (m.description[:50] + "..." if len(m.description) > 50 else m.description),
        )

    console.print(table)
    console.print(f"\n  Total: {len(models)} models")


def _list_dashboards(fmt: str, *, folder: str | None) -> None:
    """List dashboards in Omni."""
    settings = get_settings()
    settings.require_api()

    from omni_dash.api.client import OmniClient
    from omni_dash.api.documents import DocumentService

    with OmniClient(settings=settings) as client:
        service = DocumentService(client)
        dashboards = service.list_dashboards(folder_id=folder)

    if fmt == "json":
        console.print_json(json.dumps([d.model_dump() for d in dashboards], indent=2))
        return

    table = Table(title="Omni Dashboards")
    table.add_column("ID", style="dim")
    table.add_column("Name", style="cyan")
    table.add_column("Type")
    table.add_column("Updated", style="dim")

    for d in dashboards:
        table.add_row(
            (d.id[:12] + "...") if d.id else "",
            d.name or "",
            d.document_type or "",
            d.updated_at[:10] if d.updated_at else "",
        )

    console.print(table)
    console.print(f"\n  Total: {len(dashboards)} dashboards")


def _list_omni_models(fmt: str) -> None:
    """List Omni models."""
    settings = get_settings()
    settings.require_api()

    from omni_dash.api.client import OmniClient
    from omni_dash.api.models import ModelService

    with OmniClient(settings=settings) as client:
        service = ModelService(client)
        models = service.list_models()

    if fmt == "json":
        console.print_json(json.dumps([m.model_dump() for m in models], indent=2))
        return

    table = Table(title="Omni Models")
    table.add_column("ID", style="dim")
    table.add_column("Name", style="cyan")
    table.add_column("Database")
    table.add_column("Schema")

    for m in models:
        table.add_row(
            (m.id[:12] + "...") if m.id else "",
            m.name or "",
            m.database or "",
            m.schema_name or "",
        )

    console.print(table)


def _list_topics(model_id: str | None, fmt: str) -> None:
    """List topics within an Omni model."""
    if not model_id:
        console.print("[red]--model-id is required for listing topics[/red]")
        raise typer.Exit(1)

    settings = get_settings()
    settings.require_api()

    from omni_dash.api.client import OmniClient
    from omni_dash.api.models import ModelService

    with OmniClient(settings=settings) as client:
        service = ModelService(client)
        topics = service.list_topics(model_id)

    if fmt == "json":
        console.print_json(json.dumps([t.model_dump() for t in topics], indent=2))
        return

    table = Table(title="Topics")
    table.add_column("Name", style="cyan")
    table.add_column("Label")
    table.add_column("Description")

    for t in topics:
        table.add_row(t.name or "", t.label or "", (t.description or "")[:60])

    console.print(table)


def _list_folders(fmt: str) -> None:
    """List Omni folders."""
    settings = get_settings()
    settings.require_api()

    from omni_dash.api.client import OmniClient
    from omni_dash.api.documents import DocumentService

    with OmniClient(settings=settings) as client:
        service = DocumentService(client)
        folders = service.list_folders()

    if fmt == "json":
        console.print_json(json.dumps(folders, indent=2))
        return

    table = Table(title="Folders")
    table.add_column("ID", style="dim")
    table.add_column("Name", style="cyan")

    for f in folders:
        table.add_row(
            str(f.get("id", ""))[:12] + "...",
            f.get("name", ""),
        )

    console.print(table)
