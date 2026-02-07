"""omni-dash export: Export an existing Omni dashboard to YAML/JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from omni_dash.config import get_settings
from omni_dash.exceptions import OmniDashError

console = Console()


def export(
    document_id: Annotated[str, typer.Argument(help="Omni dashboard/document ID to export")],
    output: Annotated[Path | None, typer.Option("--output", "-o", help="Output file path")] = None,
    fmt: Annotated[str, typer.Option("--format", "-f", help="Output format: yaml or json")] = "yaml",
    full: Annotated[bool, typer.Option("--full", help="Include full Omni export envelope")] = False,
) -> None:
    """Export a dashboard from Omni to a local file.

    Exports the dashboard definition as YAML (for version control) or
    JSON (for re-import via the API).
    """
    try:
        settings = get_settings()
        settings.require_api()

        valid_formats = ("yaml", "json")
        if fmt not in valid_formats:
            console.print(f"[red]Invalid format '{fmt}'. Choose from: {', '.join(valid_formats)}[/red]")
            raise typer.Exit(1)

        from omni_dash.api.client import OmniClient
        from omni_dash.api.documents import DocumentService
        from omni_dash.dashboard.serializer import DashboardSerializer

        with OmniClient(settings=settings) as client:
            service = DocumentService(client)

            if full:
                # Full Omni export (for import API)
                export_data = service.export_dashboard(document_id)
                content = json.dumps(export_data, indent=2)
                default_ext = ".json"
            else:
                # Export and convert to our YAML format
                export_data = service.export_dashboard(document_id)
                definition = DashboardSerializer.from_omni_export(export_data)

                if fmt == "json":
                    content = json.dumps(definition.model_dump(), indent=2)
                    default_ext = ".json"
                else:
                    content = DashboardSerializer.to_yaml(definition)
                    default_ext = ".yml"

        # Determine output path
        if output is None:
            # Auto-generate from dashboard name
            safe_name = definition.name.lower().replace(" ", "_")[:50] if not full else document_id[:12]
            output = Path(f"dashboards/{safe_name}{default_ext}")

        # Ensure parent directory exists
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(content)

        console.print(f"[green]Exported to:[/green] {output}")
        if not full:
            console.print(f"  Tiles: {len(definition.tiles)}")

    except OmniDashError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e
