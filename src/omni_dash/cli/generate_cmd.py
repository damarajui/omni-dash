"""CLI command for AI-powered dashboard generation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from omni_dash.config import get_settings

console = Console()


def generate(
    description: str = typer.Argument(..., help="Natural language description of the dashboard."),
    dbt_path: str | None = typer.Option(
        None, "--dbt-path", help="Path to dbt project. Defaults to DBT_PROJECT_PATH env var."
    ),
    model: str = typer.Option(
        "claude-sonnet-4-5-20250929", "--model", "-m", help="Claude model to use for generation."
    ),
    output: str | None = typer.Option(
        None, "--output", "-o", help="Save dashboard definition as YAML to this path."
    ),
    preview: bool = typer.Option(
        False, "--preview", "-p", help="Show the generated definition without pushing to Omni."
    ),
    push: bool = typer.Option(
        False, "--push", help="Push the dashboard to Omni API."
    ),
    omni_model_id: str | None = typer.Option(
        None, "--omni-model-id", help="Omni model ID (required for --push)."
    ),
    folder: str | None = typer.Option(
        None, "--folder", help="Omni folder ID to create the dashboard in."
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-V", help="Show tool calls as they happen."
    ),
) -> None:
    """Generate a dashboard from a natural language description using Claude AI."""
    settings = get_settings()

    # Resolve dbt path
    resolved_dbt_path = dbt_path or settings.dbt_project_path
    if not resolved_dbt_path:
        console.print(
            "[red]Error:[/red] No dbt project path. Set DBT_PROJECT_PATH or use --dbt-path.",
        )
        raise typer.Exit(1)

    dbt_dir = Path(resolved_dbt_path).expanduser()
    if not dbt_dir.exists():
        console.print(f"[red]Error:[/red] dbt project path does not exist: {dbt_dir}")
        raise typer.Exit(1)

    # Validate push requirements
    if push and not omni_model_id:
        console.print("[red]Error:[/red] --omni-model-id is required when using --push.")
        raise typer.Exit(1)

    from omni_dash.dbt.model_registry import ModelRegistry
    from omni_dash.exceptions import AIGenerationError, AINotAvailableError, ConfigurationError

    registry = ModelRegistry(dbt_dir)

    # Verbose callback
    def on_tool_call(tool_name: str, tool_input: dict[str, Any], result: str) -> None:
        if verbose:
            input_preview = str(tool_input)[:100]
            result_preview = result[:200]
            console.print(f"  [dim]tool:[/dim] [cyan]{tool_name}[/cyan]({input_preview})")
            console.print(f"  [dim]  -> {result_preview}[/dim]")

    try:
        from omni_dash.ai.service import DashboardAI

        ai = DashboardAI(registry, model=model)

        with console.status("[bold green]Generating dashboard..."):
            result = ai.generate(description, on_tool_call=on_tool_call)

    except AINotAvailableError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except ConfigurationError as e:
        console.print(f"[red]Configuration error:[/red] {e}")
        raise typer.Exit(1)
    except AIGenerationError as e:
        console.print(f"[red]Generation failed:[/red] {e}")
        raise typer.Exit(1)

    defn = result.definition

    # Display summary
    console.print()
    console.print(Panel(
        f"[bold]{defn.name}[/bold]\n{defn.description}" if defn.description else f"[bold]{defn.name}[/bold]",
        title="Generated Dashboard",
        border_style="green",
    ))

    # Tiles table
    tile_table = Table(title="Tiles", show_lines=True)
    tile_table.add_column("Name", style="cyan")
    tile_table.add_column("Chart Type", style="magenta")
    tile_table.add_column("Size")
    tile_table.add_column("Fields")

    for tile in defn.tiles:
        fields_str = ", ".join(f.split(".")[-1] for f in tile.query.fields[:4])
        if len(tile.query.fields) > 4:
            fields_str += f" (+{len(tile.query.fields) - 4} more)"
        tile_table.add_row(tile.name, tile.chart_type, tile.size, fields_str)

    console.print(tile_table)

    # Stats
    console.print(f"\n[dim]Model: {result.model_name or 'N/A'} | "
                  f"Tool calls: {result.tool_calls_made} | "
                  f"Tiles: {defn.tile_count} | "
                  f"Filters: {len(defn.filters)}[/dim]")

    # Show reasoning if verbose
    if verbose and result.reasoning:
        console.print()
        console.print(Panel(result.reasoning[:1000], title="AI Reasoning", border_style="dim"))

    # Apply folder if specified
    if folder:
        defn.folder_id = folder

    # Output YAML
    if output or preview:
        from omni_dash.dashboard.serializer import DashboardSerializer

        yaml_str = DashboardSerializer.to_yaml(defn)

        if preview:
            console.print()
            from rich.syntax import Syntax
            console.print(Syntax(yaml_str, "yaml", theme="monokai", line_numbers=True))

        if output:
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(yaml_str)
            console.print(f"\n[green]Saved to {output_path}[/green]")

    # Push to Omni
    if push:
        from omni_dash.api.client import OmniClient
        from omni_dash.api.documents import DocumentService
        from omni_dash.dashboard.serializer import DashboardSerializer

        settings.require_api()

        # Set the model ID for the API payload (guarded by check at line 62)
        if not omni_model_id:
            console.print("[red]Error:[/red] --omni-model-id is required when using --push.")
            raise typer.Exit(1)
        defn.model_id = omni_model_id

        payload = DashboardSerializer.to_omni_create_payload(defn)

        with OmniClient(
            api_key=settings.omni_api_key,
            base_url=settings.omni_base_url,
        ) as client:
            doc_service = DocumentService(client)
            result_doc = doc_service.create_dashboard(payload, folder_id=defn.folder_id)

        console.print(
            f"\n[green]Dashboard created in Omni![/green] "
            f"Document ID: [bold]{result_doc.document_id}[/bold]"
        )
