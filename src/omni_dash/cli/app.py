"""Main CLI application for omni-dash."""

from __future__ import annotations

import typer

from omni_dash import __version__

app = typer.Typer(
    name="omni-dash",
    help="Programmatic Omni BI dashboard builder with dbt integration.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

# Subcommand groups
dbt_app = typer.Typer(
    name="dbt",
    help="Inspect dbt models and suggest dashboards.",
    no_args_is_help=True,
)
app.add_typer(dbt_app, name="dbt")


def version_callback(value: bool) -> None:
    if value:
        typer.echo(f"omni-dash {__version__}")
        raise typer.Exit()


@app.callback()
def main_callback(
    version: bool = typer.Option(
        False, "--version", "-v", help="Show version.", callback=version_callback, is_eager=True
    ),
) -> None:
    """omni-dash: Build Omni BI dashboards programmatically."""


# Import and register commands
from omni_dash.cli.create_cmd import create  # noqa: E402
from omni_dash.cli.export_cmd import export  # noqa: E402
from omni_dash.cli.import_cmd import import_dashboard  # noqa: E402
from omni_dash.cli.list_cmd import list_resources  # noqa: E402
from omni_dash.cli.preview_cmd import preview  # noqa: E402
from omni_dash.cli.dbt_cmd import models as dbt_models, inspect as dbt_inspect, suggest as dbt_suggest  # noqa: E402

app.command("create")(create)
app.command("export")(export)
app.command("import")(import_dashboard)
app.command("list")(list_resources)
app.command("preview")(preview)

dbt_app.command("models")(dbt_models)
dbt_app.command("inspect")(dbt_inspect)
dbt_app.command("suggest")(dbt_suggest)


def main() -> None:
    """Entry point for the CLI."""
    app()
