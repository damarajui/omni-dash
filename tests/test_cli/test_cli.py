"""Tests for the CLI commands (no API access required)."""

from __future__ import annotations

from typer.testing import CliRunner

from omni_dash.cli.app import app

runner = CliRunner()


def test_version_flag():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "omni-dash" in result.output
    assert "0.1.0" in result.output


def test_no_args_shows_help():
    result = runner.invoke(app, [])
    # Typer returns exit code 0 or 2 for help display
    assert result.exit_code in (0, 2)
    assert "omni-dash" in result.output.lower() or "Usage" in result.output


def test_list_templates():
    result = runner.invoke(app, ["list", "templates"])
    assert result.exit_code == 0
    assert "weekly_funnel" in result.output
    assert "time_series_kpi" in result.output
    assert "channel_breakdown" in result.output
    assert "page_performance" in result.output


def test_list_templates_json():
    result = runner.invoke(app, ["list", "templates", "--format", "json"])
    assert result.exit_code == 0
    # The JSON output should be parseable
    # Rich console may add escape sequences, so just check key text is present
    assert "weekly_funnel" in result.output


def test_list_invalid_resource():
    result = runner.invoke(app, ["list", "bananas"])
    assert result.exit_code == 1
    assert "Invalid resource" in result.output


def test_preview_template_summary():
    result = runner.invoke(app, [
        "preview",
        "--template", "weekly_funnel",
        "--var", "dashboard_name=Test Funnel",
        "--var", "omni_model_id=test-model-id",
        "--var", "omni_table=mart_seo_weekly_funnel",
        "--var", "time_column=week_start",
        "--var", 'metric_columns=["organic_visits", "signups", "conversions", "arr"]',
    ])
    assert result.exit_code == 0
    assert "Test Funnel" in result.output


def test_preview_template_yaml():
    result = runner.invoke(app, [
        "preview",
        "--template", "weekly_funnel",
        "--format", "yaml",
        "--var", "dashboard_name=YAML Preview",
        "--var", "omni_model_id=m-123",
        "--var", "omni_table=test_table",
        "--var", "time_column=week_start",
        "--var", 'metric_columns=["m1", "m2", "m3", "m4"]',
    ])
    assert result.exit_code == 0
    assert "YAML Preview" in result.output


def test_preview_template_json():
    result = runner.invoke(app, [
        "preview",
        "--template", "weekly_funnel",
        "--format", "json",
        "--var", "dashboard_name=JSON Preview",
        "--var", "omni_model_id=m-123",
        "--var", "omni_table=test_table",
        "--var", "time_column=week_start",
        "--var", 'metric_columns=["m1", "m2", "m3", "m4"]',
    ])
    assert result.exit_code == 0
    assert "JSON Preview" in result.output


def test_preview_from_file(tmp_path):
    dashboard_file = tmp_path / "test.yml"
    dashboard_file.write_text("""
dashboard:
  name: File Preview
  model_id: m-from-file
  tiles:
    - name: Test Tile
      chart_type: line
      query:
        table: my_table
        fields:
          - my_table.date
          - my_table.value
""")
    result = runner.invoke(app, ["preview", "--from-file", str(dashboard_file)])
    assert result.exit_code == 0
    assert "File Preview" in result.output


def test_preview_missing_file():
    result = runner.invoke(app, ["preview", "--from-file", "/nonexistent/file.yml"])
    assert result.exit_code == 1
    assert "not found" in result.output.lower() or "Error" in result.output


def test_preview_no_source():
    result = runner.invoke(app, ["preview"])
    assert result.exit_code == 1


def test_preview_nonexistent_template():
    result = runner.invoke(app, ["preview", "--template", "does_not_exist"])
    assert result.exit_code == 1
    assert "not found" in result.output.lower() or "Error" in result.output


def test_preview_missing_required_var():
    result = runner.invoke(app, [
        "preview",
        "--template", "weekly_funnel",
        "--var", "omni_table=test",
    ])
    assert result.exit_code == 1
    assert "requires variable" in result.output.lower() or "Error" in result.output


def test_create_no_source():
    result = runner.invoke(app, ["create"])
    assert result.exit_code == 1


def test_create_dry_run_template():
    result = runner.invoke(app, [
        "create",
        "--template", "time_series_kpi",
        "--dry-run",
        "--var", "dashboard_name=Dry Run Test",
        "--var", "omni_model_id=m-dry",
        "--var", "omni_table=test_table",
        "--var", "time_column=day_start",
        "--var", 'metric_columns=["metric1", "metric2"]',
    ])
    assert result.exit_code == 0
    assert "Dry Run Test" in result.output


def test_create_dry_run_from_file(tmp_path):
    dashboard_file = tmp_path / "dry.yml"
    dashboard_file.write_text("""
dashboard:
  name: From File Dry Run
  model_id: m-file
  tiles:
    - name: Tile
      chart_type: bar
      query:
        table: t
        fields:
          - t.a
          - t.b
""")
    result = runner.invoke(app, [
        "create",
        "--from-file", str(dashboard_file),
        "--dry-run",
    ])
    assert result.exit_code == 0
    assert "From File Dry Run" in result.output


def test_create_bad_var_format():
    result = runner.invoke(app, [
        "create",
        "--template", "weekly_funnel",
        "--var", "no_equals_sign",
    ])
    assert result.exit_code != 0
