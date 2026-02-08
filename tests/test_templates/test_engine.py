"""Tests for the template engine."""


import pytest

from omni_dash.exceptions import TemplateError
from omni_dash.templates.engine import TemplateEngine


@pytest.fixture
def engine():
    return TemplateEngine()


def test_list_templates(engine):
    templates = engine.list_templates()
    names = [t["name"] for t in templates]
    assert "weekly_funnel" in names
    assert "time_series_kpi" in names
    assert "channel_breakdown" in names
    assert "page_performance" in names


def test_get_template_meta(engine):
    meta = engine.get_template_meta("weekly_funnel")
    assert meta["name"] == "Weekly Funnel Dashboard"
    assert "funnel" in meta.get("tags", [])


def test_get_required_variables(engine):
    variables = engine.get_required_variables("weekly_funnel")
    assert "dashboard_name" in variables
    assert "omni_model_id" in variables
    assert "omni_table" in variables
    assert "metric_columns" in variables


def test_render_weekly_funnel(engine):
    definition = engine.render("weekly_funnel", {
        "dashboard_name": "SEO Funnel Test",
        "omni_model_id": "test-model-id",
        "omni_table": "mart_seo_weekly_funnel",
        "time_column": "week_start",
        "metric_columns": [
            "organic_visits_total",
            "organic_signups",
            "paywall_conversions",
            "running_organic_plg_arr",
        ],
        "rate_columns": ["visit_to_signup_rate"],
    })

    assert definition.name == "SEO Funnel Test"
    assert definition.source_template == "weekly_funnel"
    assert len(definition.tiles) == 4
    assert definition.tiles[0].chart_type == "area"

    # Verify fields are qualified
    for tile in definition.tiles:
        for field in tile.query.fields:
            assert "mart_seo_weekly_funnel." in field


def test_render_channel_breakdown(engine):
    definition = engine.render("channel_breakdown", {
        "dashboard_name": "Paid Performance",
        "omni_model_id": "test-id",
        "omni_table": "mart_monthly_paid_performance",
        "time_column": "month_start",
        "dimension_column": "channel",
        "metric_columns": ["signups", "spend"],
        "primary_metric": "signups",
    })

    assert definition.name == "Paid Performance"
    assert len(definition.tiles) == 2
    # First tile should be stacked bar
    assert definition.tiles[0].chart_type == "stacked_bar"


def test_render_missing_required_variable(engine):
    with pytest.raises(TemplateError, match="requires variable"):
        engine.render("weekly_funnel", {
            # Missing dashboard_name and others
            "omni_table": "test",
        })


def test_render_nonexistent_template(engine):
    with pytest.raises(TemplateError, match="not found"):
        engine.render("does_not_exist", {})


def test_custom_template_dir(tmp_path):
    """Test loading templates from a custom directory."""
    custom_template = tmp_path / "custom_dash.yml"
    custom_template.write_text("""meta:
  name: Custom Dashboard
  tags: [custom]
variables:
  dashboard_name:
    type: string
    required: true
dashboard:
  name: "{{ dashboard_name }}"
  model_id: "test"
  tiles:
    - name: Simple Chart
      chart_type: line
      query:
        table: test_table
        fields:
          - test_table.col1
          - test_table.col2
""")

    engine = TemplateEngine(template_dirs=[tmp_path])
    templates = engine.list_templates()
    names = [t["name"] for t in templates]
    assert "custom_dash" in names

    definition = engine.render("custom_dash", {"dashboard_name": "My Custom"})
    assert definition.name == "My Custom"
    assert len(definition.tiles) == 1
