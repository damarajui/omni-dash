"""Tests for the unified model registry."""

import json

from omni_dash.dbt.model_registry import ModelRegistry


def test_merged_model(tmp_path, sample_manifest):
    """Test that manifest + schema.yml are merged correctly."""
    # Write manifest
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(sample_manifest))

    # Write schema.yml with extra column
    models_dir = tmp_path / "models" / "mart" / "seo"
    models_dir.mkdir(parents=True)
    (models_dir / "schema.yml").write_text("""version: 2
models:
  - name: mart_seo_weekly_funnel
    description: Better description from schema.yml
    columns:
      - name: week_start
        description: Monday of the week (better docs)
        tests:
          - not_null
          - unique
      - name: organic_visits_total
        description: Organic search sessions from GA4
      - name: extra_column
        description: Only in schema.yml
""")
    (tmp_path / "dbt_project.yml").write_text("name: test\n")

    registry = ModelRegistry(tmp_path)
    model = registry.get_model("mart_seo_weekly_funnel")

    # Schema.yml description wins
    assert "Better description" in model.description

    # Columns merged: manifest has 4, schema has 3, unique union
    col_names = [c.name for c in model.columns]
    assert "week_start" in col_names
    assert "organic_visits_total" in col_names
    assert "extra_column" in col_names
    assert "visit_to_signup_rate" in col_names  # Only in manifest

    # Schema.yml descriptions win
    week = next(c for c in model.columns if c.name == "week_start")
    assert "better docs" in week.description.lower()


def test_list_mart_models(tmp_path, sample_manifest):
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(sample_manifest))
    (tmp_path / "dbt_project.yml").write_text("name: test\n")

    registry = ModelRegistry(tmp_path)
    marts = registry.list_mart_models()
    assert len(marts) == 2
    assert all(m.layer == "mart" for m in marts)


def test_search_models(tmp_path, sample_manifest):
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(sample_manifest))
    (tmp_path / "dbt_project.yml").write_text("name: test\n")

    registry = ModelRegistry(tmp_path)
    results = registry.search_models("paid")
    assert any(m.name == "mart_monthly_paid_performance" for m in results)


def test_suggest_dashboard_models(tmp_path, sample_manifest):
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(sample_manifest))
    (tmp_path / "dbt_project.yml").write_text("name: test\n")

    registry = ModelRegistry(tmp_path)
    suggestions = registry.suggest_dashboard_models()
    # mart_seo_weekly_funnel has documented columns and Omni grant
    names = [m.name for m in suggestions]
    assert "mart_seo_weekly_funnel" in names


def test_list_omni_eligible(tmp_path, sample_manifest):
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(sample_manifest))
    (tmp_path / "dbt_project.yml").write_text("name: test\n")

    registry = ModelRegistry(tmp_path)
    eligible = registry.list_omni_eligible_models()
    assert len(eligible) == 1
    assert eligible[0].name == "mart_seo_weekly_funnel"
