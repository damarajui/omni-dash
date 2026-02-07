"""Tests for the schema reader."""

from omni_dash.dbt.schema_reader import SchemaReader


def test_list_documented_models(sample_schema_yml):
    reader = SchemaReader(sample_schema_yml)
    models = reader.list_documented_models()
    assert "mart_seo_weekly_funnel" in models


def test_get_model_schema(sample_schema_yml):
    reader = SchemaReader(sample_schema_yml)
    entry = reader.get_model_schema("mart_seo_weekly_funnel")
    assert entry is not None
    assert entry.name == "mart_seo_weekly_funnel"
    assert "SEO Dashboard" in entry.description


def test_get_column_docs(sample_schema_yml):
    reader = SchemaReader(sample_schema_yml)
    cols = reader.get_column_docs("mart_seo_weekly_funnel")
    assert len(cols) == 5

    col_names = [c.name for c in cols]
    assert "week_start" in col_names
    assert "organic_visits_total" in col_names
    assert "running_organic_plg_arr" in col_names


def test_column_tests_parsed(sample_schema_yml):
    reader = SchemaReader(sample_schema_yml)
    cols = reader.get_column_docs("mart_seo_weekly_funnel")
    week_start = next(c for c in cols if c.name == "week_start")
    assert "not_null" in week_start.tests
    assert "unique" in week_start.tests


def test_missing_model_returns_none(sample_schema_yml):
    reader = SchemaReader(sample_schema_yml)
    assert reader.get_model_schema("nonexistent") is None


def test_get_model_description(sample_schema_yml):
    reader = SchemaReader(sample_schema_yml)
    desc = reader.get_model_description("mart_seo_weekly_funnel")
    assert "SEO Dashboard" in desc


def test_search_by_column(sample_schema_yml):
    reader = SchemaReader(sample_schema_yml)
    models = reader.search_by_column("week_start")
    assert "mart_seo_weekly_funnel" in models


def test_get_all_column_docs(sample_schema_yml):
    reader = SchemaReader(sample_schema_yml)
    all_docs = reader.get_all_column_docs()
    assert "mart_seo_weekly_funnel" in all_docs
    assert len(all_docs["mart_seo_weekly_funnel"]) == 5


def test_invalidate_cache(sample_schema_yml):
    reader = SchemaReader(sample_schema_yml)
    reader.list_documented_models()  # Populate cache
    reader.invalidate_cache()
    # Should reload fine
    models = reader.list_documented_models()
    assert "mart_seo_weekly_funnel" in models
