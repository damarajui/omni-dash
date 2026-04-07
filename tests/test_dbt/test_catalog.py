"""Tests for dbt.catalog — intelligent search and context block generation."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture()
def manifest_dir(tmp_path):
    """Create a minimal dbt project with manifest.json for testing."""
    target = tmp_path / "target"
    target.mkdir()

    manifest = {
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"},
        "nodes": {
            "model.project.fct_customer_daily_ts": {
                "resource_type": "model",
                "name": "fct_customer_daily_ts",
                "unique_id": "model.project.fct_customer_daily_ts",
                "description": "Daily ARR and activity metrics at the client level with credit consumption data",
                "path": "mart/fct_customer_daily_ts.sql",
                "schema": "PUBLIC",
                "database": "TRAINING_DATABASE",
                "config": {"materialized": "table"},
                "columns": {
                    "day_start": {"name": "day_start", "description": "Date for the daily metrics", "data_type": None},
                    "lindy_user_id": {"name": "lindy_user_id", "description": "User ID", "data_type": None},
                    "this_day_arr": {"name": "this_day_arr", "description": "ARR value for this day", "data_type": None},
                    "customer_type": {"name": "customer_type", "description": "Customer type (PLG or SLG)", "data_type": None},
                    "total_credits_consumed": {"name": "total_credits_consumed", "description": "Total credits consumed", "data_type": None},
                },
                "depends_on": {"nodes": ["model.project.int_client_level_daily_arr", "model.project.int_identities"]},
                "tags": ["revenue", "daily"],
                "raw_code": "SELECT ...",
                "meta": {},
            },
            "model.project.mart_seo_weekly_funnel": {
                "resource_type": "model",
                "name": "mart_seo_weekly_funnel",
                "unique_id": "model.project.mart_seo_weekly_funnel",
                "description": "Weekly organic search funnel from visits to ARR",
                "path": "mart/seo/mart_seo_weekly_funnel.sql",
                "schema": "PUBLIC",
                "database": "TRAINING_DATABASE",
                "config": {"materialized": "table"},
                "columns": {
                    "week_start": {"name": "week_start", "description": "Week start date", "data_type": None},
                    "organic_visits": {"name": "organic_visits", "description": "Organic visits", "data_type": None},
                    "signups": {"name": "signups", "description": "Signup count", "data_type": None},
                },
                "depends_on": {"nodes": ["model.project.stg_ga4__events"]},
                "tags": ["seo"],
                "raw_code": "SELECT ...",
                "meta": {},
            },
            "model.project.mart_daily_credits_revenue": {
                "resource_type": "model",
                "name": "mart_daily_credits_revenue",
                "unique_id": "model.project.mart_daily_credits_revenue",
                "description": "Daily revenue and credit usage",
                "path": "mart/mart_daily_credits_revenue.sql",
                "schema": "PUBLIC",
                "database": "TRAINING_DATABASE",
                "config": {"materialized": "table"},
                "columns": {
                    "day": {"name": "day", "description": "Date", "data_type": None},
                    "credits": {"name": "credits", "description": "Sum of credit usage", "data_type": None},
                    "dollars": {"name": "dollars", "description": "Sum of revenue", "data_type": None},
                },
                "depends_on": {"nodes": []},
                "tags": [],
                "raw_code": "SELECT ...",
                "meta": {},
            },
            "model.project.stg_mongo__identities": {
                "resource_type": "model",
                "name": "stg_mongo__identities",
                "unique_id": "model.project.stg_mongo__identities",
                "description": "Staging model for identities",
                "path": "staging/lindy/stg_mongo__identities.sql",
                "schema": "DBT_INAAN",
                "database": "DBT_DEV",
                "config": {"materialized": "view"},
                "columns": {},
                "depends_on": {"nodes": []},
                "tags": [],
                "raw_code": "SELECT ...",
                "meta": {},
            },
        },
        "sources": {},
        "parent_map": {},
        "child_map": {},
    }

    (target / "manifest.json").write_text(json.dumps(manifest))

    # Create a minimal dbt_project.yml so ModelRegistry is happy
    (tmp_path / "dbt_project.yml").write_text("name: test\nversion: 1.0.0\n")

    return tmp_path


def test_catalog_search_basic(manifest_dir):
    """Basic keyword search finds matching models."""
    from omni_dash.dbt.catalog import DbtCatalog

    catalog = DbtCatalog(project_path=str(manifest_dir))
    assert catalog.available

    results = catalog.search("revenue")
    names = [r["name"] for r in results]
    assert "mart_daily_credits_revenue" in names
    # fct_customer_daily_ts has "ARR" which is a synonym of revenue
    assert "fct_customer_daily_ts" in names


def test_catalog_search_synonym_expansion(manifest_dir):
    """Synonym expansion finds models that don't directly match the query."""
    from omni_dash.dbt.catalog import DbtCatalog

    catalog = DbtCatalog(project_path=str(manifest_dir))

    # "ARR by day split by user type" should find fct_customer_daily_ts
    # even though the query doesn't mention "fct_customer" directly
    results = catalog.search("ARR by day split by user type")
    names = [r["name"] for r in results]
    assert "fct_customer_daily_ts" in names
    # Should be top result (highest relevance)
    assert results[0]["name"] == "fct_customer_daily_ts"


def test_catalog_search_seo(manifest_dir):
    """SEO-related search finds the SEO funnel model."""
    from omni_dash.dbt.catalog import DbtCatalog

    catalog = DbtCatalog(project_path=str(manifest_dir))

    results = catalog.search("SEO traffic weekly")
    names = [r["name"] for r in results]
    assert "mart_seo_weekly_funnel" in names


def test_catalog_search_returns_columns(manifest_dir):
    """Search results include column summaries."""
    from omni_dash.dbt.catalog import DbtCatalog

    catalog = DbtCatalog(project_path=str(manifest_dir))

    results = catalog.search("daily revenue")
    assert len(results) > 0
    first = results[0]
    assert "columns" in first
    assert len(first["columns"]) > 0


def test_catalog_search_no_results(manifest_dir):
    """Search returns empty list for no matches."""
    from omni_dash.dbt.catalog import DbtCatalog

    catalog = DbtCatalog(project_path=str(manifest_dir))
    results = catalog.search("zzz_nonexistent_xyz_metric")
    assert results == []


def test_catalog_get_model(manifest_dir):
    """get_model returns full details for a specific model."""
    from omni_dash.dbt.catalog import DbtCatalog

    catalog = DbtCatalog(project_path=str(manifest_dir))
    result = catalog.get_model("fct_customer_daily_ts")

    assert result is not None
    assert result["name"] == "fct_customer_daily_ts"
    assert result["layer"] == "mart"
    assert result["materialization"] == "table"
    assert len(result["columns"]) == 5
    assert any(c["name"] == "this_day_arr" for c in result["columns"])
    assert len(result["upstream_refs"]) == 2


def test_catalog_get_model_not_found(manifest_dir):
    """get_model returns None for unknown model."""
    from omni_dash.dbt.catalog import DbtCatalog

    catalog = DbtCatalog(project_path=str(manifest_dir))
    result = catalog.get_model("nonexistent_model")
    assert result is None


def test_catalog_context_block(manifest_dir):
    """Context block includes mart models with descriptions."""
    from omni_dash.dbt.catalog import DbtCatalog

    catalog = DbtCatalog(project_path=str(manifest_dir))
    block = catalog.to_context_block()

    assert "## Available dbt Models" in block
    assert "fct_customer_daily_ts" in block
    assert "mart_seo_weekly_funnel" in block
    assert "mart_daily_credits_revenue" in block
    # Staging model should NOT be in the mart-layer block
    assert "stg_mongo__identities" not in block


def test_catalog_mart_layer_preference(manifest_dir):
    """Mart models score higher than staging models."""
    from omni_dash.dbt.catalog import DbtCatalog

    catalog = DbtCatalog(project_path=str(manifest_dir))

    results = catalog.search("identities")
    # stg model matches but mart models with columns about identity should score higher
    if len(results) > 1:
        # Mart models should come before staging
        mart_idx = next(
            (i for i, r in enumerate(results) if r["layer"] == "mart"), len(results)
        )
        stg_idx = next(
            (i for i, r in enumerate(results) if r["layer"] == "staging"), len(results)
        )
        if mart_idx < len(results) and stg_idx < len(results):
            assert mart_idx < stg_idx


def test_catalog_unavailable(monkeypatch):
    """Catalog gracefully handles missing manifest."""
    from omni_dash.dbt.catalog import DbtCatalog

    # Clear env vars that would provide fallback paths
    monkeypatch.delenv("DBT_MANIFEST_PATH", raising=False)
    monkeypatch.delenv("DBT_PROJECT_PATH", raising=False)

    catalog = DbtCatalog(manifest_path="/nonexistent/path")
    assert not catalog.available
    results = catalog.search("anything")
    assert len(results) == 1
    assert "error" in results[0]


def test_catalog_relevance_scores(manifest_dir):
    """Results include relevance scores and are sorted by them."""
    from omni_dash.dbt.catalog import DbtCatalog

    catalog = DbtCatalog(project_path=str(manifest_dir))

    results = catalog.search("daily revenue credits")
    scores = [r["relevance_score"] for r in results]
    assert scores == sorted(scores, reverse=True)
