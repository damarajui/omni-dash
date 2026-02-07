"""Shared test fixtures for omni-dash tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from omni_dash.config import reset_settings


@pytest.fixture(autouse=True)
def _reset_settings():
    """Reset settings singleton between tests."""
    reset_settings()
    yield
    reset_settings()


@pytest.fixture
def dbt_project_path() -> Path:
    """Path to the real dbt project (for integration-style tests)."""
    path = Path.home() / "dbt-1"
    if not path.exists():
        pytest.skip("dbt project not found at ~/dbt-1")
    return path


@pytest.fixture
def sample_manifest() -> dict:
    """Minimal manifest.json structure for unit tests."""
    return {
        "nodes": {
            "model.lindy.mart_seo_weekly_funnel": {
                "resource_type": "model",
                "name": "mart_seo_weekly_funnel",
                "unique_id": "model.lindy.mart_seo_weekly_funnel",
                "description": "Primary SEO Dashboard table. Wide weekly time series.",
                "database": "TRAINING_DATABASE",
                "schema": "PUBLIC",
                "path": "mart/seo/mart_seo_weekly_funnel.sql",
                "raw_code": 'SELECT * FROM {{ ref("int_ga4__seo_sessions") }}',
                "config": {
                    "materialized": "table",
                    "post-hook": [
                        "GRANT SELECT ON ALL TABLES IN SCHEMA TRAINING_DATABASE.PUBLIC TO APPLICATION OMNATA_SYNC_ENGINE"
                    ],
                },
                "columns": {
                    "week_start": {"name": "week_start", "description": "Monday of the week", "data_type": "DATE"},
                    "organic_visits_total": {"name": "organic_visits_total", "description": "Organic sessions", "data_type": "NUMBER"},
                    "organic_signups": {"name": "organic_signups", "description": "Signups from organic", "data_type": "NUMBER"},
                    "visit_to_signup_rate": {"name": "visit_to_signup_rate", "description": "Signup rate", "data_type": "FLOAT"},
                },
                "depends_on": {"nodes": ["model.lindy.int_ga4__seo_sessions"]},
                "tags": [],
                "meta": {},
            },
            "model.lindy.mart_monthly_paid_performance": {
                "resource_type": "model",
                "name": "mart_monthly_paid_performance",
                "unique_id": "model.lindy.mart_monthly_paid_performance",
                "description": "Monthly paid acquisition by channel.",
                "database": "TRAINING_DATABASE",
                "schema": "PUBLIC",
                "path": "mart/attribution/mart_monthly_paid_performance.sql",
                "raw_code": "SELECT * FROM ...",
                "config": {"materialized": "table", "post-hook": []},
                "columns": {
                    "month_start": {"name": "month_start", "description": "Month", "data_type": "DATE"},
                    "channel": {"name": "channel", "description": "Acquisition channel", "data_type": "VARCHAR"},
                    "signups": {"name": "signups", "description": "Total signups", "data_type": "NUMBER"},
                },
                "depends_on": {"nodes": []},
                "tags": [],
                "meta": {},
            },
            "model.lindy.stg_mongo__identities": {
                "resource_type": "model",
                "name": "stg_mongo__identities",
                "unique_id": "model.lindy.stg_mongo__identities",
                "description": "Staging identities.",
                "database": "TRAINING_DATABASE",
                "schema": "dbt_dev",
                "path": "staging/mongo/stg_mongo__identities.sql",
                "raw_code": "SELECT * FROM ...",
                "config": {"materialized": "view"},
                "columns": {},
                "depends_on": {"nodes": []},
                "tags": [],
                "meta": {},
            },
        }
    }


@pytest.fixture
def sample_schema_yml(tmp_path: Path) -> Path:
    """Create a temporary schema.yml file for testing."""
    models_dir = tmp_path / "models" / "mart" / "seo"
    models_dir.mkdir(parents=True)

    schema_content = """version: 2

models:
  - name: mart_seo_weekly_funnel
    description: >
      Primary SEO Dashboard table.
    columns:
      - name: week_start
        description: Monday of the week
        tests:
          - not_null
          - unique
      - name: organic_visits_total
        description: Organic search sessions from GA4
      - name: organic_signups
        description: Signups attributed to organic search
      - name: visit_to_signup_rate
        description: organic_signups / organic_visits_total
      - name: running_organic_plg_arr
        description: ARR for organic PLG customers
"""
    (models_dir / "schema.yml").write_text(schema_content)

    # Also need a dbt_project.yml at the root
    (tmp_path / "dbt_project.yml").write_text("name: test_project\nversion: '1.0'\n")

    return tmp_path
