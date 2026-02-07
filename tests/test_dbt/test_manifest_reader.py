"""Tests for the manifest reader."""

import json

import pytest

from omni_dash.dbt.manifest_reader import ManifestReader
from omni_dash.exceptions import DbtMetadataError, DbtModelNotFoundError


def test_get_model(tmp_path, sample_manifest):
    """Test getting a model from a mock manifest."""
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(sample_manifest))
    (tmp_path / "dbt_project.yml").write_text("name: test\n")

    reader = ManifestReader(tmp_path)
    model = reader.get_model("mart_seo_weekly_funnel")

    assert model.name == "mart_seo_weekly_funnel"
    assert model.database == "TRAINING_DATABASE"
    assert model.schema_name == "PUBLIC"
    assert model.materialization == "table"
    assert model.has_omni_grant is True
    assert model.layer == "mart"
    assert len(model.columns) == 4
    assert "week_start" in model.column_names


def test_model_not_found(tmp_path, sample_manifest):
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(sample_manifest))

    reader = ManifestReader(tmp_path)
    with pytest.raises(DbtModelNotFoundError, match="nonexistent"):
        reader.get_model("nonexistent")


def test_list_models(tmp_path, sample_manifest):
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(sample_manifest))

    reader = ManifestReader(tmp_path)
    models = reader.list_models()
    assert len(models) == 3

    mart_models = reader.list_models(layer="mart")
    assert len(mart_models) == 2
    assert all(m.layer == "mart" for m in mart_models)


def test_search_models(tmp_path, sample_manifest):
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(sample_manifest))

    reader = ManifestReader(tmp_path)
    results = reader.search_models("seo")
    assert len(results) >= 1
    assert any(m.name == "mart_seo_weekly_funnel" for m in results)


def test_omni_grant_detection(tmp_path, sample_manifest):
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(sample_manifest))

    reader = ManifestReader(tmp_path)
    seo = reader.get_model("mart_seo_weekly_funnel")
    paid = reader.get_model("mart_monthly_paid_performance")

    assert seo.has_omni_grant is True
    assert paid.has_omni_grant is False


def test_missing_manifest(tmp_path):
    reader = ManifestReader(tmp_path)
    with pytest.raises(DbtMetadataError, match="manifest.json not found"):
        reader.get_model("anything")


def test_model_dependencies(tmp_path, sample_manifest):
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(sample_manifest))

    reader = ManifestReader(tmp_path)
    deps = reader.get_model_dependencies("mart_seo_weekly_funnel")
    assert "int_ga4__seo_sessions" in deps


def test_list_model_names(tmp_path, sample_manifest):
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps(sample_manifest))

    reader = ManifestReader(tmp_path)
    names = reader.list_model_names()
    assert "mart_seo_weekly_funnel" in names
    assert len(names) == 3

    mart_names = reader.list_model_names(layer="mart")
    assert len(mart_names) == 2
