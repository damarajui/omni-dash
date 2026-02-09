"""Tests for the query builder."""

import pytest

from omni_dash.api.queries import QueryBuilder


def test_basic_query():
    query = (
        QueryBuilder("model-123", "mart_seo_weekly_funnel")
        .fields(["week_start", "organic_visits_total"])
        .build()
    )
    assert query.model_id == "model-123"
    assert query.table == "mart_seo_weekly_funnel"
    assert "mart_seo_weekly_funnel.week_start" in query.fields
    assert "mart_seo_weekly_funnel.organic_visits_total" in query.fields


def test_qualified_fields_preserved():
    query = (
        QueryBuilder("m", "tbl")
        .fields(["tbl.col1", "col2"])
        .build()
    )
    assert "tbl.col1" in query.fields
    assert "tbl.col2" in query.fields


def test_sort():
    query = (
        QueryBuilder("m", "tbl")
        .fields(["col1"])
        .sort("col1", descending=True)
        .build()
    )
    assert len(query.sorts) == 1
    assert query.sorts[0]["column_name"] == "tbl.col1"
    assert query.sorts[0]["sort_descending"] is True


def test_filter():
    query = (
        QueryBuilder("m", "tbl")
        .fields(["col1"])
        .filter("col1", "greaterThan", 100)
        .build()
    )
    assert "tbl.col1" in query.filters
    assert query.filters["tbl.col1"]["operator"] == "greaterThan"


def test_limit():
    query = (
        QueryBuilder("m", "tbl")
        .fields(["col1"])
        .limit(50)
        .build()
    )
    assert query.limit == 50


def test_limit_validation():
    with pytest.raises(ValueError, match="positive"):
        QueryBuilder("m", "tbl").fields(["c"]).limit(0)


def test_no_fields_raises():
    with pytest.raises(ValueError, match="field"):
        QueryBuilder("m", "tbl").build()


def test_add_field():
    builder = QueryBuilder("m", "tbl")
    builder.add_field("col1")
    builder.add_field("col2")
    builder.add_field("col1")  # duplicate should be ignored
    query = builder.build()
    assert len(query.fields) == 2


def test_to_api_dict():
    payload = (
        QueryBuilder("model-123", "tbl")
        .fields(["col1", "col2"])
        .sort("col1")
        .limit(10)
        .to_api_dict()
    )
    assert payload["query"]["modelId"] == "model-123"
    assert payload["query"]["table"] == "tbl"
    assert payload["query"]["limit"] == 10
    assert len(payload["query"]["fields"]) == 2
    assert "sorts" in payload["query"]
