"""Tests for omni_dash.api.models — ModelService introspection."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from omni_dash.api.models import ModelService, OmniModel, TopicDetail, TopicSummary
from omni_dash.exceptions import ModelNotFoundError, OmniAPIError


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def service(mock_client):
    return ModelService(mock_client)


class TestListModels:
    def test_parses_list(self, service, mock_client):
        mock_client.get.return_value = [
            {"id": "m1", "name": "Model 1", "database": "DB"},
        ]
        result = service.list_models()
        assert len(result) == 1
        assert isinstance(result[0], OmniModel)
        assert result[0].name == "Model 1"

    def test_parses_paginated_response(self, service, mock_client):
        """Omni API returns {pageInfo, records} for list endpoints."""
        mock_client.get.return_value = {
            "pageInfo": {"hasNextPage": False},
            "records": [{"id": "m1", "name": "Model 1"}],
        }
        result = service.list_models()
        assert len(result) == 1

    def test_empty_returns_empty(self, service, mock_client):
        mock_client.get.return_value = None
        assert service.list_models() == []


class TestGetModel:
    def test_returns_model(self, service, mock_client):
        mock_client.get.return_value = {"id": "m1", "name": "Test", "database": "DB"}
        result = service.get_model("m1")
        assert result.id == "m1"

    def test_not_found(self, service, mock_client):
        mock_client.get.return_value = None
        with pytest.raises(ModelNotFoundError):
            service.get_model("missing")


class TestFindModelForConnection:
    def test_finds_by_database(self, service, mock_client):
        mock_client.get.return_value = [
            {"id": "m1", "name": "M1", "database": "TRAINING_DATABASE", "schemaName": "PUBLIC"},
        ]
        result = service.find_model_for_connection("TRAINING_DATABASE")
        assert result.id == "m1"

    def test_case_insensitive(self, service, mock_client):
        mock_client.get.return_value = [
            {"id": "m1", "name": "M1", "database": "training_database"},
        ]
        result = service.find_model_for_connection("TRAINING_DATABASE")
        assert result.id == "m1"

    def test_not_found_raises(self, service, mock_client):
        mock_client.get.return_value = [
            {"id": "m1", "name": "M1", "database": "OTHER_DB"},
        ]
        with pytest.raises(ModelNotFoundError):
            service.find_model_for_connection("TRAINING_DATABASE")


class TestListTopics:
    """Tests for YAML-based topic listing."""

    def test_parses_topic_files(self, service, mock_client):
        mock_client.get.return_value = {
            "files": {
                "my_topic.topic": "base_view: my_view\nlabel: My Topic\n",
                "another.topic": "base_view: other_view\n",
                "PUBLIC/some_view.view": "schema: PUBLIC\ntable_name: SOME_TABLE\n",
            },
            "viewNames": {},
            "version": 1,
        }
        result = service.list_topics("m1")
        assert len(result) == 2
        assert isinstance(result[0], TopicSummary)
        # Sorted alphabetically
        assert result[0].name == "another"
        assert result[1].name == "my_topic"
        assert result[1].label == "My Topic"
        assert result[1].base_view == "my_view"

    def test_no_topic_files(self, service, mock_client):
        mock_client.get.return_value = {
            "files": {"PUBLIC/view.view": "schema: PUBLIC\n"},
            "viewNames": {},
            "version": 1,
        }
        assert service.list_topics("m1") == []

    def test_empty_yaml_response(self, service, mock_client):
        mock_client.get.return_value = {"files": {}, "viewNames": {}, "version": 1}
        assert service.list_topics("m1") == []

    def test_caches_yaml(self, service, mock_client):
        mock_client.get.return_value = {
            "files": {"t.topic": "base_view: v\n"},
            "viewNames": {},
            "version": 1,
        }
        service.list_topics("m1")
        service.list_topics("m1")  # Second call uses cache
        assert mock_client.get.call_count == 1


class TestGetTopic:
    """Tests for YAML-based topic detail."""

    def test_resolves_views_and_fields(self, service, mock_client):
        mock_client.get.return_value = {
            "files": {
                "my_topic.topic": "base_view: my_view\nlabel: My Topic\njoins: {}\n",
                "PUBLIC/my_view.view": (
                    "schema: PUBLIC\n"
                    "table_name: MY_TABLE\n"
                    "dimensions:\n"
                    "  col_a:\n"
                    "    sql: '\"COL_A\"'\n"
                    "  col_b:\n"
                    "    sql: '\"COL_B\"'\n"
                    "    label: Column B\n"
                    "measures:\n"
                    "  total:\n"
                    "    sql: '\"AMOUNT\"'\n"
                    "    aggregate_type: sum\n"
                ),
            },
            "viewNames": {},
            "version": 1,
        }
        result = service.get_topic("m1", "my_topic")
        assert isinstance(result, TopicDetail)
        assert result.name == "my_topic"
        assert result.label == "My Topic"
        assert result.base_view == "my_view"
        assert len(result.views) == 1
        assert result.views[0]["name"] == "my_view"

        # Should have 3 fields (2 dimensions + 1 measure)
        assert len(result.fields) == 3
        dims = [f for f in result.fields if f["type"] == "dimension"]
        measures = [f for f in result.fields if f["type"] == "measure"]
        assert len(dims) == 2
        assert len(measures) == 1
        assert measures[0]["qualified_name"] == "my_topic.total"

    def test_hidden_fields_excluded(self, service, mock_client):
        mock_client.get.return_value = {
            "files": {
                "t.topic": "base_view: v\njoins: {}\n",
                "v.view": (
                    "dimensions:\n"
                    "  visible:\n"
                    "    sql: x\n"
                    "  hidden_one:\n"
                    "    sql: y\n"
                    "    hidden: true\n"
                ),
            },
            "viewNames": {},
            "version": 1,
        }
        result = service.get_topic("m1", "t")
        assert len(result.fields) == 1
        assert result.fields[0]["name"] == "visible"

    def test_topic_not_found(self, service, mock_client):
        mock_client.get.return_value = {
            "files": {"other.topic": "base_view: v\n"},
            "viewNames": {},
            "version": 1,
        }
        with pytest.raises(OmniAPIError, match="Topic not found"):
            service.get_topic("m1", "nonexistent")

    def test_joined_views_included(self, service, mock_client):
        mock_client.get.return_value = {
            "files": {
                "t.topic": "base_view: base_v\njoins:\n  joined_v: {}\n",
                "base_v.view": "dimensions:\n  a:\n    sql: x\n",
                "PUBLIC/joined_v.view": "dimensions:\n  b:\n    sql: y\n",
            },
            "viewNames": {},
            "version": 1,
        }
        result = service.get_topic("m1", "t")
        assert len(result.views) == 2
        # Fields from both views
        field_names = [f["name"] for f in result.fields]
        assert "a" in field_names
        assert "b" in field_names


class TestListViews:
    def test_lists_views_from_yaml(self, service, mock_client):
        mock_client.get.return_value = {
            "files": {},
            "viewNames": {
                "PUBLIC/mart_seo.view": "mart_seo",
                "MONGO_LINDY/users.view": "mongo_lindy__users",
            },
            "version": 1,
        }
        result = service.list_views("m1")
        assert len(result) == 2
        # Sorted by name
        assert result[0]["name"] == "mart_seo"
        assert result[0]["schema"] == "PUBLIC"
        assert result[1]["name"] == "mongo_lindy__users"
        assert result[1]["schema"] == "MONGO_LINDY"


class TestFindViewForTable:
    def test_finds_view_by_name(self, service, mock_client):
        mock_client.get.return_value = {
            "files": {},
            "viewNames": {
                "PUBLIC/mart_seo.view": "mart_seo",
            },
            "version": 1,
        }
        result = service.find_view_for_table("m1", "mart_seo")
        assert result == "mart_seo"

    def test_returns_none_when_not_found(self, service, mock_client):
        mock_client.get.return_value = {
            "files": {},
            "viewNames": {"PUBLIC/other.view": "other"},
            "version": 1,
        }
        result = service.find_view_for_table("m1", "nonexistent")
        assert result is None


class TestCache:
    def test_cache_stores_and_retrieves(self, service):
        service._set_cache("key1", {"a": 1})
        assert service._get_cache("key1") == {"a": 1}

    def test_cache_miss(self, service):
        assert service._get_cache("missing") is None

    def test_cache_expires(self, service):
        service._cache_ttl = 0  # Expire immediately
        service._set_cache("key1", {"a": 1})
        service._cache_ts = time.monotonic() - 1  # Force expiry
        assert service._get_cache("key1") is None

    def test_clear_cache(self, service, tmp_path):
        service._set_cache("key", {"v": 1})
        cache_file = tmp_path / "cache.json"
        service.save_cache(cache_file)
        assert cache_file.exists()
        service.clear_cache(cache_file)
        assert not cache_file.exists()
        assert service._get_cache("key") is None

    def test_save_and_load_cache(self, service, tmp_path):
        service._set_cache("k", {"v": 42})
        f = tmp_path / "cache.json"
        service.save_cache(f)

        service2 = ModelService(MagicMock())
        service2.load_cache(f)
        assert service2._get_cache("k") == {"v": 42}

    def test_load_nonexistent_no_error(self, service, tmp_path):
        service.load_cache(tmp_path / "nope.json")  # Should not raise
