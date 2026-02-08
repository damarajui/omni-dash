"""Tests for omni_dash.api.models — ModelService introspection."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from omni_dash.api.models import ModelService, OmniModel, TopicSummary
from omni_dash.exceptions import ModelNotFoundError


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
    def test_parses_list(self, service, mock_client):
        mock_client.get.return_value = [
            {"name": "topic1", "label": "Topic 1", "description": "desc"},
        ]
        result = service.list_topics("m1")
        assert len(result) == 1
        assert isinstance(result[0], TopicSummary)

    def test_empty(self, service, mock_client):
        mock_client.get.return_value = None
        assert service.list_topics("m1") == []


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
