"""Tests for _sanitize_export_filters in server.py."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _mock_env(monkeypatch):
    monkeypatch.setenv("OMNI_API_KEY", "test-key")
    monkeypatch.setenv("OMNI_BASE_URL", "https://test.omniapp.co")
    monkeypatch.setenv("OMNI_SHARED_MODEL_ID", "test-model")


def test_sanitize_null_values_in_tile_filters():
    from omni_dash.mcp.server import _sanitize_export_filters

    export = {
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    {
                        "queryPresentation": {
                            "query": {
                                "queryJson": {
                                    "filters": {
                                        "table.col": {
                                            "kind": "EQUALS",
                                            "type": "string",
                                            "values": None,
                                        }
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        }
    }
    _sanitize_export_filters(export)
    filters = (
        export["dashboard"]["queryPresentationCollection"]
        ["queryPresentationCollectionMemberships"][0]
        ["queryPresentation"]["query"]["queryJson"]["filters"]
    )
    assert filters["table.col"]["values"] == []


def test_sanitize_null_values_in_dashboard_filter_config():
    from omni_dash.mcp.server import _sanitize_export_filters

    export = {
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [],
                "filterConfig": {
                    "abc123": {
                        "kind": "EQUALS",
                        "type": "string",
                        "values": None,
                        "fieldName": "table.col",
                    }
                },
            }
        }
    }
    _sanitize_export_filters(export)
    fc = export["dashboard"]["queryPresentationCollection"]["filterConfig"]
    assert fc["abc123"]["values"] == []


def test_sanitize_preserves_valid_values():
    from omni_dash.mcp.server import _sanitize_export_filters

    export = {
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    {
                        "queryPresentation": {
                            "query": {
                                "queryJson": {
                                    "filters": {
                                        "table.col": {
                                            "kind": "EQUALS",
                                            "type": "string",
                                            "values": ["foo", "bar"],
                                        }
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        }
    }
    _sanitize_export_filters(export)
    filters = (
        export["dashboard"]["queryPresentationCollection"]
        ["queryPresentationCollectionMemberships"][0]
        ["queryPresentation"]["query"]["queryJson"]["filters"]
    )
    assert filters["table.col"]["values"] == ["foo", "bar"]


def test_sanitize_handles_missing_keys():
    """Should not crash on exports with missing nested keys."""
    from omni_dash.mcp.server import _sanitize_export_filters

    # Minimal export with no filters
    export = {"dashboard": {"queryPresentationCollection": {}}}
    _sanitize_export_filters(export)  # Should not raise

    # Empty export
    _sanitize_export_filters({})  # Should not raise


def test_sanitize_handles_non_dict_filters():
    """Filters that aren't dicts (e.g., composite) should be left alone."""
    from omni_dash.mcp.server import _sanitize_export_filters

    export = {
        "dashboard": {
            "queryPresentationCollection": {
                "queryPresentationCollectionMemberships": [
                    {
                        "queryPresentation": {
                            "query": {
                                "queryJson": {
                                    "filters": {
                                        "table.col": "not_a_dict"
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        }
    }
    _sanitize_export_filters(export)  # Should not raise
