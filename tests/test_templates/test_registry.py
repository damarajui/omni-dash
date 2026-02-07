"""Tests for omni_dash.templates.registry — TemplateRegistry discovery."""

from __future__ import annotations

import pytest

from omni_dash.templates.registry import TemplateRegistry


@pytest.fixture
def registry():
    return TemplateRegistry()


class TestTemplateRegistry:
    def test_list_names(self, registry):
        names = registry.list_names()
        assert len(names) >= 4
        assert "weekly_funnel" in names
        assert "channel_breakdown" in names
        assert "time_series_kpi" in names
        assert "page_performance" in names

    def test_get_info_found(self, registry):
        info = registry.get_info("weekly_funnel")
        assert info is not None
        assert info["name"] == "weekly_funnel"
        assert "description" in info
        assert "variables" in info

    def test_get_info_not_found(self, registry):
        assert registry.get_info("nonexistent") is None

    def test_search_by_name(self, registry):
        results = registry.search("funnel")
        assert any(t["name"] == "weekly_funnel" for t in results)

    def test_search_by_tag(self, registry):
        results = registry.search("marketing")
        assert len(results) >= 1

    def test_search_no_results(self, registry):
        results = registry.search("zzz_nonexistent_keyword_zzz")
        assert results == []

    def test_get_required_variables(self, registry):
        variables = registry.get_required_variables("weekly_funnel")
        assert "dashboard_name" in variables
        assert "metric_columns" in variables

    def test_cache_invalidation(self, registry):
        _ = registry.templates  # populate cache
        assert registry._cache is not None
        registry.invalidate_cache()
        assert registry._cache is None
