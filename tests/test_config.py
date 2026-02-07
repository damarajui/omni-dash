"""Tests for omni_dash.config — settings, validation, and singleton."""

from __future__ import annotations

from pathlib import Path

import pytest

from omni_dash.config import OmniDashSettings, get_settings, reset_settings
from omni_dash.exceptions import ConfigurationError


class TestNormalizeBaseUrl:
    def test_empty_string_unchanged(self):
        s = OmniDashSettings(omni_base_url="")
        assert s.omni_base_url == ""

    def test_strips_trailing_slash(self):
        s = OmniDashSettings(omni_base_url="https://acme.omniapp.co/")
        assert s.omni_base_url == "https://acme.omniapp.co"

    def test_adds_https_prefix(self):
        s = OmniDashSettings(omni_base_url="acme.omniapp.co")
        assert s.omni_base_url == "https://acme.omniapp.co"

    def test_http_preserved(self):
        s = OmniDashSettings(omni_base_url="http://localhost:3000")
        assert s.omni_base_url == "http://localhost:3000"

    def test_strips_slash_and_adds_https(self):
        s = OmniDashSettings(omni_base_url="acme.omniapp.co/")
        assert s.omni_base_url == "https://acme.omniapp.co"


class TestRequireApi:
    def test_raises_when_no_api_key(self):
        s = OmniDashSettings(omni_api_key="", omni_base_url="https://x.co")
        with pytest.raises(ConfigurationError, match="OMNI_API_KEY"):
            s.require_api()

    def test_raises_when_no_base_url(self):
        s = OmniDashSettings(omni_api_key="key-123", omni_base_url="")
        with pytest.raises(ConfigurationError, match="OMNI_BASE_URL"):
            s.require_api()

    def test_passes_when_both_set(self):
        s = OmniDashSettings(omni_api_key="key", omni_base_url="https://x.co")
        s.require_api()  # should not raise


class TestRequireDbt:
    def test_raises_when_no_path(self):
        s = OmniDashSettings(dbt_project_path="")
        with pytest.raises(ConfigurationError, match="DBT_PROJECT_PATH"):
            s.require_dbt()

    def test_raises_when_path_missing(self, tmp_path: Path):
        s = OmniDashSettings(dbt_project_path=str(tmp_path / "nonexistent"))
        with pytest.raises(ConfigurationError, match="does not exist"):
            s.require_dbt()

    def test_returns_path_when_valid(self, tmp_path: Path):
        (tmp_path / "dbt_project.yml").write_text("name: test\n")
        s = OmniDashSettings(dbt_project_path=str(tmp_path))
        result = s.require_dbt()
        assert result == tmp_path


class TestProperties:
    def test_dbt_path_none_when_empty(self):
        s = OmniDashSettings(dbt_project_path="")
        assert s.dbt_path is None

    def test_dbt_path_returns_path(self, tmp_path: Path):
        (tmp_path / "dbt_project.yml").write_text("name: test\n")
        s = OmniDashSettings(dbt_project_path=str(tmp_path))
        assert s.dbt_path == tmp_path

    def test_template_dirs_empty(self):
        s = OmniDashSettings(omni_dash_template_dirs="")
        assert s.template_dirs == []

    def test_template_dirs_parses_csv(self, tmp_path: Path):
        d1 = tmp_path / "a"
        d2 = tmp_path / "b"
        s = OmniDashSettings(omni_dash_template_dirs=f"{d1}, {d2}")
        assert len(s.template_dirs) == 2
        assert s.template_dirs[0] == d1
        assert s.template_dirs[1] == d2

    def test_api_configured_true(self):
        s = OmniDashSettings(omni_api_key="k", omni_base_url="https://x.co")
        assert s.api_configured is True

    def test_api_configured_false_no_key(self):
        s = OmniDashSettings(omni_api_key="", omni_base_url="https://x.co")
        assert s.api_configured is False

    def test_api_configured_false_no_url(self):
        s = OmniDashSettings(omni_api_key="k", omni_base_url="")
        assert s.api_configured is False


class TestGetSettings:
    def test_returns_settings(self, monkeypatch):
        monkeypatch.setenv("OMNI_API_KEY", "test-key")
        monkeypatch.setenv("OMNI_BASE_URL", "https://test.omniapp.co")
        reset_settings()
        s = get_settings()
        assert s.omni_api_key == "test-key"
        assert s.omni_base_url == "https://test.omniapp.co"

    def test_singleton_caches(self, monkeypatch):
        monkeypatch.setenv("OMNI_API_KEY", "k1")
        reset_settings()
        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2

    def test_overrides_bypass_cache(self, monkeypatch):
        monkeypatch.setenv("OMNI_API_KEY", "k1")
        reset_settings()
        s1 = get_settings()
        s2 = get_settings(omni_api_key="k2")
        assert s2.omni_api_key == "k2"
        assert s1 is not s2
