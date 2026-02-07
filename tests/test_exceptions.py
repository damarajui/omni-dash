"""Tests for omni_dash.exceptions — exception hierarchy and formatting."""

from __future__ import annotations

import pytest

from omni_dash.exceptions import (
    AuthenticationError,
    CacheError,
    ConfigurationError,
    DashboardDefinitionError,
    DbtMetadataError,
    DbtModelNotFoundError,
    DocumentNotFoundError,
    ModelNotFoundError,
    OmniAPIError,
    OmniDashError,
    RateLimitError,
    TemplateError,
    TemplateValidationError,
)


class TestHierarchy:
    def test_all_inherit_from_base(self):
        assert issubclass(OmniAPIError, OmniDashError)
        assert issubclass(RateLimitError, OmniAPIError)
        assert issubclass(AuthenticationError, OmniAPIError)
        assert issubclass(DocumentNotFoundError, OmniAPIError)
        assert issubclass(ModelNotFoundError, OmniDashError)
        assert issubclass(TemplateError, OmniDashError)
        assert issubclass(TemplateValidationError, TemplateError)
        assert issubclass(DbtMetadataError, OmniDashError)
        assert issubclass(DbtModelNotFoundError, DbtMetadataError)
        assert issubclass(DashboardDefinitionError, OmniDashError)
        assert issubclass(ConfigurationError, OmniDashError)
        assert issubclass(CacheError, OmniDashError)


class TestOmniAPIError:
    def test_stores_status_code(self):
        e = OmniAPIError(400, "Bad Request", "body")
        assert e.status_code == 400
        assert e.response_body == "body"
        assert "400" in str(e)


class TestRateLimitError:
    def test_default(self):
        e = RateLimitError()
        assert e.status_code == 429
        assert e.retry_after is None

    def test_with_retry_after(self):
        e = RateLimitError(retry_after=2.5)
        assert e.retry_after == 2.5


class TestDocumentNotFoundError:
    def test_stores_id(self):
        e = DocumentNotFoundError("doc-123")
        assert e.document_id == "doc-123"
        assert "doc-123" in str(e)


class TestDbtModelNotFoundError:
    def test_basic(self):
        e = DbtModelNotFoundError("my_model")
        assert "my_model" in str(e)

    def test_suggests_similar(self):
        e = DbtModelNotFoundError("mart_seo_funnel", ["mart_seo_weekly_funnel", "mart_paid"])
        assert "mart_seo_weekly_funnel" in str(e)


class TestTemplateValidationError:
    def test_formats_errors(self):
        e = TemplateValidationError("weekly_funnel", ["Missing x", "Missing y"])
        assert "weekly_funnel" in str(e)
        assert "Missing x" in str(e)
        assert "Missing y" in str(e)
