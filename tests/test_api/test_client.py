"""Tests for omni_dash.api.client — OmniClient HTTP wrapper."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from omni_dash.exceptions import (
    AuthenticationError,
    DocumentNotFoundError,
    OmniAPIError,
    RateLimitError,
)


@pytest.fixture
def mock_settings():
    settings = MagicMock()
    settings.omni_api_key = "test-key-123"
    settings.omni_base_url = "https://test.omniapp.co"
    return settings


@pytest.fixture
def client(mock_settings):
    from omni_dash.api.client import OmniClient

    c = OmniClient(settings=mock_settings)
    yield c
    c.close()


class TestInit:
    def test_missing_api_key_raises(self, mock_settings):
        from omni_dash.api.client import OmniClient

        mock_settings.omni_api_key = ""
        with pytest.raises(AuthenticationError, match="OMNI_API_KEY"):
            OmniClient(settings=mock_settings)

    def test_missing_base_url_raises(self, mock_settings):
        from omni_dash.api.client import OmniClient

        mock_settings.omni_base_url = ""
        with pytest.raises(AuthenticationError, match="OMNI_BASE_URL"):
            OmniClient(settings=mock_settings)


class TestContextManager:
    def test_enter_exit(self, mock_settings):
        from omni_dash.api.client import OmniClient

        with OmniClient(settings=mock_settings) as c:
            assert c is not None


class TestRequest:
    def test_successful_get(self, client):
        response = MagicMock()
        response.status_code = 200
        response.text = '{"ok": true}'
        response.json.return_value = {"ok": True}

        with patch.object(client._http, "request", return_value=response):
            result = client.get("/api/v1/models")
        assert result == {"ok": True}

    def test_204_returns_none(self, client):
        response = MagicMock()
        response.status_code = 204
        response.text = ""

        with patch.object(client._http, "request", return_value=response):
            result = client.delete("/api/v1/documents/abc")
        assert result is None

    def test_401_raises_auth_error(self, client):
        response = MagicMock()
        response.status_code = 401
        response.text = "Unauthorized"

        with patch.object(client._http, "request", return_value=response):
            with pytest.raises(AuthenticationError):
                client.get("/api/v1/models")

    def test_404_raises_not_found(self, client):
        response = MagicMock()
        response.status_code = 404
        response.text = "Not Found"

        with patch.object(client._http, "request", return_value=response):
            with pytest.raises(DocumentNotFoundError):
                client.get("/api/v1/documents/missing-id")

    def test_400_raises_api_error(self, client):
        response = MagicMock()
        response.status_code = 400
        response.text = "Bad Request"

        with patch.object(client._http, "request", return_value=response):
            with pytest.raises(OmniAPIError) as exc_info:
                client.post("/api/v1/documents", json={})
        assert exc_info.value.status_code == 400

    def test_rate_limit_acquired(self, client):
        """Rate limiter must be acquired before each request."""
        response = MagicMock()
        response.status_code = 200
        response.text = '{"ok": true}'
        response.json.return_value = {"ok": True}

        with patch.object(client._http, "request", return_value=response):
            with patch.object(client._rate_limiter, "acquire", return_value=True) as acq:
                client.get("/test")
                acq.assert_called()

    def test_rate_limit_exhausted_raises(self, client):
        with patch.object(client._rate_limiter, "acquire", return_value=False):
            with pytest.raises(RateLimitError):
                client.get("/test")

    def test_empty_response_returns_none(self, client):
        response = MagicMock()
        response.status_code = 200
        response.text = ""

        with patch.object(client._http, "request", return_value=response):
            result = client.get("/test")
        assert result is None


class TestGetRaw:
    def test_returns_bytes(self, client):
        response = MagicMock()
        response.status_code = 200
        response.content = b"%PDF-1.4..."

        with patch.object(client._http, "request", return_value=response):
            result = client.get_raw("/download")
        assert result == b"%PDF-1.4..."

    def test_404_raises(self, client):
        response = MagicMock()
        response.status_code = 404
        response.text = "Not Found"

        with patch.object(client._http, "request", return_value=response):
            with pytest.raises(DocumentNotFoundError):
                client.get_raw("/api/v1/dashboards/xxx/download")


class TestPing:
    def test_ping_success(self, client):
        with patch.object(client, "get", return_value=[{"id": "m1"}]):
            assert client.ping() is True

    def test_ping_failure(self, client):
        with patch.object(client, "get", side_effect=OmniAPIError(500, "err")):
            assert client.ping() is False
