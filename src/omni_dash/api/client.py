"""Core HTTP client for the Omni REST API.

Wraps the official omni-python-sdk where possible and falls back to
direct httpx calls for endpoints the SDK doesn't cover.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from omni_dash.api.rate_limiter import RateLimiter
from omni_dash.config import OmniDashSettings, get_settings
from omni_dash.exceptions import (
    AuthenticationError,
    DocumentNotFoundError,
    OmniAPIError,
    RateLimitError,
)

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0  # seconds
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class OmniClient:
    """Unified client for the Omni API.

    Combines the official Python SDK with direct HTTP calls for endpoints
    the SDK doesn't cover. All requests go through a token-bucket rate
    limiter and include automatic retry with exponential backoff.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        settings: OmniDashSettings | None = None,
    ):
        s = settings or get_settings()
        self._api_key = api_key or s.omni_api_key
        self._base_url = (base_url or s.omni_base_url).rstrip("/")

        if not self._api_key:
            raise AuthenticationError("OMNI_API_KEY is required")
        if not self._base_url:
            raise AuthenticationError("OMNI_BASE_URL is required")

        self._rate_limiter = RateLimiter(max_tokens=60, refill_rate=1.0)
        self._http = httpx.Client(
            base_url=self._base_url,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
                "User-Agent": "omni-dash/0.1.0",
            },
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
        self._sdk = None  # Lazy-loaded omni-python-sdk OmniAPI

    @property
    def sdk(self):
        """Lazy-initialize the official Omni Python SDK client."""
        if self._sdk is None:
            try:
                from omni_python_sdk import OmniAPI

                self._sdk = OmniAPI(api_key=self._api_key, base_url=self._base_url)
            except ImportError:
                raise OmniAPIError(
                    0,
                    "omni-python-sdk is not installed. Run: uv add omni-python-sdk",
                )
        return self._sdk

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        params: dict | None = None,
        timeout: float | None = None,
    ) -> dict | list | None:
        """Make an authenticated, rate-limited HTTP request with retry.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, PATCH).
            path: API path (e.g., "/api/v1/documents").
            json: Request body.
            params: Query parameters.
            timeout: Per-request timeout override.

        Returns:
            Parsed JSON response body, or None for 204 responses.

        Raises:
            OmniAPIError: On non-retryable API errors.
            RateLimitError: If rate limit is exhausted after retries.
            AuthenticationError: On 401/403 responses.
        """
        last_error: OmniAPIError | None = None

        for attempt in range(MAX_RETRIES + 1):
            # Acquire rate limit token
            if not self._rate_limiter.acquire(timeout=30.0):
                raise RateLimitError()

            try:
                kwargs: dict[str, Any] = {"params": params}
                if json is not None:
                    kwargs["json"] = json
                if timeout is not None:
                    kwargs["timeout"] = timeout

                response = self._http.request(method, path, **kwargs)

                if response.status_code == 204:
                    return None

                if response.status_code in (401, 403):
                    raise AuthenticationError(response.text)

                if response.status_code == 404:
                    # Extract document_id from path if applicable
                    parts = path.split("/")
                    doc_id = parts[-1] if len(parts) > 2 else path
                    raise DocumentNotFoundError(doc_id)

                if response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", "2"))
                    last_error = RateLimitError(retry_after=retry_after)
                    logger.warning(
                        "Rate limited (attempt %d/%d), waiting %.1fs",
                        attempt + 1,
                        MAX_RETRIES + 1,
                        retry_after,
                    )
                    time.sleep(retry_after)
                    continue

                if response.status_code >= 500 and attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    last_error = OmniAPIError(
                        response.status_code, response.text, response.text
                    )
                    logger.warning(
                        "Server error %d (attempt %d/%d), retrying in %.1fs",
                        response.status_code,
                        attempt + 1,
                        MAX_RETRIES + 1,
                        wait,
                    )
                    time.sleep(wait)
                    continue

                if response.status_code >= 400:
                    raise OmniAPIError(
                        response.status_code, response.text, response.text
                    )

                if not response.text:
                    return None

                return response.json()

            except httpx.TimeoutException as e:
                last_error = OmniAPIError(0, f"Request timed out: {e}")
                if attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    logger.warning("Timeout (attempt %d/%d), retrying in %.1fs", attempt + 1, MAX_RETRIES + 1, wait)
                    time.sleep(wait)
                    continue
                raise last_error from e

            except httpx.RequestError as e:
                last_error = OmniAPIError(0, f"Connection error: {e}")
                if attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    logger.warning("Connection error (attempt %d/%d), retrying in %.1fs", attempt + 1, MAX_RETRIES + 1, wait)
                    time.sleep(wait)
                    continue
                raise last_error from e

        if last_error:
            raise last_error
        raise OmniAPIError(0, "Unexpected retry exhaustion")

    # -- Convenience HTTP methods --

    def get(self, path: str, **kwargs: Any) -> dict | list | None:
        return self._request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> dict | list | None:
        return self._request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> dict | list | None:
        return self._request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> dict | list | None:
        return self._request("DELETE", path, **kwargs)

    def patch(self, path: str, **kwargs: Any) -> dict | list | None:
        return self._request("PATCH", path, **kwargs)

    # -- Health check --

    def ping(self) -> bool:
        """Test API connectivity by listing models."""
        try:
            result = self.get("/api/v1/models")
            return result is not None
        except OmniAPIError:
            return False

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()

    def __enter__(self) -> OmniClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
