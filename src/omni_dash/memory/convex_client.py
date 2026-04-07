"""Thin Convex HTTP API client for Python.

Uses httpx (already a dependency) to call Convex queries, mutations,
and actions via the HTTP API. No additional pip packages required.

Auth: Uses CONVEX_DEPLOY_KEY for server-to-server calls.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 10.0  # seconds


class ConvexHTTPClient:
    """Lightweight Convex HTTP API client.

    Args:
        url: Convex deployment URL (e.g. "https://abc-123.convex.cloud").
            Falls back to ``CONVEX_URL`` env var.
        deploy_key: Convex deploy key for server auth.
            Falls back to ``CONVEX_DEPLOY_KEY`` env var.
    """

    def __init__(
        self,
        url: str | None = None,
        deploy_key: str | None = None,
    ) -> None:
        self._url = (url or os.environ.get("CONVEX_URL", "")).rstrip("/")
        self._deploy_key = deploy_key or os.environ.get("CONVEX_DEPLOY_KEY", "")
        self._http = httpx.Client(timeout=_DEFAULT_TIMEOUT)

    @property
    def configured(self) -> bool:
        return bool(self._url and self._deploy_key)

    def _headers(self) -> dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self._deploy_key:
            h["Authorization"] = f"Convex {self._deploy_key}"
        return h

    def _call(
        self, endpoint: str, path: str, args: dict[str, Any]
    ) -> Any:
        """Call a Convex function via HTTP API.

        Args:
            endpoint: "query", "mutation", or "action".
            path: Function path (e.g. "learnings:search").
            args: Named arguments dict.

        Returns:
            The function result value, or None on error.
        """
        if not self.configured:
            logger.debug("Convex not configured, skipping %s:%s", endpoint, path)
            return None

        url = f"{self._url}/api/{endpoint}"
        body = {"path": path, "args": args, "format": "json"}

        try:
            resp = self._http.post(url, json=body, headers=self._headers())
            data = resp.json()

            if data.get("status") == "success":
                return data.get("value")

            error_msg = data.get("errorMessage", "unknown error")
            logger.warning("Convex %s %s failed: %s", endpoint, path, error_msg)
            return None

        except httpx.TimeoutException:
            logger.warning("Convex %s %s timed out", endpoint, path)
            return None
        except Exception as e:
            logger.warning("Convex %s %s error: %s", endpoint, path, e)
            return None

    def query(self, path: str, args: dict[str, Any] | None = None) -> Any:
        """Call a Convex query function."""
        return self._call("query", path, args or {})

    def mutation(self, path: str, args: dict[str, Any] | None = None) -> Any:
        """Call a Convex mutation function."""
        return self._call("mutation", path, args or {})

    def action(self, path: str, args: dict[str, Any] | None = None) -> Any:
        """Call a Convex action function."""
        return self._call("action", path, args or {})


# Module-level singleton
_client: ConvexHTTPClient | None = None


def get_convex_client() -> ConvexHTTPClient:
    """Get or create the Convex client singleton."""
    global _client
    if _client is None:
        _client = ConvexHTTPClient()
    return _client
