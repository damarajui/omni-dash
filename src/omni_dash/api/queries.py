"""Query building and execution against the Omni API."""

from __future__ import annotations

import base64
import logging
from typing import Any

from pydantic import BaseModel, Field

from omni_dash.api.client import OmniClient
from omni_dash.exceptions import OmniAPIError

logger = logging.getLogger(__name__)


class QuerySpec(BaseModel):
    """Validated query specification ready for API submission."""

    model_id: str
    table: str
    fields: list[str]
    sorts: list[dict[str, Any]] = Field(default_factory=list)
    filters: dict[str, Any] = Field(default_factory=dict)
    limit: int = 200
    pivots: list[str] = Field(default_factory=list)


class QueryResult(BaseModel):
    """Parsed query result."""

    fields: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    truncated: bool = False

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return rows as list of dicts."""
        return self.rows


class QueryBuilder:
    """Fluent builder for constructing Omni query specifications.

    Usage:
        query = (
            QueryBuilder("model-uuid", "mart_seo_weekly_funnel")
            .fields(["week_start", "organic_visits_total"])
            .sort("week_start")
            .limit(100)
            .build()
        )
    """

    def __init__(self, model_id: str, table: str):
        self._model_id = model_id
        self._table = table
        self._fields: list[str] = []
        self._sorts: list[dict[str, Any]] = []
        self._filters: dict[str, Any] = {}
        self._limit: int = 200
        self._pivots: list[str] = []

    def fields(self, field_names: list[str]) -> QueryBuilder:
        """Set the fields to query.

        Field names can be bare ("week_start") or qualified
        ("mart_seo_weekly_funnel.week_start"). Bare names are
        auto-qualified with the table name.
        """
        self._fields = [
            f if "." in f else f"{self._table}.{f}" for f in field_names
        ]
        return self

    def add_field(self, field_name: str) -> QueryBuilder:
        """Add a single field to the query."""
        qualified = field_name if "." in field_name else f"{self._table}.{field_name}"
        if qualified not in self._fields:
            self._fields.append(qualified)
        return self

    def sort(
        self, column: str, *, descending: bool = False
    ) -> QueryBuilder:
        """Add a sort clause."""
        qualified = column if "." in column else f"{self._table}.{column}"
        self._sorts.append(
            {"column_name": qualified, "sort_descending": descending}
        )
        return self

    def filter(
        self, column: str, operator: str, value: Any
    ) -> QueryBuilder:
        """Add a filter clause.

        Args:
            column: Field name (auto-qualified if bare).
            operator: Filter operator (e.g., "is", "isNot", "greaterThan", "contains").
            value: Filter value.
        """
        qualified = column if "." in column else f"{self._table}.{column}"
        self._filters[qualified] = {"operator": operator, "value": value}
        return self

    def limit(self, n: int) -> QueryBuilder:
        """Set the row limit."""
        if n < 1:
            raise ValueError(f"Limit must be positive, got {n}")
        self._limit = n
        return self

    def pivot(self, field: str) -> QueryBuilder:
        """Add a pivot field."""
        qualified = field if "." in field else f"{self._table}.{field}"
        if qualified not in self._pivots:
            self._pivots.append(qualified)
        return self

    def build(self) -> QuerySpec:
        """Build and validate the query specification."""
        if not self._fields:
            raise ValueError("At least one field is required")

        return QuerySpec(
            model_id=self._model_id,
            table=self._table,
            fields=self._fields,
            sorts=self._sorts,
            filters=self._filters,
            limit=self._limit,
            pivots=self._pivots,
        )

    def to_api_dict(self) -> dict[str, Any]:
        """Build and return the raw API payload dict."""
        return _spec_to_payload(self.build())


def _spec_to_payload(query: QuerySpec) -> dict[str, Any]:
    """Convert a QuerySpec to an Omni API payload dict.

    Single source of truth for payload assembly — used by QueryBuilder,
    QueryRunner.run(), and QueryRunner.run_blocking().
    """
    payload: dict[str, Any] = {
        "query": {
            "modelId": query.model_id,
            "table": query.table,
            "fields": query.fields,
            "limit": query.limit,
        },
    }
    if query.sorts:
        payload["query"]["sorts"] = query.sorts
    if query.filters:
        payload["query"]["filters"] = query.filters
    if query.pivots:
        payload["query"]["pivots"] = query.pivots
    return payload


def _parse_query_result(result: dict[str, Any]) -> QueryResult:
    """Parse a raw API response into a QueryResult.

    Handles both SDK-style (data key) and direct (rows key) responses.
    """
    if "data" in result:
        rows = result["data"]
        fields = list(rows[0].keys()) if rows else []
    elif "rows" in result:
        rows = result["rows"]
        fields = result.get("fields", list(rows[0].keys()) if rows else [])
    else:
        rows = []
        fields = []

    return QueryResult(
        fields=fields,
        rows=rows,
        row_count=len(rows),
        truncated=result.get("truncated", False),
    )


def _decode_arrow_result(arrow_b64: str) -> QueryResult:
    """Decode a base64-encoded Apache Arrow IPC stream into a QueryResult."""
    try:
        import pyarrow as pa
    except ImportError:
        raise OmniAPIError(
            0,
            "pyarrow is required to decode query results. "
            "Install it: uv add pyarrow",
        )

    arrow_bytes = base64.b64decode(arrow_b64)
    reader = pa.ipc.open_stream(arrow_bytes)
    table = reader.read_all()

    # Filter out __raw columns (Omni includes both raw and formatted)
    display_cols = [c for c in table.column_names if not c.endswith("__raw")]
    if not display_cols:
        display_cols = table.column_names

    rows = []
    for row_dict in table.to_pylist():
        rows.append({k: v for k, v in row_dict.items() if k in display_cols})

    return QueryResult(
        fields=display_cols,
        rows=rows,
        row_count=len(rows),
        truncated=False,
    )


class QueryRunner:
    """Execute queries against the Omni API and parse results."""

    def __init__(self, client: OmniClient):
        self._client = client

    def run(self, query: QuerySpec | dict[str, Any]) -> QueryResult:
        """Run a query and return parsed results.

        Omni's /api/v1/query/run returns NDJSON (newline-delimited JSON):
          Line 1: {"jobs_submitted": {job_id: result_id}}
          Line 2: {"job_id": ..., "status": ..., "result": <base64 Arrow>}
          Line 3: {"remaining_job_ids": [], "timed_out": "false"}

        Results are returned as base64-encoded Apache Arrow IPC streams.
        """
        payload = _spec_to_payload(query) if isinstance(query, QuerySpec) else query

        lines = self._client.post_ndjson(
            "/api/v1/query/run", json=payload, timeout=120.0
        )

        if not lines:
            raise OmniAPIError(0, "Empty response from query execution")

        # Check for inline data (legacy/direct JSON response)
        for line in lines:
            if "data" in line or "rows" in line:
                return _parse_query_result(line)

        # Look for Arrow result in NDJSON lines
        for line in lines:
            if line.get("status") == "FAILED":
                error_msg = line.get("error_message", "Unknown error")
                missing = line.get("summary", {}).get("missing_fields", [])
                if missing:
                    error_msg += f". Missing fields: {missing}"
                raise OmniAPIError(0, f"Query failed: {error_msg}")

            if "result" in line:
                return _decode_arrow_result(line["result"])

        # No inline result — check for remaining jobs to poll
        remaining = []
        for line in lines:
            remaining.extend(line.get("remaining_job_ids", []))

        if remaining:
            return self._poll_for_result(remaining)

        raise OmniAPIError(0, "No result data in query response")

    def _poll_for_result(self, job_ids: list[str]) -> QueryResult:
        """Poll /api/v1/query/wait until the query completes."""
        import time

        for _ in range(60):  # max 5 minutes
            lines = self._client.post_ndjson(
                "/api/v1/query/wait",
                json={"jobIds": job_ids},
                timeout=300.0,
            )
            for line in lines:
                if "result" in line:
                    return _decode_arrow_result(line["result"])
                if line.get("status") == "FAILED":
                    raise OmniAPIError(
                        0, f"Query failed: {line.get('error_message', '')}"
                    )
                if "data" in line or "rows" in line:
                    return _parse_query_result(line)
            time.sleep(5)
        raise OmniAPIError(0, "Query timed out after 5 minutes")

    def run_blocking(self, query: QuerySpec | dict[str, Any]) -> QueryResult:
        """Run a query using the blocking poll pattern.

        Alias for run() which now handles polling automatically.
        """
        return self.run(query)
