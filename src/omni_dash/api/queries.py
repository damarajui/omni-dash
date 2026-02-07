"""Query building and execution against the Omni API."""

from __future__ import annotations

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
            {"columnName": qualified, "sortDescending": descending}
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
        "modelId": query.model_id,
        "query": {
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


class QueryRunner:
    """Execute queries against the Omni API and parse results."""

    def __init__(self, client: OmniClient):
        self._client = client

    def run(self, query: QuerySpec | dict[str, Any]) -> QueryResult:
        """Run a query and return parsed results.

        Accepts either a QuerySpec or a raw API payload dict.
        """
        payload = _spec_to_payload(query) if isinstance(query, QuerySpec) else query

        result = self._client.post("/api/v1/query/run", json=payload, timeout=120.0)

        if not result or not isinstance(result, dict):
            raise OmniAPIError(0, "Empty response from query execution")

        return _parse_query_result(result)

    def run_blocking(self, query: QuerySpec | dict[str, Any]) -> QueryResult:
        """Run a query using the blocking poll pattern.

        Submits the query, then polls /api/v1/query/wait until complete.
        Use this for long-running queries.
        """
        payload = _spec_to_payload(query) if isinstance(query, QuerySpec) else query

        # Submit
        submit_result = self._client.post("/api/v1/query/run", json=payload, timeout=120.0)

        if not submit_result or not isinstance(submit_result, dict):
            raise OmniAPIError(0, "Empty response from query submission")

        # If we got data directly, parse it without re-submitting
        if "data" in submit_result or "rows" in submit_result:
            return _parse_query_result(submit_result)

        # Otherwise poll via /wait
        query_id = submit_result.get("queryId", submit_result.get("id", ""))
        if not query_id:
            raise OmniAPIError(0, "No queryId in submission response")

        wait_result = self._client.get(
            "/api/v1/query/wait",
            params={"queryId": query_id},
            timeout=300.0,
        )

        if not wait_result or not isinstance(wait_result, dict):
            raise OmniAPIError(0, "Empty response from query wait")

        return _parse_query_result(wait_result)
