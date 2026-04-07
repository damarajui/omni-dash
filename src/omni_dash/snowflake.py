"""Lightweight Snowflake query runner for the Dash agent.

Provides read-only access to Snowflake for cases where Omni topics
don't cover the data. Loads credentials from environment variables
or ~/.dbt/profiles.yml.

This is intentionally simple — no connection pooling, no async.
The Slack bot calls this synchronously for spot-checks.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)

# Block all write operations
_WRITE_RE = re.compile(
    r"\b(DROP|DELETE|INSERT|UPDATE|CREATE|ALTER|TRUNCATE|MERGE|COPY\s+INTO|"
    r"PUT|GET|GRANT|REVOKE|CALL|EXECUTE\s+TASK|EXECUTE\s+IMMEDIATE)\b",
    re.IGNORECASE,
)


def _json_serializer(obj: Any) -> Any:
    """JSON serializer for Snowflake types."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    return str(obj)


def _load_credentials() -> dict[str, str] | None:
    """Load Snowflake credentials from env vars or ~/.dbt/profiles.yml."""
    # Try env vars first
    account = os.environ.get("SNOWFLAKE_ACCOUNT", "")
    user = os.environ.get("SNOWFLAKE_USER", "")
    password = os.environ.get("SNOWFLAKE_PASSWORD", "")
    if account and user and password:
        return {"account": account, "user": user, "password": password}

    # Fall back to dbt profiles
    try:
        import yaml

        path = os.path.expanduser("~/.dbt/profiles.yml")
        if not os.path.exists(path):
            return None
        with open(path) as f:
            profiles = yaml.safe_load(f)
        cfg = profiles.get("lindy", {}).get("outputs", {}).get("dev", {})
        if cfg.get("account") and cfg.get("user") and cfg.get("password"):
            return {
                "account": cfg["account"],
                "user": cfg["user"],
                "password": cfg["password"],
            }
    except Exception as e:
        logger.debug("Could not load dbt profiles: %s", e)

    return None


def query_snowflake(
    sql: str,
    database: str = "TRAINING_DATABASE",
    schema: str = "PUBLIC",
    limit: int = 100,
) -> str:
    """Execute a read-only SQL query against Snowflake.

    Args:
        sql: SQL query (SELECT only — writes are blocked).
        database: Snowflake database name.
        schema: Snowflake schema name.
        limit: Max rows to return.

    Returns:
        JSON string with columns and rows, or error.
    """
    # Block writes
    if _WRITE_RE.search(sql):
        return json.dumps({
            "error": "Write operations are not allowed. Only SELECT queries are permitted.",
        })

    creds = _load_credentials()
    if not creds:
        return json.dumps({
            "error": "Snowflake credentials not found. Set SNOWFLAKE_ACCOUNT, "
            "SNOWFLAKE_USER, SNOWFLAKE_PASSWORD env vars or configure ~/.dbt/profiles.yml.",
        })

    try:
        import snowflake.connector

        conn = snowflake.connector.connect(
            **creds,
            database=database,
            schema=schema,
            client_session_keep_alive=False,
        )
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchmany(limit)

            # Convert to list of dicts
            data = [
                {col: _json_serializer(val) if not isinstance(val, (str, int, float, type(None))) else val
                 for col, val in zip(columns, row)}
                for row in rows
            ]

            return json.dumps({
                "columns": columns,
                "rows": data,
                "row_count": len(data),
                "truncated": cursor.rowcount is not None and cursor.rowcount > limit,
                "database": database,
                "schema": schema,
            }, indent=2, default=_json_serializer)
        finally:
            conn.close()

    except ImportError:
        return json.dumps({
            "error": "snowflake-connector-python not installed. "
            "Install with: pip install snowflake-connector-python",
        })
    except Exception as e:
        return json.dumps({"error": f"Snowflake query failed: {e}"})
