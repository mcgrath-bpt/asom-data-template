"""
Database connector abstraction — local-first development.

Supports three backends behind one interface:
- DuckDB (default local) — closest to Snowflake SQL, reads Parquet natively
- SQLite3 (minimal fallback) — zero dependencies beyond stdlib
- Snowflake (live) — actual connector for DEV/QA/PROD

Switch via ASOM_ENV environment variable or config file:
    ASOM_ENV=local         → DuckDB (default)
    ASOM_ENV=local-sqlite  → SQLite3
    ASOM_ENV=dev|qa|prod   → Snowflake

Usage:
    from src.connector import get_connector

    # Automatic — reads ASOM_ENV
    db = get_connector()

    # Explicit
    db = get_connector("local")        # DuckDB
    db = get_connector("local-sqlite") # SQLite3
    db = get_connector("dev")          # Snowflake DEV

    # Use it
    db.execute("CREATE TABLE t (id INT, name TEXT)")
    rows = db.fetch_all("SELECT * FROM t")
    df = db.fetch_df("SELECT * FROM t")  # Returns polars.DataFrame
    db.close()

Reference: ASOM framework — skills/python-data-engineering.md
"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import duckdb
import polars as pl

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Protocol (interface)
# ---------------------------------------------------------------------------


@runtime_checkable
class DBConnector(Protocol):
    """Database connector interface.

    All backends implement this protocol. Code that uses get_connector()
    can work with any backend without changes.
    """

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        """Execute a SQL statement (DDL, DML, etc.)."""
        ...

    def fetch_all(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a query and return all rows as list of dicts."""
        ...

    def fetch_df(self, sql: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
        """Execute a query and return results as a polars DataFrame."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...


# ---------------------------------------------------------------------------
# DuckDB — default local backend
# ---------------------------------------------------------------------------


class DuckDBConnector:
    """DuckDB connector — default for local development.

    Features:
    - Reads Parquet files natively: SELECT * FROM 'file.parquet'
    - Supports VARIANT-like structs (STRUCT, MAP types)
    - Closest SQL dialect to Snowflake among local options
    - In-memory mode for tests, file-backed for persistent local dev
    """

    def __init__(self, database: str = ":memory:") -> None:
        if database != ":memory:":
            Path(database).parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(database)
        logger.info("DuckDB connected: %s", database)

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        if params:
            self._conn.execute(sql, params)
        else:
            self._conn.execute(sql)

    def fetch_all(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if params:
            result = self._conn.execute(sql, params)
        else:
            result = self._conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def fetch_df(self, sql: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
        rows = self.fetch_all(sql, params)
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def close(self) -> None:
        self._conn.close()
        logger.info("DuckDB connection closed")


# ---------------------------------------------------------------------------
# SQLite3 — minimal local fallback
# ---------------------------------------------------------------------------


class SQLiteConnector:
    """SQLite3 connector — minimal local fallback.

    Uses Python stdlib only (no extra dependencies).
    Limited SQL compatibility with Snowflake (no VARIANT, no MERGE).
    Best for simple validation or environments where DuckDB isn't available.
    """

    def __init__(self, database: str = ":memory:") -> None:
        if database != ":memory:":
            Path(database).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(database)
        self._conn.row_factory = sqlite3.Row
        logger.info("SQLite connected: %s", database)

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        if params:
            self._conn.execute(sql, params)
        else:
            self._conn.execute(sql)
        self._conn.commit()

    def fetch_all(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        cursor = self._conn.execute(sql, params or {})
        return [dict(row) for row in cursor.fetchall()]

    def fetch_df(self, sql: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
        rows = self.fetch_all(sql, params)
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def close(self) -> None:
        self._conn.close()
        logger.info("SQLite connection closed")


# ---------------------------------------------------------------------------
# Snowflake — live connector for DEV/QA/PROD
# ---------------------------------------------------------------------------


class SnowflakeConnector:
    """Snowflake connector — for DEV/QA/PROD environments.

    Requires snowflake-connector-python:
        pip install asom-data-project[snowflake]

    Credentials are loaded from config/settings.py (env vars or YAML).
    NEVER hardcode credentials.
    """

    def __init__(
        self,
        account: str,
        user: str,
        database: str,
        schema: str = "RAW",
        warehouse: str = "",
        role: str = "",
    ) -> None:
        try:
            import snowflake.connector as sf_connector  # type: ignore[import-untyped]
        except ImportError:
            raise ImportError(
                "snowflake-connector-python not installed. Install with: pip install asom-data-project[snowflake]"
            )

        self._sf = sf_connector  # Store module reference for later use

        connect_params: dict[str, Any] = {
            "account": account,
            "user": user,
            "database": database,
            "schema": schema,
            "authenticator": "externalbrowser",  # SSO by default
        }
        if warehouse:
            connect_params["warehouse"] = warehouse
        if role:
            connect_params["role"] = role

        self._conn = sf_connector.connect(**connect_params)
        logger.info("Snowflake connected: %s.%s", database, schema)

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql, params)
        finally:
            cursor.close()

    def fetch_all(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        cursor = self._conn.cursor(self._sf.DictCursor)
        try:
            cursor.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def fetch_df(self, sql: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
        rows = self.fetch_all(sql, params)
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def close(self) -> None:
        self._conn.close()
        logger.info("Snowflake connection closed")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_connector(env: str | None = None) -> DBConnector:
    """Create a database connector for the specified environment.

    Args:
        env: Environment name. If None, reads ASOM_ENV (default: "local").
            - "local"         → DuckDB (file-backed)
            - "local-sqlite"  → SQLite3 (file-backed)
            - "test"          → DuckDB (in-memory)
            - "dev"|"qa"|"prod" → Snowflake

    Returns:
        A DBConnector instance.
    """
    from config.settings import get_settings

    env = env or os.getenv("ASOM_ENV", "local")
    settings = get_settings(env)

    if settings.db_type == "duckdb":
        return DuckDBConnector(database=settings.db_path)
    elif settings.db_type == "sqlite":
        return SQLiteConnector(database=settings.db_path)
    elif settings.db_type == "snowflake":
        return SnowflakeConnector(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
            warehouse=settings.snowflake_warehouse,
            role=settings.snowflake_role,
        )
    else:
        raise ValueError(f"Unknown db_type: {settings.db_type!r}. Use 'duckdb', 'sqlite', or 'snowflake'.")
