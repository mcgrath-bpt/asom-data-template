"""
Tests for database connector abstraction.

Verifies that DuckDB and SQLite3 connectors implement the DBConnector protocol
correctly, and that the factory function routes to the right backend.

Test taxonomy: T1 (logic), T2 (contract), T8 (integration)

Reference: ASOM framework — skills/testing-strategies.md
"""

import polars as pl
import pytest

from src.connector import (
    DBConnector,
    DuckDBConnector,
    SQLiteConnector,
    get_connector,
)

# ---------------------------------------------------------------------------
# T2: Contract tests — connectors satisfy the protocol
# ---------------------------------------------------------------------------


class TestDuckDBContract:
    """T2: DuckDB connector satisfies DBConnector protocol."""

    @pytest.mark.t2_contract
    def test_duckdb_is_dbconnector(self, local_db: DuckDBConnector) -> None:
        assert isinstance(local_db, DBConnector)

    @pytest.mark.t2_contract
    def test_duckdb_execute(self, local_db: DuckDBConnector) -> None:
        local_db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        local_db.execute("INSERT INTO t VALUES (1, 'alice')")
        rows = local_db.fetch_all("SELECT * FROM t")
        assert len(rows) == 1
        assert rows[0]["id"] == 1

    @pytest.mark.t2_contract
    def test_duckdb_fetch_df_returns_polars(self, local_db: DuckDBConnector) -> None:
        local_db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        local_db.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
        df = local_db.fetch_df("SELECT * FROM t")
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2
        assert "id" in df.columns
        assert "name" in df.columns

    @pytest.mark.t2_contract
    def test_duckdb_fetch_all_empty(self, local_db: DuckDBConnector) -> None:
        local_db.execute("CREATE TABLE t (id INTEGER)")
        rows = local_db.fetch_all("SELECT * FROM t")
        assert rows == []


class TestSQLiteContract:
    """T2: SQLite connector satisfies DBConnector protocol."""

    @pytest.mark.t2_contract
    def test_sqlite_is_dbconnector(self, local_sqlite: SQLiteConnector) -> None:
        assert isinstance(local_sqlite, DBConnector)

    @pytest.mark.t2_contract
    def test_sqlite_execute(self, local_sqlite: SQLiteConnector) -> None:
        local_sqlite.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        local_sqlite.execute("INSERT INTO t VALUES (1, 'alice')")
        rows = local_sqlite.fetch_all("SELECT * FROM t")
        assert len(rows) == 1
        assert rows[0]["id"] == 1

    @pytest.mark.t2_contract
    def test_sqlite_fetch_df_returns_polars(self, local_sqlite: SQLiteConnector) -> None:
        local_sqlite.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        local_sqlite.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
        df = local_sqlite.fetch_df("SELECT * FROM t")
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2

    @pytest.mark.t2_contract
    def test_sqlite_fetch_all_empty(self, local_sqlite: SQLiteConnector) -> None:
        local_sqlite.execute("CREATE TABLE t (id INTEGER)")
        rows = local_sqlite.fetch_all("SELECT * FROM t")
        assert rows == []


# ---------------------------------------------------------------------------
# T1: Logic tests — factory function routing
# ---------------------------------------------------------------------------


class TestGetConnector:
    """T1: Factory function routes to correct backend."""

    @pytest.mark.t1_logic
    def test_local_returns_duckdb(self) -> None:
        conn = get_connector("local")
        assert isinstance(conn, DuckDBConnector)
        conn.close()

    @pytest.mark.t1_logic
    def test_local_sqlite_returns_sqlite(self) -> None:
        conn = get_connector("local-sqlite")
        assert isinstance(conn, SQLiteConnector)
        conn.close()

    @pytest.mark.t1_logic
    def test_test_env_returns_duckdb(self) -> None:
        conn = get_connector("test")
        assert isinstance(conn, DuckDBConnector)
        conn.close()


# ---------------------------------------------------------------------------
# T8: Integration tests — cross-backend consistency
# ---------------------------------------------------------------------------


class TestCrossBackendConsistency:
    """T8: Same operations produce consistent results across backends."""

    @pytest.mark.t8_integration
    def test_insert_and_select_consistent(self, local_db: DuckDBConnector, local_sqlite: SQLiteConnector) -> None:
        """Both backends should return the same data for the same operations."""
        for db in [local_db, local_sqlite]:
            db.execute("CREATE TABLE customers (id INTEGER, name TEXT, email TEXT)")
            db.execute("INSERT INTO customers VALUES (1, 'Alice', 'alice@test.com')")
            db.execute("INSERT INTO customers VALUES (2, 'Bob', 'bob@test.com')")

        duck_rows = local_db.fetch_all("SELECT id, name FROM customers ORDER BY id")
        sqlite_rows = local_sqlite.fetch_all("SELECT id, name FROM customers ORDER BY id")

        assert duck_rows == sqlite_rows

    @pytest.mark.t8_integration
    def test_fetch_df_schema_consistent(self, local_db: DuckDBConnector, local_sqlite: SQLiteConnector) -> None:
        """DataFrames from both backends should have the same column names."""
        for db in [local_db, local_sqlite]:
            db.execute("CREATE TABLE t (id INTEGER, value REAL)")
            db.execute("INSERT INTO t VALUES (1, 3.14)")

        duck_df = local_db.fetch_df("SELECT * FROM t")
        sqlite_df = local_sqlite.fetch_df("SELECT * FROM t")

        assert set(duck_df.columns) == set(sqlite_df.columns)
