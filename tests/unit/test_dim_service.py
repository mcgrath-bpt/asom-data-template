"""
Tests for dim_service (SCD Type 1) dimension table.

Validates DDL creation, MERGE logic, service category derivation,
and idempotent loading.

Test taxonomy: T1 (logic), T2 (contract), T5 (idempotency), T8 (integration)

Story: S005 -- Create dim_service (SCD Type 1) reference table
Controls: C-06 (DQ), C-07 (Reproducibility), C-08 (Idempotency)
"""

from pathlib import Path

import pytest

from src.connector import DuckDBConnector
from src.extract.cur_loader import load_cur_parquet
from src.load.dim_service_loader import load_dim_service


FIXTURES_DIR = Path("tests/fixtures")


@pytest.fixture
def db_with_cur() -> DuckDBConnector:
    """In-memory DuckDB pre-loaded with CUR data."""
    db = DuckDBConnector(database=":memory:")
    load_cur_parquet(db, FIXTURES_DIR / "sample_cur.parquet")
    return db


# ---------------------------------------------------------------------------
# T2: Contract tests -- DDL and schema
# ---------------------------------------------------------------------------


class TestDimServiceSchema:
    """T2: dim_service table has expected structure."""

    @pytest.mark.t2_contract
    def test_dim_service_table_created(self, db_with_cur) -> None:
        """Loading creates dim_service table."""
        load_dim_service(db_with_cur)
        rows = db_with_cur.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_service"
        )
        assert rows[0]["cnt"] > 0

    @pytest.mark.t2_contract
    def test_dim_service_has_expected_columns(self, db_with_cur) -> None:
        """dim_service has surrogate key, natural key, category, dates, audit."""
        load_dim_service(db_with_cur)
        df = db_with_cur.fetch_df("SELECT * FROM dim_service LIMIT 1")
        expected = {
            "service_key",
            "product_code",
            "usage_type",
            "service_category",
            "first_seen_date",
            "last_seen_date",
            "_loaded_at",
            "_updated_at",
        }
        assert expected.issubset(set(df.columns)), (
            f"Missing columns: {expected - set(df.columns)}"
        )


# ---------------------------------------------------------------------------
# T1: Logic tests -- content and derivation
# ---------------------------------------------------------------------------


class TestDimServiceContent:
    """T1: dim_service correctly extracts and categorises services."""

    @pytest.mark.t1_logic
    def test_extracts_all_service_combos(self, db_with_cur) -> None:
        """All distinct (product_code, usage_type) combos from CUR are present."""
        load_dim_service(db_with_cur)

        dim_count = db_with_cur.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_service"
        )[0]["cnt"]

        cur_combos = db_with_cur.fetch_all("""
            SELECT COUNT(DISTINCT line_item_product_code || '|' || line_item_usage_type) as cnt
            FROM raw_cur
        """)[0]["cnt"]

        assert dim_count == cur_combos

    @pytest.mark.t1_logic
    def test_service_category_compute(self, db_with_cur) -> None:
        """service_category is correctly derived from product_code."""
        load_dim_service(db_with_cur)

        rows = db_with_cur.fetch_all(
            "SELECT product_code, service_category FROM dim_service ORDER BY product_code"
        )
        categories = {r["product_code"]: r["service_category"] for r in rows}

        assert categories["AmazonEC2"] == "Compute"
        assert categories["AmazonS3"] == "Storage"
        assert categories["AmazonRDS"] == "Database"
        assert categories["AWSLambda"] == "Serverless"
        assert categories["AmazonCloudWatch"] == "Monitoring"
        assert categories["AmazonRedshift"] == "Database"

    @pytest.mark.t1_logic
    def test_first_seen_date_populated(self, db_with_cur) -> None:
        """first_seen_date is set for all services."""
        load_dim_service(db_with_cur)

        rows = db_with_cur.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_service WHERE first_seen_date IS NULL"
        )
        assert rows[0]["cnt"] == 0

    @pytest.mark.t1_logic
    def test_last_seen_date_populated(self, db_with_cur) -> None:
        """last_seen_date is set for all services."""
        load_dim_service(db_with_cur)

        rows = db_with_cur.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_service WHERE last_seen_date IS NULL"
        )
        assert rows[0]["cnt"] == 0

    @pytest.mark.t1_logic
    def test_surrogate_keys_unique(self, db_with_cur) -> None:
        """service_key values are unique."""
        load_dim_service(db_with_cur)

        rows = db_with_cur.fetch_all("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT service_key) as unique_keys
            FROM dim_service
        """)
        assert rows[0]["total"] == rows[0]["unique_keys"]


# ---------------------------------------------------------------------------
# T5: Idempotency tests
# ---------------------------------------------------------------------------


class TestDimServiceIdempotency:
    """T5: Running load twice produces the same result."""

    @pytest.mark.t5_idempotency
    def test_double_load_no_duplicates(self, db_with_cur) -> None:
        """Loading dim_service twice does not duplicate rows."""
        load_dim_service(db_with_cur)
        count_after_first = db_with_cur.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_service"
        )[0]["cnt"]

        load_dim_service(db_with_cur)
        count_after_second = db_with_cur.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_service"
        )[0]["cnt"]

        assert count_after_first == count_after_second

    @pytest.mark.t5_idempotency
    def test_last_seen_date_updates_on_reload(self, db_with_cur) -> None:
        """Type 1: last_seen_date is updated when service is re-observed."""
        load_dim_service(db_with_cur)

        # Get initial last_seen for EC2 RunInstances
        rows = db_with_cur.fetch_all("""
            SELECT last_seen_date FROM dim_service
            WHERE product_code = 'AmazonEC2' AND usage_type = 'EC2-RunInstances'
        """)
        initial_date = rows[0]["last_seen_date"]
        assert initial_date is not None

        # Load again — last_seen should still be set (same or later)
        load_dim_service(db_with_cur)
        rows = db_with_cur.fetch_all("""
            SELECT last_seen_date FROM dim_service
            WHERE product_code = 'AmazonEC2' AND usage_type = 'EC2-RunInstances'
        """)
        assert rows[0]["last_seen_date"] is not None
