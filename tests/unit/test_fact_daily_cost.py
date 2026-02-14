"""
Tests for fact_daily_cost — daily cost fact table linked to dim_service.

Validates DDL creation, fact loading from raw_cur + dim_service,
idempotency, NULL handling, monetary precision, and reconciliation.

Test taxonomy: T1 (logic), T2 (contract), T3 (quality), T5 (idempotency), T8 (integration)

Story: S008 -- Daily Cost Fact Table
Controls: C-06 (DQ), C-07 (Reproducibility), C-08 (Idempotency)
"""

from pathlib import Path

import pytest

from src.connector import DuckDBConnector
from src.extract.cur_loader import load_cur_parquet
from src.load.dim_service_loader import load_dim_service
from src.load.fact_daily_cost_loader import load_fact_daily_cost


FIXTURES_DIR = Path("tests/fixtures")


@pytest.fixture
def db_with_dims() -> DuckDBConnector:
    """In-memory DuckDB pre-loaded with CUR data and dim_service."""
    db = DuckDBConnector(database=":memory:")
    load_cur_parquet(db, FIXTURES_DIR / "sample_cur.parquet")
    load_dim_service(db)
    return db


# ---------------------------------------------------------------------------
# T2: Contract tests — DDL and schema
# ---------------------------------------------------------------------------


class TestFactDailyCostSchema:
    """T2: fact_daily_cost table has expected structure."""

    @pytest.mark.t2_contract
    def test_table_created(self, db_with_dims) -> None:
        """Loading creates fact_daily_cost table."""
        load_fact_daily_cost(db_with_dims)
        rows = db_with_dims.fetch_all(
            "SELECT COUNT(*) as cnt FROM fact_daily_cost"
        )
        assert rows[0]["cnt"] > 0

    @pytest.mark.t2_contract
    def test_has_expected_columns(self, db_with_dims) -> None:
        """fact_daily_cost has date, service FK, cost, count, audit columns."""
        load_fact_daily_cost(db_with_dims)
        df = db_with_dims.fetch_df("SELECT * FROM fact_daily_cost LIMIT 1")
        expected = {
            "usage_date",
            "service_key",
            "daily_cost",
            "record_count",
            "null_cost_count",
            "_loaded_at",
        }
        assert expected.issubset(set(df.columns)), (
            f"Missing columns: {expected - set(df.columns)}"
        )


# ---------------------------------------------------------------------------
# T1: Logic tests — content and correctness
# ---------------------------------------------------------------------------


class TestFactDailyCostContent:
    """T1: fact_daily_cost correctly aggregates costs by date and service."""

    @pytest.mark.t1_logic
    def test_records_queryable_by_date_and_service(self, db_with_dims) -> None:
        """AC-1: Daily cost records persisted, queryable by date and service."""
        load_fact_daily_cost(db_with_dims)

        rows = db_with_dims.fetch_all("""
            SELECT usage_date, service_key, daily_cost
            FROM fact_daily_cost
            WHERE usage_date = '2024-01-01'
            ORDER BY service_key
        """)
        assert len(rows) > 0
        for row in rows:
            assert row["usage_date"] == "2024-01-01"
            assert row["service_key"] is not None

    @pytest.mark.t1_logic
    def test_linked_to_dim_service_via_fk(self, db_with_dims) -> None:
        """AC-2: Every fact record has a valid service_key in dim_service."""
        load_fact_daily_cost(db_with_dims)

        orphans = db_with_dims.fetch_all("""
            SELECT COUNT(*) as cnt
            FROM fact_daily_cost f
            LEFT JOIN dim_service d ON f.service_key = d.service_key
            WHERE d.service_key IS NULL
        """)
        assert orphans[0]["cnt"] == 0

    @pytest.mark.t1_logic
    def test_grain_is_date_service(self, db_with_dims) -> None:
        """Grain is (usage_date, service_key) — one row per combination."""
        load_fact_daily_cost(db_with_dims)

        rows = db_with_dims.fetch_all("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT usage_date || '|' || CAST(service_key AS VARCHAR)) as unique_grains
            FROM fact_daily_cost
        """)
        assert rows[0]["total"] == rows[0]["unique_grains"]

    @pytest.mark.t1_logic
    def test_row_count_matches_expected_grain(self, db_with_dims) -> None:
        """Fact should have rows for each (date, service) in raw_cur."""
        load_fact_daily_cost(db_with_dims)

        # Count distinct (date, product_code, usage_type) in raw_cur
        # then map to dim_service to get (date, service_key) combos
        expected = db_with_dims.fetch_all("""
            SELECT COUNT(DISTINCT
                line_item_usage_start_date || '|' ||
                line_item_product_code || '|' ||
                line_item_usage_type
            ) as cnt
            FROM raw_cur
        """)[0]["cnt"]

        actual = db_with_dims.fetch_all(
            "SELECT COUNT(*) as cnt FROM fact_daily_cost"
        )[0]["cnt"]

        assert actual == expected

    @pytest.mark.t1_logic
    def test_daily_cost_sums_correctly(self, db_with_dims) -> None:
        """daily_cost is the sum of unblended_cost for each (date, service)."""
        load_fact_daily_cost(db_with_dims)

        # Pick a specific date and service for spot-check
        sample = db_with_dims.fetch_all("""
            SELECT f.usage_date, f.service_key, f.daily_cost,
                   d.product_code, d.usage_type
            FROM fact_daily_cost f
            JOIN dim_service d ON f.service_key = d.service_key
            WHERE f.usage_date = '2024-01-15'
            AND d.product_code = 'AmazonEC2'
            LIMIT 1
        """)
        if sample:
            row = sample[0]
            raw_sum = db_with_dims.fetch_all(f"""
                SELECT ROUND(SUM(COALESCE(CAST(line_item_unblended_cost AS DOUBLE), 0.0)), 2) as raw_cost
                FROM raw_cur
                WHERE line_item_usage_start_date = '{row["usage_date"]}'
                  AND line_item_product_code = '{row["product_code"]}'
                  AND line_item_usage_type = '{row["usage_type"]}'
            """)[0]["raw_cost"]
            assert row["daily_cost"] == raw_sum

    @pytest.mark.t1_logic
    def test_returns_row_count(self, db_with_dims) -> None:
        """Loader returns the number of rows loaded."""
        count = load_fact_daily_cost(db_with_dims)
        assert isinstance(count, int)
        assert count > 0


# ---------------------------------------------------------------------------
# T3: Data quality tests — NULL handling, precision
# ---------------------------------------------------------------------------


class TestFactDailyCostQuality:
    """T3: Data quality — NULL coalescing, monetary precision, reconciliation."""

    @pytest.mark.t3_quality
    def test_null_costs_coalesced_to_zero(self, db_with_dims) -> None:
        """AC-4: No NULL daily_cost values — NULLs coalesced to 0."""
        load_fact_daily_cost(db_with_dims)

        nulls = db_with_dims.fetch_all(
            "SELECT COUNT(*) as cnt FROM fact_daily_cost WHERE daily_cost IS NULL"
        )
        assert nulls[0]["cnt"] == 0

    @pytest.mark.t3_quality
    def test_null_cost_count_audit_trail(self, db_with_dims) -> None:
        """AC-4: null_cost_count tracks how many source rows had NULL cost."""
        load_fact_daily_cost(db_with_dims)

        rows = db_with_dims.fetch_all(
            "SELECT null_cost_count FROM fact_daily_cost WHERE null_cost_count IS NULL"
        )
        assert len(rows) == 0  # all rows should have a count (0 or more)

    @pytest.mark.t3_quality
    def test_monetary_precision_two_decimals(self, db_with_dims) -> None:
        """AC-6: Monetary values at consistent 2 decimal places."""
        load_fact_daily_cost(db_with_dims)

        rows = db_with_dims.fetch_all(
            "SELECT daily_cost FROM fact_daily_cost"
        )
        for row in rows:
            cost_str = f"{row['daily_cost']:.10f}"
            # Check that rounding to 2 decimals gives same value
            assert round(row["daily_cost"], 2) == row["daily_cost"], (
                f"Cost {row['daily_cost']} not at 2-decimal precision"
            )

    @pytest.mark.t3_quality
    def test_record_count_preserved(self, db_with_dims) -> None:
        """AC-7: record_count per day-service grain matches source rows."""
        load_fact_daily_cost(db_with_dims)

        # Total record_count should equal total raw_cur rows
        fact_total = db_with_dims.fetch_all(
            "SELECT SUM(record_count) as total FROM fact_daily_cost"
        )[0]["total"]

        raw_total = db_with_dims.fetch_all(
            "SELECT COUNT(*) as cnt FROM raw_cur"
        )[0]["cnt"]

        assert fact_total == raw_total

    @pytest.mark.t3_quality
    def test_no_negative_record_counts(self, db_with_dims) -> None:
        """record_count is always positive."""
        load_fact_daily_cost(db_with_dims)

        rows = db_with_dims.fetch_all(
            "SELECT COUNT(*) as cnt FROM fact_daily_cost WHERE record_count <= 0"
        )
        assert rows[0]["cnt"] == 0


# ---------------------------------------------------------------------------
# T5: Idempotency tests
# ---------------------------------------------------------------------------


class TestFactDailyCostIdempotency:
    """T5: Running load twice produces the same result (C-08)."""

    @pytest.mark.t5_idempotency
    def test_double_load_no_duplicates(self, db_with_dims) -> None:
        """AC-3: Re-run does not duplicate records."""
        load_fact_daily_cost(db_with_dims)
        count_first = db_with_dims.fetch_all(
            "SELECT COUNT(*) as cnt FROM fact_daily_cost"
        )[0]["cnt"]

        load_fact_daily_cost(db_with_dims)
        count_second = db_with_dims.fetch_all(
            "SELECT COUNT(*) as cnt FROM fact_daily_cost"
        )[0]["cnt"]

        assert count_first == count_second

    @pytest.mark.t5_idempotency
    def test_double_load_same_costs(self, db_with_dims) -> None:
        """Costs are identical after re-run (C-07 reproducibility)."""
        load_fact_daily_cost(db_with_dims)
        costs_first = db_with_dims.fetch_all(
            "SELECT usage_date, service_key, daily_cost FROM fact_daily_cost ORDER BY usage_date, service_key"
        )

        load_fact_daily_cost(db_with_dims)
        costs_second = db_with_dims.fetch_all(
            "SELECT usage_date, service_key, daily_cost FROM fact_daily_cost ORDER BY usage_date, service_key"
        )

        assert costs_first == costs_second


# ---------------------------------------------------------------------------
# T8: Integration tests — full pipeline
# ---------------------------------------------------------------------------


class TestFactDailyCostIntegration:
    """T8: End-to-end load from raw CUR through dimensions to fact."""

    @pytest.mark.t8_integration
    def test_full_pipeline_loads_fact(self) -> None:
        """AC-5: Loadable from existing pipeline entry points."""
        db = DuckDBConnector(database=":memory:")
        # Extract
        load_cur_parquet(db, FIXTURES_DIR / "sample_cur.parquet")
        # Dimension
        dim_count = load_dim_service(db)
        assert dim_count > 0
        # Fact
        fact_count = load_fact_daily_cost(db)
        assert fact_count > 0
        db.close()

    @pytest.mark.t8_integration
    def test_fact_joins_to_dim_service(self, db_with_dims) -> None:
        """AC-2: Fact FK joins produce valid enriched records."""
        load_fact_daily_cost(db_with_dims)

        rows = db_with_dims.fetch_all("""
            SELECT f.usage_date, d.product_code, d.service_category,
                   f.daily_cost, f.record_count
            FROM fact_daily_cost f
            JOIN dim_service d ON f.service_key = d.service_key
            ORDER BY f.usage_date, d.product_code
            LIMIT 5
        """)
        assert len(rows) == 5
        for row in rows:
            assert row["product_code"] is not None
            assert row["service_category"] is not None
            assert row["daily_cost"] is not None
