"""
Tests for fact_customer_cost — customer cost attribution fact table.

Validates DDL creation, cost allocation across customers with SCD2 temporal
joins, PII controls, idempotency, NULL handling, and end-to-end integration.

Test taxonomy: T1 (logic), T2 (contract), T3 (quality), T4 (access/PII),
               T5 (idempotency), T8 (integration)

Story: S009 -- Customer Cost Attribution Fact Table
Controls: C-04 (PII), C-06 (DQ), C-07 (Reproducibility), C-08 (Idempotency)
"""

from pathlib import Path

import pytest

from src.connector import DuckDBConnector
from src.extract.cur_loader import load_cur_parquet
from src.load.dim_customer_loader import load_dim_customer
from src.load.dim_service_loader import load_dim_service
from src.load.fact_customer_cost_loader import load_fact_customer_cost


FIXTURES_DIR = Path("tests/fixtures")


@pytest.fixture
def db_with_dims() -> DuckDBConnector:
    """In-memory DuckDB with CUR, dim_service, and dim_customer (v1 only).

    v1 loads 30 customers, all with effective_from='2025-01-30',
    effective_to=NULL, is_current=TRUE.
    All 30 days of CUR data (2025-01-01 to 2025-01-30) fall before the
    customer effective_from, so the temporal join uses effective_from <= date
    convention. Since v1 effective_from='2025-01-30', this covers the full range.
    """
    db = DuckDBConnector(database=":memory:")
    load_cur_parquet(db, FIXTURES_DIR / "sample_cur.parquet")
    load_dim_service(db)
    load_dim_customer(db, FIXTURES_DIR / "sample_customers_v1.parquet",
                      snapshot_date="2025-01-01")
    return db


@pytest.fixture
def db_with_scd2() -> DuckDBConnector:
    """In-memory DuckDB with SCD2 history: v1 (Jan 1) then v2 (Jan 15).

    v1: 30 customers active from 2025-01-01
    v2: 6 segment changes (effective_from=2025-01-15), 3 new customers
    After v2: 39 total rows, 33 current, 6 expired
    """
    db = DuckDBConnector(database=":memory:")
    load_cur_parquet(db, FIXTURES_DIR / "sample_cur.parquet")
    load_dim_service(db)
    load_dim_customer(db, FIXTURES_DIR / "sample_customers_v1.parquet",
                      snapshot_date="2025-01-01")
    load_dim_customer(db, FIXTURES_DIR / "sample_customers_v2.parquet",
                      snapshot_date="2025-01-15")
    return db


# ---------------------------------------------------------------------------
# T2: Contract tests — DDL and schema
# ---------------------------------------------------------------------------


class TestFactCustomerCostSchema:
    """T2: fact_customer_cost table has expected structure."""

    @pytest.mark.t2_contract
    def test_table_created(self, db_with_dims) -> None:
        """Loading creates fact_customer_cost table."""
        load_fact_customer_cost(db_with_dims)
        rows = db_with_dims.fetch_all(
            "SELECT COUNT(*) as cnt FROM fact_customer_cost"
        )
        assert rows[0]["cnt"] > 0

    @pytest.mark.t2_contract
    def test_has_expected_columns(self, db_with_dims) -> None:
        """fact_customer_cost has date, customer FK, service FK, cost, audit columns."""
        load_fact_customer_cost(db_with_dims)
        df = db_with_dims.fetch_df("SELECT * FROM fact_customer_cost LIMIT 1")
        expected = {
            "usage_date",
            "customer_key",
            "service_key",
            "allocated_cost",
            "record_count",
            "null_cost_count",
            "_loaded_at",
        }
        assert expected.issubset(set(df.columns)), (
            f"Missing columns: {expected - set(df.columns)}"
        )


# ---------------------------------------------------------------------------
# T1: Logic tests — customer attribution and cost allocation
# ---------------------------------------------------------------------------


class TestFactCustomerCostAttribution:
    """T1: Cost is correctly attributed to customers by date."""

    @pytest.mark.t1_logic
    def test_records_queryable_by_customer_and_date(self, db_with_dims) -> None:
        """AC-1: Cost records attributed to customers, queryable by customer and date."""
        load_fact_customer_cost(db_with_dims)

        rows = db_with_dims.fetch_all("""
            SELECT usage_date, customer_key, service_key, allocated_cost
            FROM fact_customer_cost
            WHERE usage_date = '2025-01-01'
            ORDER BY customer_key, service_key
        """)
        assert len(rows) > 0
        for row in rows:
            assert row["customer_key"] is not None
            assert row["usage_date"] == "2025-01-01"

    @pytest.mark.t1_logic
    def test_linked_to_dim_customer_via_fk(self, db_with_dims) -> None:
        """AC-2: Every fact record has a valid customer_key in dim_customer."""
        load_fact_customer_cost(db_with_dims)

        orphans = db_with_dims.fetch_all("""
            SELECT COUNT(*) as cnt
            FROM fact_customer_cost f
            LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
            WHERE c.customer_key IS NULL
        """)
        assert orphans[0]["cnt"] == 0

    @pytest.mark.t1_logic
    def test_linked_to_dim_service_via_fk(self, db_with_dims) -> None:
        """AC-2: Every fact record has a valid service_key in dim_service."""
        load_fact_customer_cost(db_with_dims)

        orphans = db_with_dims.fetch_all("""
            SELECT COUNT(*) as cnt
            FROM fact_customer_cost f
            LEFT JOIN dim_service d ON f.service_key = d.service_key
            WHERE d.service_key IS NULL
        """)
        assert orphans[0]["cnt"] == 0

    @pytest.mark.t1_logic
    def test_grain_is_date_customer_service(self, db_with_dims) -> None:
        """Grain is (usage_date, customer_key, service_key) — one row per combo."""
        load_fact_customer_cost(db_with_dims)

        rows = db_with_dims.fetch_all("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT
                       usage_date || '|' ||
                       CAST(customer_key AS VARCHAR) || '|' ||
                       CAST(service_key AS VARCHAR)
                   ) as unique_grains
            FROM fact_customer_cost
        """)
        assert rows[0]["total"] == rows[0]["unique_grains"]

    @pytest.mark.t1_logic
    def test_all_active_customers_get_cost(self, db_with_dims) -> None:
        """Each active customer on a given date receives allocated cost."""
        load_fact_customer_cost(db_with_dims)

        # With v1 only (30 customers all active), each date should have
        # 30 customers × 12 services = 360 rows per date
        customers_per_date = db_with_dims.fetch_all("""
            SELECT usage_date, COUNT(DISTINCT customer_key) as cust_count
            FROM fact_customer_cost
            GROUP BY usage_date
            ORDER BY usage_date
        """)
        for row in customers_per_date:
            assert row["cust_count"] == 30, (
                f"Date {row['usage_date']}: expected 30 customers, got {row['cust_count']}"
            )

    @pytest.mark.t1_logic
    def test_cost_allocation_sums_to_daily_total(self, db_with_dims) -> None:
        """Allocated costs across customers sum to the service's daily total."""
        load_fact_customer_cost(db_with_dims)

        # For each (date, service), sum of allocated_cost across customers
        # should equal the total daily cost from raw_cur
        discrepancies = db_with_dims.fetch_all("""
            WITH raw_totals AS (
                SELECT
                    r.line_item_usage_start_date AS usage_date,
                    d.service_key,
                    ROUND(SUM(COALESCE(CAST(r.line_item_unblended_cost AS DOUBLE), 0.0)), 2) AS raw_cost
                FROM raw_cur r
                JOIN dim_service d
                    ON r.line_item_product_code = d.product_code
                   AND r.line_item_usage_type = d.usage_type
                GROUP BY r.line_item_usage_start_date, d.service_key
            ),
            fact_totals AS (
                SELECT
                    usage_date,
                    service_key,
                    ROUND(SUM(allocated_cost), 2) AS total_allocated
                FROM fact_customer_cost
                GROUP BY usage_date, service_key
            )
            SELECT rt.usage_date, rt.service_key,
                   rt.raw_cost, ft.total_allocated,
                   ABS(rt.raw_cost - ft.total_allocated) AS diff
            FROM raw_totals rt
            JOIN fact_totals ft
                ON rt.usage_date = ft.usage_date
               AND rt.service_key = ft.service_key
            WHERE ABS(rt.raw_cost - ft.total_allocated) > 0.20
        """)
        assert len(discrepancies) == 0, (
            f"Cost allocation mismatch: {discrepancies[:5]}"
        )

    @pytest.mark.t1_logic
    def test_returns_row_count(self, db_with_dims) -> None:
        """Loader returns the number of rows loaded."""
        count = load_fact_customer_cost(db_with_dims)
        assert isinstance(count, int)
        assert count > 0


# ---------------------------------------------------------------------------
# T1: Logic tests — SCD2 temporal join
# ---------------------------------------------------------------------------


class TestFactCustomerCostSCD2:
    """T1: SCD2 temporal join — costs attributed to correct customer version."""

    @pytest.mark.t1_logic
    def test_scd2_before_change_uses_original_version(self, db_with_scd2) -> None:
        """AC-3: Costs before segment change use original customer version.

        Customer 2 changed segment on 2025-01-15. Costs on 2025-01-14
        should be attributed to the original (expired) customer_key.
        """
        load_fact_customer_cost(db_with_scd2)

        # Get the original (expired) version of customer 2
        original = db_with_scd2.fetch_all("""
            SELECT customer_key FROM dim_customer
            WHERE customer_id = 2 AND is_current = FALSE
        """)
        assert len(original) == 1
        original_key = original[0]["customer_key"]

        # Cost on 2025-01-14 should reference original key
        rows = db_with_scd2.fetch_all(f"""
            SELECT COUNT(*) as cnt FROM fact_customer_cost
            WHERE usage_date = '2025-01-14'
              AND customer_key = {original_key}
        """)
        assert rows[0]["cnt"] > 0, (
            f"Expected costs for original customer_key={original_key} on 2025-01-14"
        )

    @pytest.mark.t1_logic
    def test_scd2_after_change_uses_new_version(self, db_with_scd2) -> None:
        """AC-3: Costs after segment change use new customer version.

        Customer 2 changed segment on 2025-01-15. Costs on 2025-01-15
        should be attributed to the new (current) customer_key.
        """
        load_fact_customer_cost(db_with_scd2)

        # Get the new (current) version of customer 2
        current = db_with_scd2.fetch_all("""
            SELECT customer_key FROM dim_customer
            WHERE customer_id = 2 AND is_current = TRUE
        """)
        assert len(current) == 1
        current_key = current[0]["customer_key"]

        # Cost on 2025-01-15 should reference current key
        rows = db_with_scd2.fetch_all(f"""
            SELECT COUNT(*) as cnt FROM fact_customer_cost
            WHERE usage_date = '2025-01-15'
              AND customer_key = {current_key}
        """)
        assert rows[0]["cnt"] > 0, (
            f"Expected costs for current customer_key={current_key} on 2025-01-15"
        )

    @pytest.mark.t1_logic
    def test_scd2_no_overlap_same_customer_id_same_date(self, db_with_scd2) -> None:
        """AC-3: A customer_id should appear at most once per date per service.

        SCD2 temporal join must not double-count by matching both versions.
        """
        load_fact_customer_cost(db_with_scd2)

        # Join back to dim_customer to get customer_id, check for duplicates
        duplicates = db_with_scd2.fetch_all("""
            SELECT f.usage_date, c.customer_id, f.service_key, COUNT(*) as cnt
            FROM fact_customer_cost f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY f.usage_date, c.customer_id, f.service_key
            HAVING COUNT(*) > 1
        """)
        assert len(duplicates) == 0, (
            f"Duplicate customer_id per date/service: {duplicates[:5]}"
        )

    @pytest.mark.t1_logic
    def test_scd2_new_customers_only_from_effective_date(self, db_with_scd2) -> None:
        """AC-3: New customers (31, 32, 33) only get costs from 2025-01-15 onward."""
        load_fact_customer_cost(db_with_scd2)

        early_rows = db_with_scd2.fetch_all("""
            SELECT COUNT(*) as cnt
            FROM fact_customer_cost f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            WHERE c.customer_id IN (31, 32, 33)
              AND f.usage_date < '2025-01-15'
        """)
        assert early_rows[0]["cnt"] == 0, (
            "New customers should have no costs before their effective_from date"
        )

    @pytest.mark.t1_logic
    def test_scd2_customer_count_changes_at_boundary(self, db_with_scd2) -> None:
        """AC-3: Customer count per date changes at the SCD2 boundary.

        Before 2025-01-15: 30 customers active.
        From 2025-01-15: 33 customers active (30 original + 3 new).
        """
        load_fact_customer_cost(db_with_scd2)

        counts = db_with_scd2.fetch_all("""
            SELECT usage_date, COUNT(DISTINCT customer_key) as cust_count
            FROM fact_customer_cost
            GROUP BY usage_date
            ORDER BY usage_date
        """)
        for row in counts:
            if row["usage_date"] < "2025-01-15":
                assert row["cust_count"] == 30, (
                    f"Date {row['usage_date']}: expected 30, got {row['cust_count']}"
                )
            else:
                assert row["cust_count"] == 33, (
                    f"Date {row['usage_date']}: expected 33, got {row['cust_count']}"
                )


# ---------------------------------------------------------------------------
# T1: Logic tests — segment aggregation
# ---------------------------------------------------------------------------


class TestFactCustomerCostSegment:
    """T1: AC-6 — supports aggregation by customer segment."""

    @pytest.mark.t1_logic
    def test_aggregation_by_segment(self, db_with_dims) -> None:
        """AC-6: Costs can be aggregated by customer segment via dimension join."""
        load_fact_customer_cost(db_with_dims)

        rows = db_with_dims.fetch_all("""
            SELECT c.segment,
                   ROUND(SUM(f.allocated_cost), 2) AS segment_cost
            FROM fact_customer_cost f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.segment
            ORDER BY c.segment
        """)
        assert len(rows) > 0
        for row in rows:
            assert row["segment"] is not None
            assert row["segment_cost"] is not None


# ---------------------------------------------------------------------------
# T3: Data quality tests — NULL handling, precision, referential integrity
# ---------------------------------------------------------------------------


class TestFactCustomerCostQuality:
    """T3: Data quality — NULL coalescing, monetary precision, referential integrity."""

    @pytest.mark.t3_quality
    def test_null_costs_coalesced_to_zero(self, db_with_dims) -> None:
        """AC-5: No NULL allocated_cost values — NULLs coalesced to 0."""
        load_fact_customer_cost(db_with_dims)

        nulls = db_with_dims.fetch_all(
            "SELECT COUNT(*) as cnt FROM fact_customer_cost WHERE allocated_cost IS NULL"
        )
        assert nulls[0]["cnt"] == 0

    @pytest.mark.t3_quality
    def test_null_cost_count_audit_trail(self, db_with_dims) -> None:
        """AC-5: null_cost_count tracks how many source rows had NULL cost."""
        load_fact_customer_cost(db_with_dims)

        rows = db_with_dims.fetch_all(
            "SELECT null_cost_count FROM fact_customer_cost WHERE null_cost_count IS NULL"
        )
        assert len(rows) == 0

    @pytest.mark.t3_quality
    def test_monetary_precision_two_decimals(self, db_with_dims) -> None:
        """Monetary values at consistent 2 decimal places."""
        load_fact_customer_cost(db_with_dims)

        rows = db_with_dims.fetch_all(
            "SELECT allocated_cost FROM fact_customer_cost"
        )
        for row in rows:
            assert round(row["allocated_cost"], 2) == row["allocated_cost"], (
                f"Cost {row['allocated_cost']} not at 2-decimal precision"
            )

    @pytest.mark.t3_quality
    def test_no_negative_record_counts(self, db_with_dims) -> None:
        """record_count is always positive."""
        load_fact_customer_cost(db_with_dims)

        rows = db_with_dims.fetch_all(
            "SELECT COUNT(*) as cnt FROM fact_customer_cost WHERE record_count <= 0"
        )
        assert rows[0]["cnt"] == 0

    @pytest.mark.t3_quality
    def test_no_orphan_customer_keys(self, db_with_dims) -> None:
        """C-06: No orphan customer FKs — all customer_keys exist in dim_customer."""
        load_fact_customer_cost(db_with_dims)

        orphans = db_with_dims.fetch_all("""
            SELECT COUNT(*) as cnt
            FROM fact_customer_cost f
            LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
            WHERE c.customer_key IS NULL
        """)
        assert orphans[0]["cnt"] == 0

    @pytest.mark.t3_quality
    def test_no_orphan_service_keys(self, db_with_dims) -> None:
        """C-06: No orphan service FKs — all service_keys exist in dim_service."""
        load_fact_customer_cost(db_with_dims)

        orphans = db_with_dims.fetch_all("""
            SELECT COUNT(*) as cnt
            FROM fact_customer_cost f
            LEFT JOIN dim_service d ON f.service_key = d.service_key
            WHERE d.service_key IS NULL
        """)
        assert orphans[0]["cnt"] == 0


# ---------------------------------------------------------------------------
# T4: PII/access control tests
# ---------------------------------------------------------------------------


class TestFactCustomerCostPII:
    """T4: PII controls — fact table contains no raw PII."""

    @pytest.mark.t4_access
    def test_no_email_in_fact_table(self, db_with_dims) -> None:
        """AC-7 / C-04: No email column in fact table."""
        load_fact_customer_cost(db_with_dims)
        df = db_with_dims.fetch_df("SELECT * FROM fact_customer_cost LIMIT 1")
        email_cols = [c for c in df.columns if "email" in c.lower()]
        assert len(email_cols) == 0, f"Email columns found: {email_cols}"

    @pytest.mark.t4_access
    def test_no_phone_in_fact_table(self, db_with_dims) -> None:
        """AC-7 / C-04: No phone column in fact table."""
        load_fact_customer_cost(db_with_dims)
        df = db_with_dims.fetch_df("SELECT * FROM fact_customer_cost LIMIT 1")
        phone_cols = [c for c in df.columns if "phone" in c.lower()]
        assert len(phone_cols) == 0, f"Phone columns found: {phone_cols}"

    @pytest.mark.t4_access
    def test_no_name_in_fact_table(self, db_with_dims) -> None:
        """AC-7 / C-04: No name columns in fact table."""
        load_fact_customer_cost(db_with_dims)
        df = db_with_dims.fetch_df("SELECT * FROM fact_customer_cost LIMIT 1")
        name_cols = [c for c in df.columns if "name" in c.lower()]
        assert len(name_cols) == 0, f"Name columns found: {name_cols}"

    @pytest.mark.t4_access
    def test_surrogate_key_only(self, db_with_dims) -> None:
        """AC-7 / C-04: Customer reference is surrogate key only, no customer_id."""
        load_fact_customer_cost(db_with_dims)
        df = db_with_dims.fetch_df("SELECT * FROM fact_customer_cost LIMIT 1")
        assert "customer_key" in df.columns
        assert "customer_id" not in df.columns, (
            "customer_id (natural key) should not be in fact table"
        )


# ---------------------------------------------------------------------------
# T5: Idempotency tests
# ---------------------------------------------------------------------------


class TestFactCustomerCostIdempotency:
    """T5: Running load twice produces the same result (C-08)."""

    @pytest.mark.t5_idempotency
    def test_double_load_no_duplicates(self, db_with_dims) -> None:
        """AC-4: Re-run does not duplicate records."""
        load_fact_customer_cost(db_with_dims)
        count_first = db_with_dims.fetch_all(
            "SELECT COUNT(*) as cnt FROM fact_customer_cost"
        )[0]["cnt"]

        load_fact_customer_cost(db_with_dims)
        count_second = db_with_dims.fetch_all(
            "SELECT COUNT(*) as cnt FROM fact_customer_cost"
        )[0]["cnt"]

        assert count_first == count_second

    @pytest.mark.t5_idempotency
    def test_double_load_same_costs(self, db_with_dims) -> None:
        """Costs are identical after re-run (C-07 reproducibility)."""
        load_fact_customer_cost(db_with_dims)
        costs_first = db_with_dims.fetch_all("""
            SELECT usage_date, customer_key, service_key, allocated_cost
            FROM fact_customer_cost
            ORDER BY usage_date, customer_key, service_key
        """)

        load_fact_customer_cost(db_with_dims)
        costs_second = db_with_dims.fetch_all("""
            SELECT usage_date, customer_key, service_key, allocated_cost
            FROM fact_customer_cost
            ORDER BY usage_date, customer_key, service_key
        """)

        assert costs_first == costs_second


# ---------------------------------------------------------------------------
# T8: Integration tests — full pipeline
# ---------------------------------------------------------------------------


class TestFactCustomerCostIntegration:
    """T8: End-to-end load from raw CUR through dimensions to fact."""

    @pytest.mark.t8_integration
    def test_full_pipeline_loads_fact(self) -> None:
        """Full pipeline: extract → dimensions → fact_customer_cost."""
        db = DuckDBConnector(database=":memory:")
        # Extract
        load_cur_parquet(db, FIXTURES_DIR / "sample_cur.parquet")
        # Dimensions
        dim_svc_count = load_dim_service(db)
        assert dim_svc_count > 0
        dim_cust_count = load_dim_customer(
            db, FIXTURES_DIR / "sample_customers_v1.parquet",
            snapshot_date="2025-01-01"
        )
        assert dim_cust_count > 0
        # Fact
        fact_count = load_fact_customer_cost(db)
        assert fact_count > 0
        db.close()

    @pytest.mark.t8_integration
    def test_fact_joins_to_both_dimensions(self, db_with_dims) -> None:
        """AC-2: Fact FK joins to both dim_customer and dim_service."""
        load_fact_customer_cost(db_with_dims)

        rows = db_with_dims.fetch_all("""
            SELECT f.usage_date,
                   c.segment,
                   d.product_code,
                   f.allocated_cost
            FROM fact_customer_cost f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            JOIN dim_service d ON f.service_key = d.service_key
            ORDER BY f.usage_date, c.segment, d.product_code
            LIMIT 5
        """)
        assert len(rows) == 5
        for row in rows:
            assert row["segment"] is not None
            assert row["product_code"] is not None
            assert row["allocated_cost"] is not None

    @pytest.mark.t8_integration
    def test_scd2_full_pipeline_with_segment_changes(self) -> None:
        """AC-3 + AC-6: Full pipeline with SCD2 — segment reporting correct."""
        db = DuckDBConnector(database=":memory:")
        load_cur_parquet(db, FIXTURES_DIR / "sample_cur.parquet")
        load_dim_service(db)
        load_dim_customer(db, FIXTURES_DIR / "sample_customers_v1.parquet",
                          snapshot_date="2025-01-01")
        load_dim_customer(db, FIXTURES_DIR / "sample_customers_v2.parquet",
                          snapshot_date="2025-01-15")

        count = load_fact_customer_cost(db)
        assert count > 0

        # Verify segment aggregation works across SCD2 boundary
        segment_costs = db.fetch_all("""
            SELECT c.segment,
                   ROUND(SUM(f.allocated_cost), 2) AS total_cost
            FROM fact_customer_cost f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.segment
            ORDER BY c.segment
        """)
        assert len(segment_costs) > 0
        # Total allocated cost should equal total raw cost
        total_allocated = sum(r["total_cost"] for r in segment_costs)
        raw_total = db.fetch_all("""
            SELECT ROUND(SUM(COALESCE(CAST(line_item_unblended_cost AS DOUBLE), 0.0)), 2) as total
            FROM raw_cur
        """)[0]["total"]
        # Allow small rounding tolerance (30 customers × 30 days × 12 services)
        assert abs(total_allocated - raw_total) < 1.0, (
            f"Total allocated {total_allocated} != raw total {raw_total}"
        )

        db.close()
