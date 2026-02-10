"""
Tests for dim_customer (SCD Type 2) dimension table.

Validates DDL creation, SCD2 versioning logic, PII masking,
and idempotent loading across two snapshot versions.

Test taxonomy: T1 (logic), T2 (contract), T4 (security/PII), T5 (idempotency)

Story: S006 -- Create dim_customer (SCD Type 2) with history tracking
Controls: C-04 (PII), C-05 (Access), C-06 (DQ), C-07 (Reproducibility), C-08 (Idempotency)
"""

from pathlib import Path

import pytest

from src.connector import DuckDBConnector
from src.load.dim_customer_loader import load_dim_customer


FIXTURES_DIR = Path("tests/fixtures")


@pytest.fixture
def empty_db() -> DuckDBConnector:
    """In-memory DuckDB with no data."""
    db = DuckDBConnector(database=":memory:")
    yield db  # type: ignore[misc]
    db.close()


# ---------------------------------------------------------------------------
# T2: Contract tests -- DDL and schema
# ---------------------------------------------------------------------------


class TestDimCustomerSchema:
    """T2: dim_customer table has expected SCD2 structure."""

    @pytest.mark.t2_contract
    def test_table_created(self, empty_db) -> None:
        """Loading creates dim_customer table."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        rows = empty_db.fetch_all("SELECT COUNT(*) as cnt FROM dim_customer")
        assert rows[0]["cnt"] > 0

    @pytest.mark.t2_contract
    def test_has_scd2_columns(self, empty_db) -> None:
        """dim_customer has SCD2 versioning columns."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        df = empty_db.fetch_df("SELECT * FROM dim_customer LIMIT 1")
        expected = {
            "customer_key",
            "customer_id",
            "email_token",
            "phone_redacted",
            "first_name",
            "last_name",
            "segment",
            "effective_from",
            "effective_to",
            "is_current",
            "_loaded_at",
        }
        assert expected.issubset(set(df.columns)), (
            f"Missing columns: {expected - set(df.columns)}"
        )


# ---------------------------------------------------------------------------
# T1: Logic tests -- initial load (v1)
# ---------------------------------------------------------------------------


class TestDimCustomerInitialLoad:
    """T1: First load inserts all customers as current records."""

    @pytest.mark.t1_logic
    def test_v1_loads_all_customers(self, empty_db) -> None:
        """AC2: All 30 v1 customers inserted."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        rows = empty_db.fetch_all("SELECT COUNT(*) as cnt FROM dim_customer")
        assert rows[0]["cnt"] == 30

    @pytest.mark.t1_logic
    def test_all_records_are_current(self, empty_db) -> None:
        """AC2: All initial records have is_current = TRUE."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        rows = empty_db.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_customer WHERE is_current = TRUE"
        )
        assert rows[0]["cnt"] == 30

    @pytest.mark.t1_logic
    def test_effective_to_is_null_for_current(self, empty_db) -> None:
        """AC2: Current records have effective_to = NULL."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        rows = empty_db.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_customer WHERE effective_to IS NOT NULL"
        )
        assert rows[0]["cnt"] == 0

    @pytest.mark.t1_logic
    def test_customer_keys_unique(self, empty_db) -> None:
        """Surrogate keys are unique."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        rows = empty_db.fetch_all("""
            SELECT COUNT(*) as total, COUNT(DISTINCT customer_key) as unique_keys
            FROM dim_customer
        """)
        assert rows[0]["total"] == rows[0]["unique_keys"]


# ---------------------------------------------------------------------------
# T1: Logic tests -- SCD2 versioning (v1 then v2)
# ---------------------------------------------------------------------------


class TestDimCustomerSCD2:
    """T1: Loading v2 after v1 produces correct SCD2 history."""

    @pytest.mark.t1_logic
    def test_segment_change_creates_new_version(self, empty_db) -> None:
        """AC3: Segment change expires old record, creates new current."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v2.parquet")

        # Customer 2 changed: standard -> premium
        rows = empty_db.fetch_all("""
            SELECT * FROM dim_customer
            WHERE customer_id = 2
            ORDER BY effective_from
        """)
        assert len(rows) == 2, f"Expected 2 versions for customer 2, got {len(rows)}"

        # First version: expired
        assert rows[0]["segment"] == "standard"
        assert rows[0]["is_current"] is False
        assert rows[0]["effective_to"] is not None

        # Second version: current
        assert rows[1]["segment"] == "premium"
        assert rows[1]["is_current"] is True
        assert rows[1]["effective_to"] is None

    @pytest.mark.t1_logic
    def test_new_customers_inserted(self, empty_db) -> None:
        """AC2: New customers in v2 are inserted as current."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v2.parquet")

        # Customer 31 is new in v2
        rows = empty_db.fetch_all(
            "SELECT * FROM dim_customer WHERE customer_id = 31"
        )
        assert len(rows) == 1
        assert rows[0]["is_current"] is True

    @pytest.mark.t1_logic
    def test_unchanged_customers_not_duplicated(self, empty_db) -> None:
        """AC4: Customers with no changes have exactly one record."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v2.parquet")

        # Customer 1 (Alice) had no segment change
        rows = empty_db.fetch_all(
            "SELECT * FROM dim_customer WHERE customer_id = 1"
        )
        assert len(rows) == 1
        assert rows[0]["is_current"] is True

    @pytest.mark.t1_logic
    def test_all_segment_changes_tracked(self, empty_db) -> None:
        """AC3: All 6 segment changes produce 2 versions each."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v2.parquet")

        changed_ids = [2, 4, 7, 11, 15, 22]
        for cid in changed_ids:
            rows = empty_db.fetch_all(
                f"SELECT COUNT(*) as cnt FROM dim_customer WHERE customer_id = {cid}"
            )
            assert rows[0]["cnt"] == 2, (
                f"Customer {cid} should have 2 versions, got {rows[0]['cnt']}"
            )

    @pytest.mark.t1_logic
    def test_current_count_equals_active_customers(self, empty_db) -> None:
        """AC7: is_current=TRUE count = number of distinct active customers."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v2.parquet")

        current_count = empty_db.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_customer WHERE is_current = TRUE"
        )[0]["cnt"]
        # 30 original + 3 new = 33 active customers
        assert current_count == 33

    @pytest.mark.t1_logic
    def test_total_records_correct(self, empty_db) -> None:
        """Total = 30 originals + 6 new versions (changed) + 3 new customers = 39."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v2.parquet")

        total = empty_db.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_customer"
        )[0]["cnt"]
        assert total == 39


# ---------------------------------------------------------------------------
# T4: PII security tests
# ---------------------------------------------------------------------------


class TestDimCustomerPII:
    """T4: PII is properly masked in dim_customer."""

    @pytest.mark.t4_access
    def test_email_is_masked(self, empty_db) -> None:
        """AC5: No raw email addresses in dim_customer."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        rows = empty_db.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_customer WHERE email_token LIKE '%@%'"
        )
        assert rows[0]["cnt"] == 0, "Raw emails found in dim_customer"

    @pytest.mark.t4_access
    def test_email_is_sha256_hex(self, empty_db) -> None:
        """AC5: email_token is a 64-char hex string (SHA256)."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        rows = empty_db.fetch_all(
            "SELECT email_token FROM dim_customer LIMIT 1"
        )
        token = rows[0]["email_token"]
        assert len(token) == 64
        assert all(c in "0123456789abcdef" for c in token)

    @pytest.mark.t4_access
    def test_phone_is_redacted(self, empty_db) -> None:
        """AC5: phone_redacted matches XXX-XXX-NNNN format."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        rows = empty_db.fetch_all("SELECT phone_redacted FROM dim_customer")
        for row in rows:
            phone = row["phone_redacted"]
            assert phone.startswith("XXX-XXX-"), f"Phone not redacted: {phone}"
            assert len(phone) == 12, f"Unexpected phone format: {phone}"


# ---------------------------------------------------------------------------
# T5: Idempotency tests
# ---------------------------------------------------------------------------


class TestDimCustomerIdempotency:
    """T5: Reloading the same snapshot does not create duplicates."""

    @pytest.mark.t5_idempotency
    def test_double_load_same_snapshot(self, empty_db) -> None:
        """AC4: Loading v1 twice produces 30 records (not 60)."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        rows = empty_db.fetch_all("SELECT COUNT(*) as cnt FROM dim_customer")
        assert rows[0]["cnt"] == 30

    @pytest.mark.t5_idempotency
    def test_triple_load_v1_v2_v2(self, empty_db) -> None:
        """AC4: Loading v1, v2, v2 produces same result as v1, v2."""
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v1.parquet")
        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v2.parquet")
        count_after_v2 = empty_db.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_customer"
        )[0]["cnt"]

        load_dim_customer(empty_db, FIXTURES_DIR / "sample_customers_v2.parquet")
        count_after_v2_again = empty_db.fetch_all(
            "SELECT COUNT(*) as cnt FROM dim_customer"
        )[0]["cnt"]

        assert count_after_v2 == count_after_v2_again
