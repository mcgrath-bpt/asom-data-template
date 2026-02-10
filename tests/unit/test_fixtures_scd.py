"""
Tests for expanded SCD fixture data.

Validates that generated fixtures meet the acceptance criteria
for S004: Expand fixture data for SCD testing.

Test taxonomy: T1 (logic), T2 (contract/schema), T7 (reproducibility)

Story: S004 -- Expand fixture data for SCD testing
Controls: C-07 (Reproducibility)
"""

from pathlib import Path

import polars as pl
import pytest


FIXTURES_DIR = Path("tests/fixtures")


# ---------------------------------------------------------------------------
# T2: Contract tests -- customer snapshot schema
# ---------------------------------------------------------------------------


class TestCustomerSnapshotSchema:
    """T2: Both customer snapshots must share the same schema."""

    EXPECTED_COLUMNS = {
        "customer_id",
        "email",
        "phone",
        "first_name",
        "last_name",
        "segment",
        "created_at",
    }

    @pytest.mark.t2_contract
    def test_v1_has_expected_columns(self) -> None:
        """Baseline snapshot has all required customer columns."""
        df = pl.read_parquet(FIXTURES_DIR / "sample_customers_v1.parquet")
        assert set(df.columns) == self.EXPECTED_COLUMNS

    @pytest.mark.t2_contract
    def test_v2_has_expected_columns(self) -> None:
        """Later snapshot has all required customer columns."""
        df = pl.read_parquet(FIXTURES_DIR / "sample_customers_v2.parquet")
        assert set(df.columns) == self.EXPECTED_COLUMNS

    @pytest.mark.t2_contract
    def test_v1_and_v2_schemas_match(self) -> None:
        """AC3: Both snapshots share the same schema."""
        v1 = pl.read_parquet(FIXTURES_DIR / "sample_customers_v1.parquet")
        v2 = pl.read_parquet(FIXTURES_DIR / "sample_customers_v2.parquet")
        assert v1.schema == v2.schema


# ---------------------------------------------------------------------------
# T1: Logic tests -- customer snapshot content
# ---------------------------------------------------------------------------


class TestCustomerSnapshotContent:
    """T1: Customer snapshots meet size and change requirements."""

    @pytest.mark.t1_logic
    def test_v1_has_minimum_customers(self) -> None:
        """AC1: Baseline snapshot has 25+ customers."""
        df = pl.read_parquet(FIXTURES_DIR / "sample_customers_v1.parquet")
        assert len(df) >= 25

    @pytest.mark.t1_logic
    def test_v1_has_all_segments(self) -> None:
        """AC1: Baseline has premium, standard, and enterprise segments."""
        df = pl.read_parquet(FIXTURES_DIR / "sample_customers_v1.parquet")
        segments = set(df["segment"].to_list())
        assert segments == {"premium", "standard", "enterprise"}

    @pytest.mark.t1_logic
    def test_v2_has_segment_changes(self) -> None:
        """AC2: At least 5 customers changed segment between v1 and v2."""
        v1 = pl.read_parquet(FIXTURES_DIR / "sample_customers_v1.parquet")
        v2 = pl.read_parquet(FIXTURES_DIR / "sample_customers_v2.parquet")

        # Join on customer_id, compare segments
        merged = v1.select(["customer_id", "segment"]).join(
            v2.select(["customer_id", "segment"]),
            on="customer_id",
            suffix="_v2",
        )
        changed = merged.filter(pl.col("segment") != pl.col("segment_v2"))
        assert len(changed) >= 5, f"Only {len(changed)} segment changes, need >= 5"

    @pytest.mark.t1_logic
    def test_v2_has_new_customers(self) -> None:
        """AC2: At least 3 new customers in v2 that were not in v1."""
        v1 = pl.read_parquet(FIXTURES_DIR / "sample_customers_v1.parquet")
        v2 = pl.read_parquet(FIXTURES_DIR / "sample_customers_v2.parquet")

        v1_ids = set(v1["customer_id"].to_list())
        v2_ids = set(v2["customer_id"].to_list())
        new_ids = v2_ids - v1_ids
        assert len(new_ids) >= 3, f"Only {len(new_ids)} new customers, need >= 3"

    @pytest.mark.t1_logic
    def test_v2_has_contact_changes(self) -> None:
        """AC2: At least 2 customers changed contact details between v1 and v2."""
        v1 = pl.read_parquet(FIXTURES_DIR / "sample_customers_v1.parquet")
        v2 = pl.read_parquet(FIXTURES_DIR / "sample_customers_v2.parquet")

        merged = v1.select(["customer_id", "email", "phone"]).join(
            v2.select(["customer_id", "email", "phone"]),
            on="customer_id",
            suffix="_v2",
        )
        contact_changed = merged.filter(
            (pl.col("email") != pl.col("email_v2"))
            | (pl.col("phone") != pl.col("phone_v2"))
        )
        assert len(contact_changed) >= 2, (
            f"Only {len(contact_changed)} contact changes, need >= 2"
        )

    @pytest.mark.t1_logic
    def test_customer_ids_are_unique_in_v1(self) -> None:
        """Each customer_id appears exactly once in v1."""
        df = pl.read_parquet(FIXTURES_DIR / "sample_customers_v1.parquet")
        assert df["customer_id"].n_unique() == len(df)

    @pytest.mark.t1_logic
    def test_customer_ids_are_unique_in_v2(self) -> None:
        """Each customer_id appears exactly once in v2."""
        df = pl.read_parquet(FIXTURES_DIR / "sample_customers_v2.parquet")
        assert df["customer_id"].n_unique() == len(df)


# ---------------------------------------------------------------------------
# T1: Logic tests -- CUR fixture for dim_service
# ---------------------------------------------------------------------------


class TestCurServiceCombinations:
    """T1: CUR fixture has sufficient service/usage-type diversity."""

    @pytest.mark.t1_logic
    def test_cur_has_distinct_service_usage_combos(self) -> None:
        """AC4: CUR data has more than 5 distinct (product_code, usage_type) combos."""
        df = pl.read_parquet(FIXTURES_DIR / "sample_cur.parquet")
        combos = df.select([
            "line_item_product_code",
            "line_item_usage_type",
        ]).unique()
        assert len(combos) > 5, (
            f"Only {len(combos)} distinct service/usage combos, need > 5"
        )


# ---------------------------------------------------------------------------
# T1: Reproducibility -- deterministic generation
# ---------------------------------------------------------------------------


class TestFixtureReproducibility:
    """T1/C-07: Fixtures are deterministic."""

    @pytest.mark.t1_logic
    def test_v1_is_deterministic(self) -> None:
        """AC5: Reading v1 twice yields identical content."""
        df1 = pl.read_parquet(FIXTURES_DIR / "sample_customers_v1.parquet")
        df2 = pl.read_parquet(FIXTURES_DIR / "sample_customers_v1.parquet")
        assert df1.equals(df2)

    @pytest.mark.t1_logic
    def test_v2_is_deterministic(self) -> None:
        """AC5: Reading v2 twice yields identical content."""
        df1 = pl.read_parquet(FIXTURES_DIR / "sample_customers_v2.parquet")
        df2 = pl.read_parquet(FIXTURES_DIR / "sample_customers_v2.parquet")
        assert df1.equals(df2)
