"""
Tests for CUR (Cost & Usage Report) data loader.

Verifies Parquet loading, schema validation, idempotent insert,
and null handling for AWS CUR data.

Test taxonomy: T1 (logic), T2 (contract/schema), T5 (idempotency)

Reference: ASOM framework -- skills/testing-strategies.md
Story: S001 -- Load CUR Parquet files into raw layer
"""

from pathlib import Path

import polars as pl
import pytest

from src.extract.cur_loader import (
    CUR_EXPECTED_COLUMNS,
    load_cur_parquet,
    validate_cur_schema,
)


FIXTURES_DIR = Path("tests/fixtures")


# ---------------------------------------------------------------------------
# T1: Logic tests -- CUR loading
# ---------------------------------------------------------------------------


class TestLoadCurParquet:
    """T1: Load CUR Parquet files into a connector."""

    @pytest.mark.t1_logic
    def test_load_returns_row_count(self, local_db) -> None:
        """Loading a valid Parquet file returns the number of rows loaded."""
        count = load_cur_parquet(local_db, FIXTURES_DIR / "sample_cur.parquet")
        assert count == 151

    @pytest.mark.t1_logic
    def test_loaded_data_queryable(self, local_db) -> None:
        """After loading, data is queryable from raw_cur table."""
        load_cur_parquet(local_db, FIXTURES_DIR / "sample_cur.parquet")
        rows = local_db.fetch_all("SELECT COUNT(*) as cnt FROM raw_cur")
        assert rows[0]["cnt"] == 151

    @pytest.mark.t1_logic
    def test_loaded_columns_present(self, local_db) -> None:
        """All expected CUR columns are present in loaded table."""
        load_cur_parquet(local_db, FIXTURES_DIR / "sample_cur.parquet")
        df = local_db.fetch_df("SELECT * FROM raw_cur LIMIT 1")
        for col in CUR_EXPECTED_COLUMNS:
            assert col in df.columns, f"Missing expected column: {col}"

    @pytest.mark.t1_logic
    def test_null_cost_preserved(self, local_db) -> None:
        """NULL costs are preserved in loaded data (not silently dropped)."""
        load_cur_parquet(local_db, FIXTURES_DIR / "sample_cur.parquet")
        rows = local_db.fetch_all(
            "SELECT * FROM raw_cur WHERE line_item_unblended_cost IS NULL"
        )
        assert len(rows) == 1
        assert rows[0]["identity_line_item_id"] == "lid-null-test"


# ---------------------------------------------------------------------------
# T2: Contract tests -- schema validation
# ---------------------------------------------------------------------------


class TestValidateCurSchema:
    """T2: Schema validation for CUR data."""

    @pytest.mark.t2_contract
    def test_valid_parquet_passes(self) -> None:
        """A valid CUR Parquet file passes schema validation."""
        df = pl.read_parquet(FIXTURES_DIR / "sample_cur.parquet")
        errors = validate_cur_schema(df)
        assert errors == []

    @pytest.mark.t2_contract
    def test_missing_column_detected(self) -> None:
        """Schema validation catches missing required columns."""
        df = pl.DataFrame({"identity_line_item_id": ["a"], "bogus_col": [1]})
        errors = validate_cur_schema(df)
        assert len(errors) > 0
        assert any("line_item_product_code" in e for e in errors)

    @pytest.mark.t2_contract
    def test_empty_dataframe_detected(self) -> None:
        """Schema validation catches empty DataFrames."""
        df = pl.DataFrame()
        errors = validate_cur_schema(df)
        assert len(errors) > 0


# ---------------------------------------------------------------------------
# T5: Idempotency tests -- re-run safety
# ---------------------------------------------------------------------------


class TestIdempotentLoad:
    """T5: Loading the same file twice does not duplicate data."""

    @pytest.mark.t5_idempotency
    def test_double_load_no_duplicates(self, local_db) -> None:
        """Loading same Parquet file twice yields same row count."""
        load_cur_parquet(local_db, FIXTURES_DIR / "sample_cur.parquet")
        load_cur_parquet(local_db, FIXTURES_DIR / "sample_cur.parquet")
        rows = local_db.fetch_all("SELECT COUNT(*) as cnt FROM raw_cur")
        assert rows[0]["cnt"] == 151  # Not 302

    @pytest.mark.t5_idempotency
    def test_idempotent_load_returns_zero_on_rerun(self, local_db) -> None:
        """Second load returns 0 (no new rows inserted)."""
        first_count = load_cur_parquet(local_db, FIXTURES_DIR / "sample_cur.parquet")
        second_count = load_cur_parquet(local_db, FIXTURES_DIR / "sample_cur.parquet")
        assert first_count == 151
        assert second_count == 0
