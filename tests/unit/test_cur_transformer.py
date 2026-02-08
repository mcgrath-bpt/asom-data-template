"""
Tests for CUR cost transformation logic.

Verifies daily cost aggregation, NULL handling, and 7-day moving average
trend calculation for AWS CUR data.

Test taxonomy: T1 (logic), T3 (data quality)

Reference: ASOM framework -- skills/testing-strategies.md
Story: S002 -- Transform CUR data into daily cost summary
"""

from pathlib import Path

import polars as pl
import pytest

from src.extract.cur_loader import load_cur_parquet
from src.transform.cur_transformer import (
    build_daily_cost_summary,
    compute_trend,
)


FIXTURES_DIR = Path("tests/fixtures")


@pytest.fixture()
def loaded_db(local_db):
    """In-memory DuckDB with CUR data loaded."""
    load_cur_parquet(local_db, FIXTURES_DIR / "sample_cur.parquet")
    return local_db


# ---------------------------------------------------------------------------
# T1: Logic tests -- daily cost aggregation
# ---------------------------------------------------------------------------


class TestBuildDailyCostSummary:
    """T1: Daily cost summary aggregation logic."""

    @pytest.mark.t1_logic
    def test_returns_polars_dataframe(self, loaded_db) -> None:
        """build_daily_cost_summary returns a polars DataFrame."""
        df = build_daily_cost_summary(loaded_db)
        assert isinstance(df, pl.DataFrame)

    @pytest.mark.t1_logic
    def test_has_expected_columns(self, loaded_db) -> None:
        """Output has expected schema: usage_date, service_name, daily_cost, record_count."""
        df = build_daily_cost_summary(loaded_db)
        expected = {"usage_date", "service_name", "daily_cost", "record_count"}
        assert expected.issubset(set(df.columns))

    @pytest.mark.t1_logic
    def test_aggregates_by_date_and_service(self, loaded_db) -> None:
        """Each row is unique by (usage_date, service_name)."""
        df = build_daily_cost_summary(loaded_db)
        unique_count = df.select(["usage_date", "service_name"]).unique().height
        assert unique_count == df.height

    @pytest.mark.t1_logic
    def test_covers_all_services(self, loaded_db) -> None:
        """All 5 services from fixture data appear in summary."""
        df = build_daily_cost_summary(loaded_db)
        services = df["service_name"].unique().to_list()
        expected_services = ["AmazonEC2", "AmazonS3", "AmazonRDS", "AWSLambda", "AmazonCloudWatch"]
        for svc in expected_services:
            assert svc in services, f"Missing service: {svc}"

    @pytest.mark.t1_logic
    def test_covers_all_dates(self, loaded_db) -> None:
        """All 30 days from fixture data appear in summary."""
        df = build_daily_cost_summary(loaded_db)
        dates = df["usage_date"].unique().to_list()
        assert len(dates) == 30

    @pytest.mark.t1_logic
    def test_record_count_correct(self, loaded_db) -> None:
        """Record count reflects actual rows per (date, service)."""
        df = build_daily_cost_summary(loaded_db)
        # Most date/service combos have 1 row, except 2025-01-15 AmazonEC2 has 2
        # (one regular + one null-cost row)
        ec2_jan15 = df.filter(
            (pl.col("service_name") == "AmazonEC2")
            & (pl.col("usage_date") == "2025-01-15")
        )
        assert ec2_jan15["record_count"][0] == 2


# ---------------------------------------------------------------------------
# T3: Data quality tests -- NULL handling, cost values
# ---------------------------------------------------------------------------


class TestDailyCostQuality:
    """T3: Data quality checks for cost summary."""

    @pytest.mark.t3_quality
    def test_null_costs_treated_as_zero(self, loaded_db) -> None:
        """NULL unblended_cost values are treated as 0.00 in aggregation."""
        df = build_daily_cost_summary(loaded_db)
        # No NULLs in daily_cost output
        null_count = df.filter(pl.col("daily_cost").is_null()).height
        assert null_count == 0

    @pytest.mark.t3_quality
    def test_all_costs_non_negative(self, loaded_db) -> None:
        """All daily costs are >= 0."""
        df = build_daily_cost_summary(loaded_db)
        negative_count = df.filter(pl.col("daily_cost") < 0).height
        assert negative_count == 0

    @pytest.mark.t3_quality
    def test_no_empty_service_names(self, loaded_db) -> None:
        """No empty or null service names."""
        df = build_daily_cost_summary(loaded_db)
        empty_count = df.filter(
            pl.col("service_name").is_null() | (pl.col("service_name") == "")
        ).height
        assert empty_count == 0


# ---------------------------------------------------------------------------
# T1: Logic tests -- trend calculation
# ---------------------------------------------------------------------------


class TestComputeTrend:
    """T1: 7-day moving average trend calculation."""

    @pytest.mark.t1_logic
    def test_trend_has_moving_avg_column(self, loaded_db) -> None:
        """compute_trend adds a cost_7d_avg column."""
        summary = build_daily_cost_summary(loaded_db)
        trend = compute_trend(summary)
        assert "cost_7d_avg" in trend.columns

    @pytest.mark.t1_logic
    def test_trend_preserves_rows(self, loaded_db) -> None:
        """Trend calculation doesn't drop any rows."""
        summary = build_daily_cost_summary(loaded_db)
        trend = compute_trend(summary)
        assert trend.height == summary.height

    @pytest.mark.t1_logic
    def test_first_days_have_null_or_partial_avg(self, loaded_db) -> None:
        """First 6 days of each service should have NULL moving average (not enough data)."""
        summary = build_daily_cost_summary(loaded_db)
        trend = compute_trend(summary)
        # For any single service, the first 6 entries (days 1-6) should be null
        ec2_trend = trend.filter(pl.col("service_name") == "AmazonEC2").sort("usage_date")
        assert ec2_trend["cost_7d_avg"][0] is None  # Day 1 has no 7-day window

    @pytest.mark.t1_logic
    def test_seventh_day_has_valid_avg(self, loaded_db) -> None:
        """By day 7, the moving average should be populated."""
        summary = build_daily_cost_summary(loaded_db)
        trend = compute_trend(summary)
        ec2_trend = trend.filter(pl.col("service_name") == "AmazonEC2").sort("usage_date")
        # Day 7 (index 6) should have a valid average
        assert ec2_trend["cost_7d_avg"][6] is not None
        assert ec2_trend["cost_7d_avg"][6] > 0
