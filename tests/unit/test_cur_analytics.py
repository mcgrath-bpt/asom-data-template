"""
Tests for CUR cost analytics query layer.

Verifies top-N services, month-over-month change, and anomaly detection.

Test taxonomy: T1 (logic), T8 (integration -- end-to-end pipeline)

Reference: ASOM framework -- skills/testing-strategies.md
Story: S003 -- Build cost analytics query layer
"""

from pathlib import Path

import polars as pl
import pytest

from src.extract.cur_loader import load_cur_parquet
from src.transform.cur_analytics import (
    detect_cost_anomalies,
    month_over_month_change,
    top_services_by_cost,
)
from src.transform.cur_transformer import build_daily_cost_summary


FIXTURES_DIR = Path("tests/fixtures")


@pytest.fixture()
def summary_df(local_db):
    """Daily cost summary from fixture CUR data."""
    load_cur_parquet(local_db, FIXTURES_DIR / "sample_cur.parquet")
    return build_daily_cost_summary(local_db)


# ---------------------------------------------------------------------------
# T1: Logic tests -- analytics queries
# ---------------------------------------------------------------------------


class TestTopServicesByCost:
    """T1: Top-N services by total cost."""

    @pytest.mark.t1_logic
    def test_returns_dataframe(self, summary_df) -> None:
        result = top_services_by_cost(summary_df)
        assert isinstance(result, pl.DataFrame)

    @pytest.mark.t1_logic
    def test_default_top_10(self, summary_df) -> None:
        """Default returns up to 10 services."""
        result = top_services_by_cost(summary_df)
        assert result.height <= 10

    @pytest.mark.t1_logic
    def test_top_3(self, summary_df) -> None:
        """Can limit to top N."""
        result = top_services_by_cost(summary_df, n=3)
        assert result.height == 3

    @pytest.mark.t1_logic
    def test_sorted_descending(self, summary_df) -> None:
        """Results are sorted by total_cost descending."""
        result = top_services_by_cost(summary_df, n=5)
        costs = result["total_cost"].to_list()
        assert costs == sorted(costs, reverse=True)

    @pytest.mark.t1_logic
    def test_ec2_is_most_expensive(self, summary_df) -> None:
        """AmazonEC2 should be the most expensive service (highest base cost)."""
        result = top_services_by_cost(summary_df, n=1)
        assert result["service_name"][0] == "AmazonEC2"


class TestMonthOverMonthChange:
    """T1: Month-over-month cost change percentage."""

    @pytest.mark.t1_logic
    def test_returns_dataframe(self, summary_df) -> None:
        result = month_over_month_change(summary_df)
        assert isinstance(result, pl.DataFrame)

    @pytest.mark.t1_logic
    def test_has_pct_change_column(self, summary_df) -> None:
        result = month_over_month_change(summary_df)
        assert "pct_change" in result.columns

    @pytest.mark.t1_logic
    def test_has_service_and_month(self, summary_df) -> None:
        result = month_over_month_change(summary_df)
        assert "service_name" in result.columns
        assert "month" in result.columns


class TestDetectCostAnomalies:
    """T1: Cost anomaly detection (>20% week-over-week increase)."""

    @pytest.mark.t1_logic
    def test_returns_dataframe(self, summary_df) -> None:
        result = detect_cost_anomalies(summary_df)
        assert isinstance(result, pl.DataFrame)

    @pytest.mark.t1_logic
    def test_has_expected_columns(self, summary_df) -> None:
        result = detect_cost_anomalies(summary_df)
        expected = {"service_name", "week", "weekly_cost", "prev_weekly_cost", "pct_change"}
        assert expected.issubset(set(result.columns))

    @pytest.mark.t1_logic
    def test_only_increases_above_threshold(self, summary_df) -> None:
        """Only flags services with >20% increase."""
        result = detect_cost_anomalies(summary_df, threshold=0.20)
        if result.height > 0:
            assert all(result["pct_change"] > 0.20)


# ---------------------------------------------------------------------------
# T8: Integration test -- end-to-end pipeline
# ---------------------------------------------------------------------------


class TestEndToEndPipeline:
    """T8: Full pipeline from Parquet to analytics."""

    @pytest.mark.t8_integration
    def test_full_pipeline(self, local_db) -> None:
        """Load -> Transform -> Analyze works end-to-end."""
        # Extract
        count = load_cur_parquet(local_db, FIXTURES_DIR / "sample_cur.parquet")
        assert count == 361

        # Transform
        summary = build_daily_cost_summary(local_db)
        assert summary.height > 0

        # Analyze
        top = top_services_by_cost(summary, n=5)
        assert top.height == 5

        mom = month_over_month_change(summary)
        assert isinstance(mom, pl.DataFrame)

        anomalies = detect_cost_anomalies(summary)
        assert isinstance(anomalies, pl.DataFrame)
