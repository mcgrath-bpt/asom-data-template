"""
CUR cost analytics query layer.

Provides analytical queries on top of the daily cost summary:
top-N services, month-over-month change, and cost anomaly detection.

Story: S003 -- Build cost analytics query layer
Controls: C-06 (DQ)
"""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)


def top_services_by_cost(summary: pl.DataFrame, n: int = 10) -> pl.DataFrame:
    """Return the top N services ranked by total cost.

    Args:
        summary: Daily cost summary DataFrame (from build_daily_cost_summary).
        n: Number of top services to return (default 10).

    Returns:
        DataFrame with columns: service_name, total_cost -- sorted descending.
    """
    result = (
        summary.group_by("service_name")
        .agg(pl.col("daily_cost").sum().alias("total_cost"))
        .sort("total_cost", descending=True)
        .head(n)
    )
    logger.info("Top %d services by cost computed", n)
    return result


def month_over_month_change(summary: pl.DataFrame) -> pl.DataFrame:
    """Calculate month-over-month cost change percentage per service.

    Args:
        summary: Daily cost summary DataFrame.

    Returns:
        DataFrame with columns: service_name, month, monthly_cost, pct_change.
        First month per service will have NULL pct_change.
    """
    # Extract month from usage_date string (YYYY-MM-DD -> YYYY-MM)
    monthly = (
        summary.with_columns(pl.col("usage_date").str.slice(0, 7).alias("month"))
        .group_by(["service_name", "month"])
        .agg(pl.col("daily_cost").sum().alias("monthly_cost"))
        .sort(["service_name", "month"])
    )

    # Calculate pct_change within each service
    result = monthly.with_columns(
        (
            (pl.col("monthly_cost") - pl.col("monthly_cost").shift(1).over("service_name"))
            / pl.col("monthly_cost").shift(1).over("service_name")
        ).alias("pct_change")
    )

    logger.info("Month-over-month change computed for %d service-months", result.height)
    return result


def detect_cost_anomalies(summary: pl.DataFrame, threshold: float = 0.20) -> pl.DataFrame:
    """Flag services with week-over-week cost increase above threshold.

    Args:
        summary: Daily cost summary DataFrame.
        threshold: Minimum percentage increase to flag (default 0.20 = 20%).

    Returns:
        DataFrame of anomalies with columns:
        service_name, week, weekly_cost, prev_weekly_cost, pct_change.
        Only rows where pct_change > threshold are returned.
    """
    # Extract ISO week from usage_date
    weekly = (
        summary.with_columns(pl.col("usage_date").str.to_date("%Y-%m-%d").dt.strftime("%Y-W%W").alias("week"))
        .group_by(["service_name", "week"])
        .agg(pl.col("daily_cost").sum().alias("weekly_cost"))
        .sort(["service_name", "week"])
    )

    # Calculate week-over-week change
    with_change = weekly.with_columns(
        pl.col("weekly_cost").shift(1).over("service_name").alias("prev_weekly_cost"),
    ).with_columns(
        ((pl.col("weekly_cost") - pl.col("prev_weekly_cost")) / pl.col("prev_weekly_cost")).alias("pct_change")
    )

    # Filter to anomalies only
    anomalies = with_change.filter(pl.col("pct_change") > threshold).drop_nulls("pct_change")

    logger.info("Detected %d cost anomalies (>%.0f%% WoW increase)", anomalies.height, threshold * 100)
    return anomalies
