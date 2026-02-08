"""
CUR cost transformation logic.

Transforms raw CUR data into daily cost summaries with trend analysis.
NULL costs are treated as 0.00 with logged warnings.

Story: S002 -- Transform CUR data into daily cost summary
Controls: C-06 (DQ)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from src.connector import DBConnector

logger = logging.getLogger(__name__)


def build_daily_cost_summary(db: DBConnector) -> pl.DataFrame:
    """Aggregate raw CUR data into daily cost per service.

    Groups by (usage_date, service_name) and sums unblended_cost.
    NULL costs are treated as 0.00.

    Args:
        db: Database connector with raw_cur table loaded.

    Returns:
        DataFrame with columns: usage_date, service_name, daily_cost, record_count.
    """
    rows = db.fetch_all(
        """
        SELECT
            line_item_usage_start_date AS usage_date,
            line_item_product_code AS service_name,
            COALESCE(CAST(line_item_unblended_cost AS DOUBLE), 0.0) AS cost,
            1 AS row_count
        FROM raw_cur
        """
    )

    if not rows:
        logger.warning("No rows in raw_cur -- returning empty summary")
        return pl.DataFrame(
            schema={
                "usage_date": pl.Utf8,
                "service_name": pl.Utf8,
                "daily_cost": pl.Float64,
                "record_count": pl.UInt32,
            }
        )

    df = pl.DataFrame(rows)

    # Log null cost handling
    null_count = len([r for r in rows if r["cost"] == 0.0])
    if null_count > 0:
        logger.info("Treated %d NULL/zero costs as 0.00", null_count)

    summary = (
        df.group_by(["usage_date", "service_name"])
        .agg(
            pl.col("cost").sum().alias("daily_cost"),
            pl.col("row_count").sum().cast(pl.UInt32).alias("record_count"),
        )
        .sort(["usage_date", "service_name"])
    )

    logger.info(
        "Built daily cost summary: %d rows (%d dates, %d services)",
        summary.height,
        summary["usage_date"].n_unique(),
        summary["service_name"].n_unique(),
    )

    return summary


def compute_trend(summary: pl.DataFrame, window: int = 7) -> pl.DataFrame:
    """Add a rolling moving average of daily_cost per service.

    Args:
        summary: Output of build_daily_cost_summary().
        window: Number of days for rolling average (default 7).

    Returns:
        Input DataFrame with additional cost_7d_avg column.
        First (window-1) days per service will have NULL average.
    """
    result = summary.sort(["service_name", "usage_date"]).with_columns(
        pl.col("daily_cost")
        .rolling_mean(window_size=window, min_samples=window)
        .over("service_name")
        .alias("cost_7d_avg")
    )

    logger.info("Computed %d-day moving average trend", window)
    return result
