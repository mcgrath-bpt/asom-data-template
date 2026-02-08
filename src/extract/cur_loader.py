"""
CUR (Cost & Usage Report) data loader.

Loads AWS CUR Parquet files into the raw layer via the connector abstraction.
Supports idempotent loading -- re-running does not duplicate data.

Story: S001 -- Load CUR Parquet files into raw layer
Controls: C-06 (DQ), C-07 (Reproducibility)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from src.connector import DBConnector

logger = logging.getLogger(__name__)

# Expected columns in a valid CUR Parquet export (subset used by this pipeline)
CUR_EXPECTED_COLUMNS: list[str] = [
    "identity_line_item_id",
    "identity_time_interval",
    "bill_payer_account_id",
    "line_item_usage_account_id",
    "line_item_line_item_type",
    "line_item_usage_start_date",
    "line_item_usage_end_date",
    "line_item_product_code",
    "line_item_usage_type",
    "line_item_operation",
    "line_item_usage_amount",
    "line_item_unblended_cost",
    "line_item_blended_cost",
    "line_item_currency_code",
]


def validate_cur_schema(df: pl.DataFrame) -> list[str]:
    """Validate that a DataFrame has the expected CUR schema.

    Args:
        df: DataFrame to validate.

    Returns:
        List of error messages. Empty list means valid.
    """
    errors: list[str] = []

    if len(df.columns) == 0:
        errors.append("DataFrame has no columns")
        return errors

    missing = [col for col in CUR_EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")

    return errors


def load_cur_parquet(db: DBConnector, parquet_path: Path | str) -> int:
    """Load a CUR Parquet file into the raw_cur table.

    Idempotent: uses identity_line_item_id to skip already-loaded rows.

    Args:
        db: Database connector (DuckDB, SQLite, or Snowflake).
        parquet_path: Path to the CUR Parquet file.

    Returns:
        Number of new rows inserted.

    Raises:
        ValueError: If the Parquet file fails schema validation.
    """
    parquet_path = Path(parquet_path)
    if not parquet_path.exists():
        raise FileNotFoundError(f"CUR Parquet file not found: {parquet_path}")

    logger.info("Loading CUR data from %s", parquet_path)

    # Read and validate schema before any DB operations
    df = pl.read_parquet(parquet_path)
    errors = validate_cur_schema(df)
    if errors:
        raise ValueError(f"CUR schema validation failed: {'; '.join(errors)}")

    logger.info("Read %d rows, %d columns from %s", len(df), len(df.columns), parquet_path.name)

    # Ensure raw_cur table exists
    col_defs = ", ".join(f"{col} TEXT" for col in CUR_EXPECTED_COLUMNS)
    db.execute(f"CREATE TABLE IF NOT EXISTS raw_cur ({col_defs})")

    # Check which rows are already loaded (idempotency via identity_line_item_id)
    existing_rows = db.fetch_all("SELECT identity_line_item_id FROM raw_cur")
    existing_ids = {row["identity_line_item_id"] for row in existing_rows}

    # Filter to new rows only
    new_rows = df.filter(~pl.col("identity_line_item_id").is_in(list(existing_ids)))

    if len(new_rows) == 0:
        logger.info("No new rows to load (all %d rows already exist)", len(df))
        return 0

    # Insert new rows row-by-row (compatible with all backends)
    cols = ", ".join(CUR_EXPECTED_COLUMNS)
    for row in new_rows.iter_rows(named=True):
        values = []
        for col in CUR_EXPECTED_COLUMNS:
            val = row[col]
            if val is None:
                values.append("NULL")
            else:
                values.append(f"'{_sql_escape(str(val))}'")
        db.execute(f"INSERT INTO raw_cur ({cols}) VALUES ({', '.join(values)})")

    inserted = len(new_rows)
    logger.info("Loaded %d new rows into raw_cur", inserted)
    return inserted


def _sql_escape(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")
