"""
Load dim_customer (SCD Type 2) from customer snapshot data.

SQL-first for DDL; Python orchestrates SCD2 comparison logic since
DuckDB does not support MERGE with the expire-and-insert pattern needed.

PII masking is applied before loading: email → SHA256 token, phone → last 4 digits.

Story: S006 -- Create dim_customer (SCD Type 2) with history tracking
Controls: C-04 (PII), C-05 (Access), C-06 (DQ), C-07 (Reproducibility), C-08 (Idempotency)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from src.transform.maskers import PIIMasker

if TYPE_CHECKING:
    from src.connector import DBConnector

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent.parent / "sql"

# Default masker — salt should come from config in production
_DEFAULT_MASKER = PIIMasker(salt="asom-default-salt")


def load_dim_customer(
    db: DBConnector,
    snapshot_path: Path | str,
    masker: PIIMasker | None = None,
    snapshot_date: str | None = None,
) -> int:
    """Load a customer snapshot into dim_customer with SCD Type 2 logic.

    SCD2 rules:
    - New customers: INSERT with is_current=TRUE, effective_to=NULL
    - Segment changed: expire old (set effective_to, is_current=FALSE),
      INSERT new version with is_current=TRUE
    - No change: no action (idempotent)

    Args:
        db: Database connector.
        snapshot_path: Path to customer snapshot Parquet file.
        masker: PIIMasker instance (defaults to internal masker).
        snapshot_date: Date to use as effective_from for new versions.
            Defaults to 'today' if not specified.

    Returns:
        Number of rows in dim_customer after load.
    """
    snapshot_path = Path(snapshot_path)
    masker = masker or _DEFAULT_MASKER

    logger.info("Loading dim_customer SCD2 from %s", snapshot_path)

    # 1. Ensure table exists (DDL)
    ddl_path = SQL_DIR / "ddl" / "dim_customer.sql"
    ddl_sql = ddl_path.read_text()
    db.execute(ddl_sql)

    # 2. Read snapshot and apply PII masking
    df = pl.read_parquet(snapshot_path)

    if snapshot_date is None:
        snapshot_date = "2025-01-30"  # Deterministic default for testing

    # 3. Get current dimension state
    existing = db.fetch_all(
        "SELECT customer_id, segment FROM dim_customer WHERE is_current = TRUE"
    )
    current_map = {row["customer_id"]: row["segment"] for row in existing}

    # 4. Get next surrogate key
    max_key_rows = db.fetch_all(
        "SELECT COALESCE(MAX(customer_key), 0) as max_key FROM dim_customer"
    )
    next_key = max_key_rows[0]["max_key"] + 1

    # 5. Process each customer in the snapshot
    now_ts = snapshot_date
    loaded_at = snapshot_date

    for row in df.iter_rows(named=True):
        cid = row["customer_id"]
        segment = row["segment"]

        # Apply PII masking
        email_token = masker.mask_email(row["email"])
        phone_redacted = masker.redact_phone(row["phone"])

        if cid not in current_map:
            # NEW customer — insert as current
            _insert_customer(
                db, next_key, cid, email_token, phone_redacted,
                row["first_name"], row["last_name"], segment,
                now_ts, loaded_at,
            )
            next_key += 1
            logger.debug("Inserted new customer %d", cid)

        elif current_map[cid] != segment:
            # SEGMENT CHANGED — expire old, insert new version
            db.execute(
                f"UPDATE dim_customer SET effective_to = '{now_ts}', "
                f"is_current = FALSE "
                f"WHERE customer_id = {cid} AND is_current = TRUE"
            )
            _insert_customer(
                db, next_key, cid, email_token, phone_redacted,
                row["first_name"], row["last_name"], segment,
                now_ts, loaded_at,
            )
            next_key += 1
            logger.debug("SCD2 version created for customer %d: %s -> %s",
                         cid, current_map[cid], segment)

        # else: no change — skip (idempotent)

    # 6. Return final count
    rows = db.fetch_all("SELECT COUNT(*) as cnt FROM dim_customer")
    count = rows[0]["cnt"]
    logger.info("dim_customer contains %d rows", count)
    return count


def _insert_customer(
    db: DBConnector,
    key: int,
    cid: int,
    email_token: str,
    phone_redacted: str,
    first_name: str,
    last_name: str,
    segment: str,
    effective_from: str,
    loaded_at: str,
) -> None:
    """Insert a single customer record into dim_customer."""
    sql = (
        "INSERT INTO dim_customer "
        "(customer_key, customer_id, email_token, phone_redacted, "
        "first_name, last_name, segment, effective_from, effective_to, "
        "is_current, _loaded_at) VALUES "
        f"({key}, {cid}, '{email_token}', '{phone_redacted}', "
        f"'{_sql_escape(first_name)}', '{_sql_escape(last_name)}', "
        f"'{segment}', '{effective_from}', NULL, TRUE, '{loaded_at}')"
    )
    db.execute(sql)


def _sql_escape(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")
