"""
Load dim_service (SCD Type 1) from raw CUR data.

Reads SQL DDL and DML from sql/ directory and executes against the
database connector. SQL-first approach — Python orchestrates, SQL does the work.

Story: S005 -- Create dim_service (SCD Type 1) reference table
Controls: C-06 (DQ), C-07 (Reproducibility), C-08 (Idempotency)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.connector import DBConnector

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent.parent / "sql"


def load_dim_service(db: DBConnector) -> int:
    """Create and populate dim_service from raw_cur data.

    Idempotent: uses INSERT ON CONFLICT to upsert.
    Type 1: existing services get last_seen_date updated.

    Args:
        db: Database connector (DuckDB, SQLite, or Snowflake).

    Returns:
        Number of rows in dim_service after load.
    """
    logger.info("Loading dim_service (SCD Type 1)")

    # 1. Ensure table exists (DDL)
    ddl_path = SQL_DIR / "ddl" / "dim_service.sql"
    ddl_sql = ddl_path.read_text()
    db.execute(ddl_sql)
    logger.info("dim_service DDL applied")

    # 2. Merge data from raw_cur (DML)
    dml_path = SQL_DIR / "dml" / "merge_dim_service.sql"
    dml_sql = dml_path.read_text()
    db.execute(dml_sql)
    logger.info("dim_service MERGE completed")

    # 3. Return final count
    rows = db.fetch_all("SELECT COUNT(*) as cnt FROM dim_service")
    count = rows[0]["cnt"]
    logger.info("dim_service contains %d rows", count)
    return count
