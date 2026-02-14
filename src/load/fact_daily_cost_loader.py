"""
Load fact_daily_cost from raw_cur joined to dim_service.

SQL-first approach — Python orchestrates, SQL does the work.
Aggregates daily cost by (date, service) with NULL coalescing and
2-decimal monetary precision.

Story: S008 -- Daily Cost Fact Table
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


def load_fact_daily_cost(db: DBConnector) -> int:
    """Create and populate fact_daily_cost from raw_cur + dim_service.

    Idempotent: uses INSERT ON CONFLICT on (usage_date, service_key).
    NULL costs coalesced to 0.00 with null_cost_count audit trail.
    Monetary values rounded to 2 decimal places.

    Args:
        db: Database connector (DuckDB, SQLite, or Snowflake).
            Requires raw_cur and dim_service to be loaded first.

    Returns:
        Number of rows in fact_daily_cost after load.
    """
    logger.info("Loading fact_daily_cost")

    # 1. Ensure table exists (DDL)
    ddl_path = SQL_DIR / "ddl" / "fact_daily_cost.sql"
    ddl_sql = ddl_path.read_text()
    db.execute(ddl_sql)
    logger.info("fact_daily_cost DDL applied")

    # 2. Load data from raw_cur + dim_service (DML)
    dml_path = SQL_DIR / "dml" / "load_fact_daily_cost.sql"
    dml_sql = dml_path.read_text()
    db.execute(dml_sql)
    logger.info("fact_daily_cost load completed")

    # 3. Return final count
    rows = db.fetch_all("SELECT COUNT(*) as cnt FROM fact_daily_cost")
    count = rows[0]["cnt"]
    logger.info("fact_daily_cost contains %d rows", count)
    return count
