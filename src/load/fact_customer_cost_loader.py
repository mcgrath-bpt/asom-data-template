"""
Load fact_customer_cost from raw_cur joined to dim_service and dim_customer.

SQL-first approach — Python orchestrates, SQL does the work.
Allocates daily service costs equally to all customers active on that date,
using SCD2 temporal join for correct customer version attribution.

Story: S009 -- Customer Cost Attribution Fact Table
Controls: C-04 (PII), C-06 (DQ), C-07 (Reproducibility), C-08 (Idempotency)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.connector import DBConnector

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent.parent / "sql"


def load_fact_customer_cost(db: DBConnector) -> int:
    """Create and populate fact_customer_cost.

    Allocates each service's daily cost equally among all customers
    active on that date (SCD2 temporal join). Grain: (usage_date,
    customer_key, service_key).

    Idempotent: uses INSERT ON CONFLICT on the composite key.
    NULL costs coalesced to 0.00 with null_cost_count audit trail.
    Monetary values rounded to 2 decimal places.

    PII control: fact table references customer_key (surrogate) only.
    No raw PII (email, phone, name) is stored.

    Args:
        db: Database connector. Requires raw_cur, dim_service, and
            dim_customer to be loaded first.

    Returns:
        Number of rows in fact_customer_cost after load.
    """
    logger.info("Loading fact_customer_cost")

    # 1. Ensure table exists (DDL)
    ddl_path = SQL_DIR / "ddl" / "fact_customer_cost.sql"
    ddl_sql = ddl_path.read_text()
    db.execute(ddl_sql)
    logger.info("fact_customer_cost DDL applied")

    # 2. Load data (DML) — SCD2 temporal join + cost allocation
    dml_path = SQL_DIR / "dml" / "load_fact_customer_cost.sql"
    dml_sql = dml_path.read_text()
    db.execute(dml_sql)
    logger.info("fact_customer_cost load completed")

    # 3. Return final count
    rows = db.fetch_all("SELECT COUNT(*) as cnt FROM fact_customer_cost")
    count = rows[0]["cnt"]
    logger.info("fact_customer_cost contains %d rows", count)
    return count
