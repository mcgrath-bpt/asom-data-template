"""
Seed local database with fixture data for development.

Creates tables and loads sample data from tests/fixtures/ into the local
database (DuckDB or SQLite depending on ASOM_ENV).

Usage:
    ASOM_ENV=local python scripts/seed_local_db.py
    # Or via Makefile:
    make local-db

This gives developers a working local dataset to query and test against
without needing access to Snowflake.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.connector import get_connector


FIXTURES_DIR = Path("tests/fixtures")


def seed_customers_table(env: str = "local") -> None:
    """Create and populate a sample customers table."""
    db = get_connector(env)

    try:
        # Create schema matching medallion architecture
        if env.startswith("local") and not env.endswith("sqlite"):
            # DuckDB can read Parquet directly
            parquet_path = FIXTURES_DIR / "sample_customers.parquet"
            if parquet_path.exists():
                db.execute(f"""
                    CREATE OR REPLACE TABLE raw_customers AS
                    SELECT * FROM read_parquet('{parquet_path}')
                """)
                print(f"✅ raw_customers loaded from {parquet_path}")
            else:
                _seed_manual(db)
        else:
            _seed_manual(db)

        # Show what was loaded
        rows = db.fetch_all("SELECT COUNT(*) as cnt FROM raw_customers")
        print(f"   {rows[0]['cnt']} rows in raw_customers")

    finally:
        db.close()


def _seed_manual(db: object) -> None:
    """Seed with hardcoded sample data (SQLite or when no Parquet available)."""
    from src.connector import DBConnector

    assert hasattr(db, "execute"), "db must be a DBConnector"
    connector: DBConnector = db  # type: ignore[assignment]

    connector.execute("""
        CREATE TABLE IF NOT EXISTS raw_customers (
            customer_id INTEGER,
            email TEXT,
            phone TEXT,
            first_name TEXT,
            last_name TEXT,
            segment TEXT,
            created_at TEXT
        )
    """)

    sample_data = [
        (1, "alice@example.com", "555-100-1001", "Alice", "Smith", "premium", "2024-01-15"),
        (2, "bob@example.com", "555-100-1002", "Bob", "Jones", "standard", "2024-02-20"),
        (3, "carol@example.com", "555-100-1003", "Carol", "Williams", "premium", "2024-03-10"),
        (4, "dave@example.com", "555-100-1004", "Dave", "Brown", "standard", "2024-04-05"),
        (5, "eve@example.com", "555-100-1005", "Eve", "Davis", "enterprise", "2024-05-01"),
    ]

    for row in sample_data:
        connector.execute(
            "INSERT INTO raw_customers VALUES (?, ?, ?, ?, ?, ?, ?)",
            dict(zip(["p1", "p2", "p3", "p4", "p5", "p6", "p7"], row)),
        )

    print("✅ raw_customers seeded with sample data")


if __name__ == "__main__":
    import os

    env = os.getenv("ASOM_ENV", "local")
    print(f"Seeding local database (env={env})...")
    seed_customers_table(env)
    print("Done.")
