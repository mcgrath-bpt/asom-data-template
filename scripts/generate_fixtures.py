"""
Generate test fixture data files.

Run once after initial setup to create sample Parquet files:
    python scripts/generate_fixtures.py

These fixtures are checked into git so other developers don't need to regenerate.
"""
from pathlib import Path

import polars as pl


FIXTURES_DIR = Path("tests/fixtures")


def generate_sample_customers() -> None:
    """Generate a small sample customers Parquet file."""
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    df = pl.DataFrame({
        "customer_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "email": [
            "alice@example.com",
            "bob@example.com",
            "carol@example.com",
            "dave@example.com",
            "eve@example.com",
            "frank@example.com",
            "grace@example.com",
            "heidi@example.com",
            "ivan@example.com",
            "judy@example.com",
        ],
        "phone": [
            "555-100-1001",
            "555-100-1002",
            "555-100-1003",
            "555-100-1004",
            "555-100-1005",
            "555-100-1006",
            "555-100-1007",
            "555-100-1008",
            "555-100-1009",
            "555-100-1010",
        ],
        "first_name": [
            "Alice", "Bob", "Carol", "Dave", "Eve",
            "Frank", "Grace", "Heidi", "Ivan", "Judy",
        ],
        "last_name": [
            "Smith", "Jones", "Williams", "Brown", "Davis",
            "Miller", "Wilson", "Moore", "Taylor", "Anderson",
        ],
        "segment": [
            "premium", "standard", "premium", "standard", "enterprise",
            "standard", "premium", "standard", "enterprise", "premium",
        ],
        "created_at": [
            "2024-01-15", "2024-02-20", "2024-03-10", "2024-04-05", "2024-05-01",
            "2024-06-12", "2024-07-08", "2024-08-22", "2024-09-15", "2024-10-03",
        ],
    })

    output_path = FIXTURES_DIR / "sample_customers.parquet"
    df.write_parquet(output_path)
    print(f"✅ Generated {output_path} ({len(df)} rows)")


if __name__ == "__main__":
    generate_sample_customers()
    print("Done.")
