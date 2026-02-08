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


def generate_sample_cur() -> None:
    """Generate a small sample AWS CUR Parquet file.

    Simulates 30 days of AWS cost data across 5 services.
    Matches the schema of real CUR Parquet exports (subset of columns).
    """
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    import datetime
    import random

    random.seed(42)  # Deterministic fixtures

    services = ["AmazonEC2", "AmazonS3", "AmazonRDS", "AWSLambda", "AmazonCloudWatch"]
    base_costs = {"AmazonEC2": 45.0, "AmazonS3": 12.0, "AmazonRDS": 30.0, "AWSLambda": 5.0, "AmazonCloudWatch": 3.0}
    start_date = datetime.date(2025, 1, 1)

    rows: list[dict] = []
    for day_offset in range(30):
        usage_date = start_date + datetime.timedelta(days=day_offset)
        for service in services:
            base = base_costs[service]
            # Add some variance (+/- 20%) and a mild upward trend
            trend_factor = 1.0 + (day_offset * 0.005)
            variance = random.uniform(0.8, 1.2)
            cost = round(base * trend_factor * variance, 6)

            rows.append({
                "identity_line_item_id": f"lid-{day_offset:03d}-{service[:3].lower()}",
                "identity_time_interval": f"{usage_date}T00:00:00Z/{usage_date + datetime.timedelta(days=1)}T00:00:00Z",
                "bill_payer_account_id": "123456789012",
                "line_item_usage_account_id": "123456789012",
                "line_item_line_item_type": "Usage",
                "line_item_usage_start_date": str(usage_date),
                "line_item_usage_end_date": str(usage_date + datetime.timedelta(days=1)),
                "line_item_product_code": service,
                "line_item_usage_type": f"{service}-RunInstances",
                "line_item_operation": "RunInstances",
                "line_item_usage_amount": round(random.uniform(1.0, 100.0), 6),
                "line_item_unblended_cost": cost,
                "line_item_blended_cost": round(cost * 0.95, 6),
                "line_item_currency_code": "USD",
            })

    # Add one row with NULL cost to test null handling
    rows.append({
        "identity_line_item_id": "lid-null-test",
        "identity_time_interval": "2025-01-15T00:00:00Z/2025-01-16T00:00:00Z",
        "bill_payer_account_id": "123456789012",
        "line_item_usage_account_id": "123456789012",
        "line_item_line_item_type": "Usage",
        "line_item_usage_start_date": "2025-01-15",
        "line_item_usage_end_date": "2025-01-16",
        "line_item_product_code": "AmazonEC2",
        "line_item_usage_type": "AmazonEC2-RunInstances",
        "line_item_operation": "RunInstances",
        "line_item_usage_amount": 10.0,
        "line_item_unblended_cost": None,
        "line_item_blended_cost": None,
        "line_item_currency_code": "USD",
    })

    df = pl.DataFrame(rows)
    output_path = FIXTURES_DIR / "sample_cur.parquet"
    df.write_parquet(output_path)
    print(f"Generated {output_path} ({len(df)} rows)")


if __name__ == "__main__":
    generate_sample_customers()
    generate_sample_cur()
    print("Done.")
