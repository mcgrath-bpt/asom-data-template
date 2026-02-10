"""
Generate test fixture data files.

Run once after initial setup to create sample Parquet files:
    python scripts/generate_fixtures.py

These fixtures are checked into git so other developers don't need to regenerate.

Fixtures generated:
- sample_customers.parquet       -- Original 10-row customer file (Sprint 1)
- sample_customers_v1.parquet    -- Baseline customer snapshot for SCD2 testing (30 rows)
- sample_customers_v2.parquet    -- Later snapshot with changes for SCD2 testing
- sample_cur.parquet             -- AWS CUR data with diverse service/usage combos
"""
from pathlib import Path

import polars as pl


FIXTURES_DIR = Path("tests/fixtures")


def generate_sample_customers() -> None:
    """Generate the original small sample customers Parquet file (Sprint 1)."""
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
    print(f"Generated {output_path} ({len(df)} rows)")


# ---------------------------------------------------------------------------
# SCD fixture data (S004)
# ---------------------------------------------------------------------------

# Shared customer seed data — 30 customers for v1 baseline
_CUSTOMER_SEED = [
    # id, email, phone, first, last, segment, created_at
    (1, "alice@example.com", "555-100-1001", "Alice", "Smith", "premium", "2024-01-15"),
    (2, "bob@example.com", "555-100-1002", "Bob", "Jones", "standard", "2024-02-20"),
    (3, "carol@example.com", "555-100-1003", "Carol", "Williams", "premium", "2024-03-10"),
    (4, "dave@example.com", "555-100-1004", "Dave", "Brown", "standard", "2024-04-05"),
    (5, "eve@example.com", "555-100-1005", "Eve", "Davis", "enterprise", "2024-05-01"),
    (6, "frank@example.com", "555-100-1006", "Frank", "Miller", "standard", "2024-06-12"),
    (7, "grace@example.com", "555-100-1007", "Grace", "Wilson", "premium", "2024-07-08"),
    (8, "heidi@example.com", "555-100-1008", "Heidi", "Moore", "standard", "2024-08-22"),
    (9, "ivan@example.com", "555-100-1009", "Ivan", "Taylor", "enterprise", "2024-09-15"),
    (10, "judy@example.com", "555-100-1010", "Judy", "Anderson", "premium", "2024-10-03"),
    (11, "karl@example.com", "555-100-1011", "Karl", "Thomas", "standard", "2024-01-20"),
    (12, "laura@example.com", "555-100-1012", "Laura", "Jackson", "premium", "2024-02-14"),
    (13, "mike@example.com", "555-100-1013", "Mike", "White", "standard", "2024-03-05"),
    (14, "nina@example.com", "555-100-1014", "Nina", "Harris", "enterprise", "2024-04-18"),
    (15, "oscar@example.com", "555-100-1015", "Oscar", "Martin", "standard", "2024-05-22"),
    (16, "pat@example.com", "555-100-1016", "Pat", "Garcia", "premium", "2024-06-01"),
    (17, "quinn@example.com", "555-100-1017", "Quinn", "Martinez", "standard", "2024-06-30"),
    (18, "ruth@example.com", "555-100-1018", "Ruth", "Robinson", "enterprise", "2024-07-15"),
    (19, "sam@example.com", "555-100-1019", "Sam", "Clark", "standard", "2024-08-01"),
    (20, "tina@example.com", "555-100-1020", "Tina", "Rodriguez", "premium", "2024-08-20"),
    (21, "uma@example.com", "555-100-1021", "Uma", "Lewis", "standard", "2024-09-05"),
    (22, "vic@example.com", "555-100-1022", "Vic", "Lee", "premium", "2024-09-18"),
    (23, "wendy@example.com", "555-100-1023", "Wendy", "Walker", "standard", "2024-10-01"),
    (24, "xander@example.com", "555-100-1024", "Xander", "Hall", "enterprise", "2024-10-15"),
    (25, "yara@example.com", "555-100-1025", "Yara", "Allen", "standard", "2024-11-01"),
    (26, "zach@example.com", "555-100-1026", "Zach", "Young", "premium", "2024-11-10"),
    (27, "amber@example.com", "555-100-1027", "Amber", "King", "standard", "2024-11-20"),
    (28, "blake@example.com", "555-100-1028", "Blake", "Wright", "enterprise", "2024-12-01"),
    (29, "chloe@example.com", "555-100-1029", "Chloe", "Lopez", "premium", "2024-12-10"),
    (30, "derek@example.com", "555-100-1030", "Derek", "Hill", "standard", "2024-12-20"),
]


def _customers_from_seed(seed: list[tuple]) -> pl.DataFrame:
    """Build a customer DataFrame from seed tuples."""
    return pl.DataFrame(
        {
            "customer_id": [r[0] for r in seed],
            "email": [r[1] for r in seed],
            "phone": [r[2] for r in seed],
            "first_name": [r[3] for r in seed],
            "last_name": [r[4] for r in seed],
            "segment": [r[5] for r in seed],
            "created_at": [r[6] for r in seed],
        }
    )


def generate_sample_customers_v1() -> None:
    """Generate baseline customer snapshot for SCD2 testing.

    30 customers across premium/standard/enterprise segments.
    This represents the 'initial load' state.
    """
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    df = _customers_from_seed(_CUSTOMER_SEED)

    output_path = FIXTURES_DIR / "sample_customers_v1.parquet"
    df.write_parquet(output_path)
    print(f"Generated {output_path} ({len(df)} rows)")


def generate_sample_customers_v2() -> None:
    """Generate later customer snapshot with SCD2-relevant changes.

    Changes from v1:
    - 6 segment changes (upgrades and downgrades)
    - 3 new customers (IDs 31-33)
    - 2 contact detail changes (email/phone updates)
    - All other customers unchanged
    """
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    # Start with v1 seed and apply changes
    v2_seed = list(_CUSTOMER_SEED)

    # --- Segment changes (6 customers) ---
    # Customer 2: standard -> premium (upgrade)
    v2_seed[1] = (2, "bob@example.com", "555-100-1002", "Bob", "Jones", "premium", "2024-02-20")
    # Customer 4: standard -> enterprise (upgrade)
    v2_seed[3] = (4, "dave@example.com", "555-100-1004", "Dave", "Brown", "enterprise", "2024-04-05")
    # Customer 7: premium -> standard (downgrade)
    v2_seed[6] = (7, "grace@example.com", "555-100-1007", "Grace", "Wilson", "standard", "2024-07-08")
    # Customer 11: standard -> premium (upgrade)
    v2_seed[10] = (11, "karl@example.com", "555-100-1011", "Karl", "Thomas", "premium", "2024-01-20")
    # Customer 15: standard -> enterprise (upgrade)
    v2_seed[14] = (15, "oscar@example.com", "555-100-1015", "Oscar", "Martin", "enterprise", "2024-05-22")
    # Customer 22: premium -> standard (downgrade)
    v2_seed[21] = (22, "vic@example.com", "555-100-1022", "Vic", "Lee", "standard", "2024-09-18")

    # --- Contact detail changes (2 customers, no segment change) ---
    # Customer 6: new email
    v2_seed[5] = (6, "frank.miller@newdomain.com", "555-100-1006", "Frank", "Miller", "standard", "2024-06-12")
    # Customer 19: new phone
    v2_seed[18] = (19, "sam@example.com", "555-200-9999", "Sam", "Clark", "standard", "2024-08-01")

    # --- New customers (3) ---
    v2_seed.extend([
        (31, "elena@example.com", "555-100-1031", "Elena", "Scott", "premium", "2025-01-05"),
        (32, "finn@example.com", "555-100-1032", "Finn", "Green", "standard", "2025-01-12"),
        (33, "gina@example.com", "555-100-1033", "Gina", "Adams", "enterprise", "2025-01-18"),
    ])

    df = _customers_from_seed(v2_seed)

    output_path = FIXTURES_DIR / "sample_customers_v2.parquet"
    df.write_parquet(output_path)
    print(f"Generated {output_path} ({len(df)} rows)")


def generate_sample_cur() -> None:
    """Generate a sample AWS CUR Parquet file.

    Simulates 30 days of AWS cost data with diverse service/usage-type
    combinations for dim_service extraction.

    Matches the schema of real CUR Parquet exports (subset of columns).
    """
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    import datetime
    import random

    random.seed(42)  # Deterministic fixtures

    # Expanded: multiple usage types per service for richer dim_service
    service_usage_types = [
        ("AmazonEC2", "EC2-RunInstances", "RunInstances"),
        ("AmazonEC2", "EC2-EBS:VolumeUsage", "CreateVolume"),
        ("AmazonEC2", "EC2-ElasticIP:IdleAddress", "AllocateAddress"),
        ("AmazonS3", "S3-TimedStorage-ByteHrs", "StandardStorage"),
        ("AmazonS3", "S3-Requests-Tier1", "PutObject"),
        ("AmazonRDS", "RDS-InstanceUsage:db.m5.large", "CreateDBInstance"),
        ("AmazonRDS", "RDS-StorageUsage", "AllocateStorage"),
        ("AWSLambda", "Lambda-GB-Second", "Invoke"),
        ("AWSLambda", "Lambda-Request", "Invoke"),
        ("AmazonCloudWatch", "CW-Metrics", "PutMetricData"),
        ("AmazonCloudWatch", "CW-Logs", "PutLogEvents"),
        ("AmazonRedshift", "Redshift-Node:dc2.large", "CreateCluster"),
    ]

    base_costs = {
        "EC2-RunInstances": 45.0,
        "EC2-EBS:VolumeUsage": 8.0,
        "EC2-ElasticIP:IdleAddress": 1.5,
        "S3-TimedStorage-ByteHrs": 12.0,
        "S3-Requests-Tier1": 2.0,
        "RDS-InstanceUsage:db.m5.large": 30.0,
        "RDS-StorageUsage": 5.0,
        "Lambda-GB-Second": 4.0,
        "Lambda-Request": 1.0,
        "CW-Metrics": 2.0,
        "CW-Logs": 1.5,
        "Redshift-Node:dc2.large": 55.0,
    }

    start_date = datetime.date(2025, 1, 1)

    rows: list[dict] = []
    for day_offset in range(30):
        usage_date = start_date + datetime.timedelta(days=day_offset)
        for product_code, usage_type, operation in service_usage_types:
            base = base_costs[usage_type]
            trend_factor = 1.0 + (day_offset * 0.005)
            variance = random.uniform(0.8, 1.2)
            cost = round(base * trend_factor * variance, 6)

            rows.append({
                "identity_line_item_id": f"lid-{day_offset:03d}-{usage_type[:6].lower()}",
                "identity_time_interval": (
                    f"{usage_date}T00:00:00Z/"
                    f"{usage_date + datetime.timedelta(days=1)}T00:00:00Z"
                ),
                "bill_payer_account_id": "123456789012",
                "line_item_usage_account_id": "123456789012",
                "line_item_line_item_type": "Usage",
                "line_item_usage_start_date": str(usage_date),
                "line_item_usage_end_date": str(usage_date + datetime.timedelta(days=1)),
                "line_item_product_code": product_code,
                "line_item_usage_type": usage_type,
                "line_item_operation": operation,
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
        "line_item_usage_type": "EC2-RunInstances",
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
    generate_sample_customers_v1()
    generate_sample_customers_v2()
    generate_sample_cur()
    print("Done.")
