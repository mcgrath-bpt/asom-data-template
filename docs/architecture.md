# CUR Pipeline Architecture

## Overview

The AWS Cost & Usage Report (CUR) pipeline ingests Parquet-format CUR exports, transforms them into daily cost summaries, and provides analytical queries for cost optimisation.

## Data Flow

```
CUR Parquet Files (S3 / local)
        |
        v
  [Extract Layer]
  cur_loader.py
  - Schema validation (CUR_EXPECTED_COLUMNS)
  - Idempotent load (identity_line_item_id dedup)
  - NULL preservation
        |
        v
  raw_cur (DuckDB / Snowflake)
  - All columns stored as TEXT
  - Source-of-truth, no transformations
        |
        v
  [Transform Layer]
  cur_transformer.py
  - Daily cost aggregation by (date, service)
  - NULL costs -> 0.00 with logging
  - 7-day rolling average per service
        |
        v
  Daily Cost Summary (polars DataFrame)
  - usage_date, service_name, daily_cost, record_count
  - Optional: cost_7d_avg (trend)
        |
        v
  [Analytics Layer]
  cur_analytics.py
  - Top-N services by total cost
  - Month-over-month change percentage
  - Cost anomaly detection (>20% WoW)
```

## Database Abstraction

The pipeline uses the `DBConnector` protocol from `src/connector.py`:

| Environment | Backend | Configuration |
|------------|---------|---------------|
| local | DuckDB (in-memory or file) | `config/local.yaml` |
| local-sqlite | SQLite3 | `config/local-sqlite.yaml` |
| dev | Snowflake DEV | `config/dev.yaml` + env vars |
| test | DuckDB (in-memory) | `config/test.yaml` |
| prod | Snowflake PROD | `config/prod.yaml` + env vars |

All SQL is written to be compatible across DuckDB and Snowflake. The `CAST(... AS DOUBLE)` and `COALESCE` functions work identically on both.

## Schema Design

### raw_cur (Extract Layer)

All columns stored as TEXT to preserve source fidelity:

- `identity_line_item_id` -- Natural key, used for idempotency
- `identity_time_interval` -- Usage time window
- `bill_payer_account_id` -- AWS account (payer)
- `line_item_usage_account_id` -- AWS account (usage)
- `line_item_line_item_type` -- Usage, Tax, Credit, etc.
- `line_item_usage_start_date` -- YYYY-MM-DD
- `line_item_usage_end_date` -- YYYY-MM-DD
- `line_item_product_code` -- AWS service name
- `line_item_usage_type` -- Usage type detail
- `line_item_operation` -- Operation type
- `line_item_usage_amount` -- Usage quantity
- `line_item_unblended_cost` -- Cost (may be NULL)
- `line_item_blended_cost` -- Blended cost (may be NULL)
- `line_item_currency_code` -- USD

### Daily Cost Summary (Transform Layer -- in-memory)

- `usage_date` (Utf8) -- YYYY-MM-DD
- `service_name` (Utf8) -- AWS service
- `daily_cost` (Float64) -- Sum of unblended_cost, NULL->0.00
- `record_count` (UInt32) -- Source row count

## Key Design Decisions

1. **All-TEXT raw layer**: Preserves source fidelity. Type casting happens in transform SQL.
2. **Row-by-row inserts**: Compatible across all backends. Batch optimisation deferred to production Snowflake path.
3. **Idempotent by natural key**: Uses `identity_line_item_id` to prevent duplicates on re-run.
4. **In-memory transforms**: Daily summary and analytics computed in polars, not persisted to DB. This keeps the pipeline simple and avoids additional DDL.
5. **7-day minimum window**: Rolling average requires 7 data points; earlier days return NULL (not partial averages).

## Dimensional Model (Sprint 2)

The pipeline produces two slowly changing dimensions from raw CUR and customer snapshot data.

### Data Flow — Dimensions

```
raw_cur (DuckDB / Snowflake)
        |
        v
  [dim_service Loader]
  dim_service_loader.py
  - Reads DDL from sql/ddl/dim_service.sql
  - Executes DML from sql/dml/merge_dim_service.sql
  - INSERT ON CONFLICT UPDATE (SCD Type 1)
        |
        v
  dim_service (DuckDB / Snowflake)
  - Surrogate key (service_key)
  - Natural key: (product_code, usage_type)
  - Derived: service_category
  - Audit: first_seen_date, last_seen_date, _loaded_at, _updated_at


Customer Snapshots (Parquet v1, v2, ...)
        |
        v
  [dim_customer Loader]
  dim_customer_loader.py
  - Compare-and-version logic (SCD Type 2)
  - PII masking before insert (PIIMasker)
  - Python-orchestrated (DuckDB lacks MERGE with expire-and-insert)
        |
        v
  dim_customer (DuckDB / Snowflake)
  - Surrogate key (customer_key)
  - Natural key: customer_id
  - SCD2: effective_from, effective_to, is_current
  - Tracked attribute: segment
  - PII-masked: email_token (SHA256), phone_redacted (XXX-XXX-NNNN)
```

### dim_service Schema (SCD Type 1)

| Column | Type | Description |
|--------|------|-------------|
| service_key | INTEGER PK | Surrogate key (generated) |
| product_code | VARCHAR | AWS service (e.g. AmazonEC2) |
| usage_type | VARCHAR | Usage type (e.g. RunInstances) |
| service_category | VARCHAR | Derived: Compute, Storage, Database, Serverless, Monitoring, Other |
| first_seen_date | VARCHAR | First observation date |
| last_seen_date | VARCHAR | Last observation date (updated on re-run) |
| _loaded_at | VARCHAR | Initial load timestamp |
| _updated_at | VARCHAR | Last update timestamp |

- **Unique constraint**: (product_code, usage_type)
- **SCD behaviour**: Type 1 — latest values overwrite, no history
- **Idempotency**: INSERT ON CONFLICT UPDATE ensures re-run safety

### dim_customer Schema (SCD Type 2)

| Column | Type | Description |
|--------|------|-------------|
| customer_key | INTEGER PK | Surrogate key (auto-increment) |
| customer_id | VARCHAR | Natural key (stable across versions) |
| email_token | VARCHAR | SHA256 hash of email (PII masked) |
| phone_redacted | VARCHAR | XXX-XXX-NNNN format (last 4 digits) |
| first_name | VARCHAR | Customer first name |
| last_name | VARCHAR | Customer last name |
| segment | VARCHAR | Customer segment (premium/standard/enterprise) |
| effective_from | VARCHAR | Version start date |
| effective_to | VARCHAR | Version end date (NULL = current) |
| is_current | BOOLEAN | TRUE for active version |
| _loaded_at | VARCHAR | Load timestamp |

- **SCD2 tracked attribute**: segment (change triggers expire + new version)
- **SCD1 attributes**: email_token, phone_redacted (overwritten, no history)
- **PII handling**: Raw email/phone never stored — masking applied before insert
- **Idempotency**: Unchanged customers skipped on re-load, no duplicates

### Key Design Decisions (Sprint 2)

1. **SQL-first for dim_service**: DDL and DML in SQL files, Python is thin orchestrator. Portable to Snowflake.
2. **Python-orchestrated SCD2 for dim_customer**: DuckDB lacks MERGE with expire-and-insert semantics. Python handles compare-and-version logic. Would need refactoring for Snowflake MERGE in production.
3. **PII masking at load time**: PIIMasker applied before any data reaches the dimension table. No raw PII in the data warehouse.
4. **Row-by-row SCD2 processing**: Acceptable for dimension tables (tens of thousands of rows). Would need batch processing for large-scale customer bases.
5. **Deterministic fixtures for testing**: Customer snapshots v1 (baseline) and v2 (with changes) generated with fixed seeds for reproducible tests.

## Data Classification

- **Classification**: Internal-Confidential
- **PII**: Customer email and phone in source data — masked before storage (C-04)
  - Email: SHA256 hashed via PIIMasker (deterministic, salted)
  - Phone: Redacted to XXX-XXX-NNNN (last 4 digits preserved)
- **CUR data**: No PII (AWS account IDs, service names, costs)
- **Controls**: C-04 (PII Masking), C-05 (Access Control), C-06 (DQ), C-07 (Reproducibility), C-08 (Idempotency)
