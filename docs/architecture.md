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

## Data Classification

- **Classification**: Internal-Confidential
- **PII**: None (AWS account IDs, service names, costs)
- **Controls**: C-04 (Data Classification), C-06 (DQ), C-07 (Reproducibility)
