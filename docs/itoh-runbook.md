# CUR Pipeline Runbook (ITOH)

## Pipeline Overview

| Attribute | Value |
|-----------|-------|
| Pipeline | AWS CUR Cost Trend Analysis |
| Schedule | Daily, 06:00 UTC (after CUR delivery) |
| Runtime | < 5 minutes (30 days, 5 services) |
| Owner | Data Engineering |
| Escalation | #data-eng-oncall |

## Normal Operation

### Daily Load Sequence

1. CUR Parquet files delivered to S3 by AWS (typically 02:00-04:00 UTC)
2. Pipeline triggered at 06:00 UTC
3. Extract: `load_cur_parquet()` reads new Parquet, inserts to `raw_cur`
4. Transform: `build_daily_cost_summary()` aggregates by (date, service)
5. Analytics: `top_services_by_cost()`, `detect_cost_anomalies()` run
6. Results available for downstream consumers

### Expected Behaviour

- First run: loads all rows from Parquet file
- Subsequent runs: loads only new rows (idempotent via `identity_line_item_id`)
- Re-run same day: returns 0 new rows (safe to retry)
- NULL costs: treated as 0.00, logged as INFO

## Failure Scenarios

### Parquet file not found

- **Symptom**: `FileNotFoundError: CUR Parquet file not found`
- **Cause**: CUR delivery delayed or S3 path changed
- **Action**: Check S3 bucket for latest CUR export. If delayed, wait and retry. If path changed, update config.
- **Severity**: Low (pipeline is idempotent -- next run catches up)

### Schema validation failure

- **Symptom**: `ValueError: CUR schema validation failed: Missing required columns`
- **Cause**: AWS changed CUR format or wrong file loaded
- **Action**: Compare actual columns with `CUR_EXPECTED_COLUMNS` in `cur_loader.py`. If AWS changed format, update expected columns and add migration.
- **Severity**: Medium (blocks load until resolved)

### Database connection failure

- **Symptom**: Connection error from connector
- **Cause**: Snowflake maintenance, network issue, expired credentials
- **Action**: Check Snowflake status page. Verify credentials. Retry after 5 minutes.
- **Severity**: Medium (pipeline is idempotent -- retry safe)

### Duplicate detection false positive

- **Symptom**: 0 rows loaded when new data expected
- **Cause**: `identity_line_item_id` collision (unlikely but possible with CUR format changes)
- **Action**: Check `raw_cur` for existing rows matching the expected time period. If IDs collide, investigate CUR generation.
- **Severity**: Low (data integrity preserved, just missing new data)

## Monitoring (Sprint 2)

> Note: Alerting infrastructure deferred to Sprint 2 (C-09 deferred).

### Planned Monitors

- Pipeline execution success/failure
- Row count per load (expect > 0 for new CUR periods)
- Anomaly detection alerts (>20% WoW cost increase)
- Pipeline runtime (alert if > 15 minutes)

### Current Manual Checks

Until automated monitoring is in place:
- Check daily load logs for errors
- Review `raw_cur` row counts weekly
- Review cost anomaly output weekly

## Dimension Loads (Sprint 2)

### dim_service (SCD Type 1)

| Attribute | Value |
|-----------|-------|
| Trigger | After CUR load completes (step 3 above) |
| Runtime | < 1 minute |
| Idempotent | Yes — INSERT ON CONFLICT UPDATE |

**Normal operation:**
1. Runs after `raw_cur` is loaded
2. Extracts distinct (product_code, usage_type) from `raw_cur`
3. Inserts new combinations, updates `last_seen_date` for existing
4. Derives `service_category` from product_code

**Failure scenarios:**

| Symptom | Cause | Action | Severity |
|---------|-------|--------|----------|
| DDL file not found | sql/ddl/dim_service.sql missing | Check file exists in deployment | Medium |
| DML file not found | sql/dml/merge_dim_service.sql missing | Check file exists in deployment | Medium |
| 0 rows inserted | raw_cur empty or all services already loaded | Check raw_cur row count; verify CUR load ran first | Low |
| SQL error on category CASE | Unknown product_code pattern | Falls through to 'Other' category — no action needed | None |

**Re-run:** Safe. Re-running updates `last_seen_date` and `_updated_at` but does not duplicate rows.

### dim_customer (SCD Type 2)

| Attribute | Value |
|-----------|-------|
| Trigger | When new customer snapshot is available |
| Runtime | < 1 minute (scales linearly with customer count) |
| Idempotent | Yes — unchanged customers skipped, no duplicates |
| PII | Email and phone masked before insert |

**Normal operation:**
1. Receives path to customer snapshot Parquet file
2. Reads snapshot, applies PII masking (email → SHA256, phone → XXX-XXX-NNNN)
3. Compares each customer against current dimension state
4. New customers: INSERT with is_current=TRUE
5. Segment changed: expire old record (set effective_to, is_current=FALSE), INSERT new version
6. Unchanged: skip (no action)

**Failure scenarios:**

| Symptom | Cause | Action | Severity |
|---------|-------|--------|----------|
| Snapshot file not found | Parquet path incorrect or file not delivered | Verify file path and delivery schedule | Medium |
| PII masking error | PIIMasker configuration issue (missing salt) | Check masker initialisation, verify salt is set | High |
| Duplicate current records | Bug in expire logic (should not happen with tests) | Query `SELECT customer_id, COUNT(*) FROM dim_customer WHERE is_current GROUP BY 1 HAVING COUNT(*) > 1` to identify | High |
| History chain broken | effective_to not set on expired record | Query for records where is_current=FALSE AND effective_to IS NULL | High |

**Re-run:** Safe. Loading the same snapshot twice produces no changes (idempotent). Loading a new snapshot after a previous one correctly creates new versions only for changed customers.

**PII incident response:** If unmasked PII is discovered in dim_customer:
1. Immediately truncate dim_customer table
2. Verify PIIMasker is configured with valid salt
3. Re-run dimension load with masking confirmed
4. Report incident per data classification policy

## Rollback

The pipeline is append-only and idempotent. There is no destructive state change.

- **To re-process CUR**: Delete rows from `raw_cur` for the affected date range, then re-run
- **To re-process dim_service**: DROP and re-create table (DDL), then re-run loader. All state derived from raw_cur.
- **To re-process dim_customer**: DROP and re-create table (DDL), then re-load all snapshots in chronological order. History chain rebuilds from scratch.
- **To rollback code**: Revert git commit, re-deploy previous version
- **Data is never modified in place**: raw layer is insert-only, dimensions are versioned (SCD2) or idempotent (SCD1)

## Contacts

| Role | Contact |
|------|---------|
| Pipeline Owner | Data Engineering team lead |
| On-call | #data-eng-oncall Slack channel |
| Snowflake Admin | Platform team |
| AWS CUR Support | AWS Support (billing) |
