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

## Rollback

The pipeline is append-only and idempotent. There is no destructive state change.

- **To re-process**: Delete rows from `raw_cur` for the affected date range, then re-run
- **To rollback code**: Revert git commit, re-deploy previous version
- **Data is never modified in place**: raw layer is insert-only

## Contacts

| Role | Contact |
|------|---------|
| Pipeline Owner | Data Engineering team lead |
| On-call | #data-eng-oncall Slack channel |
| Snowflake Admin | Platform team |
| AWS CUR Support | AWS Support (billing) |
