-- fact_daily_cost: Daily cost aggregation by date and service
-- Story: S008
-- Controls: C-06 (DQ), C-07 (Reproducibility), C-08 (Idempotency)
--
-- Grain: (usage_date, service_key) — one row per day per service
-- FK: service_key references dim_service
-- Idempotency: UNIQUE constraint + INSERT ON CONFLICT

CREATE TABLE IF NOT EXISTS fact_daily_cost (
    usage_date      TEXT NOT NULL,
    service_key     INTEGER NOT NULL,
    daily_cost      DOUBLE NOT NULL,
    record_count    INTEGER NOT NULL,
    null_cost_count INTEGER NOT NULL,
    _loaded_at      TEXT NOT NULL,
    UNIQUE (usage_date, service_key)
);
