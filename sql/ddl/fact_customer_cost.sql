-- fact_customer_cost: Customer cost attribution by date, customer, and service
-- Story: S009
-- Controls: C-04 (PII), C-06 (DQ), C-07 (Reproducibility), C-08 (Idempotency)
--
-- Grain: (usage_date, customer_key, service_key) — one row per day per customer per service
-- FK: customer_key references dim_customer, service_key references dim_service
-- PII: surrogate key references only — no raw customer PII
-- Idempotency: UNIQUE constraint + INSERT ON CONFLICT

CREATE TABLE IF NOT EXISTS fact_customer_cost (
    usage_date      TEXT NOT NULL,
    customer_key    INTEGER NOT NULL,
    service_key     INTEGER NOT NULL,
    allocated_cost  DOUBLE NOT NULL,
    record_count    INTEGER NOT NULL,
    null_cost_count INTEGER NOT NULL,
    _loaded_at      TEXT NOT NULL,
    UNIQUE (usage_date, customer_key, service_key)
);
