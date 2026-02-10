-- dim_customer: SCD Type 2 dimension with history tracking
-- Story: S006
-- Controls: C-04 (PII), C-05 (Access), C-06 (DQ), C-08 (Idempotency)
--
-- Natural key: customer_id
-- Tracked SCD2 attribute: segment
-- PII: email masked (SHA256), phone redacted (last 4 digits)

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    INTEGER PRIMARY KEY,
    customer_id     INTEGER NOT NULL,
    email_token     TEXT NOT NULL,
    phone_redacted  TEXT NOT NULL,
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    segment         TEXT NOT NULL,
    effective_from  TEXT NOT NULL,
    effective_to    TEXT,
    is_current      BOOLEAN NOT NULL DEFAULT TRUE,
    _loaded_at      TEXT NOT NULL
);
