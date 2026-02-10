-- dim_service: SCD Type 1 reference table for AWS services
-- Story: S005
-- Controls: C-06 (DQ), C-07 (Reproducibility)
--
-- Natural key: (product_code, usage_type)
-- Overwrite semantics: last_seen_date updated on re-observation

CREATE TABLE IF NOT EXISTS dim_service (
    service_key     INTEGER PRIMARY KEY,
    product_code    TEXT NOT NULL,
    usage_type      TEXT NOT NULL,
    service_category TEXT NOT NULL,
    first_seen_date TEXT NOT NULL,
    last_seen_date  TEXT NOT NULL,
    _loaded_at      TEXT NOT NULL,
    _updated_at     TEXT NOT NULL,
    UNIQUE (product_code, usage_type)
);
