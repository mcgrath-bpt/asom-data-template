-- Load fact_daily_cost from raw_cur joined to dim_service
-- Story: S008
-- Controls: C-06 (DQ), C-07 (Reproducibility), C-08 (Idempotency)
--
-- Aggregates unblended_cost by (usage_date, product_code, usage_type),
-- joins to dim_service for surrogate key, rounds to 2 decimals.
-- INSERT ON CONFLICT ensures idempotent re-runs.

INSERT INTO fact_daily_cost (
    usage_date,
    service_key,
    daily_cost,
    record_count,
    null_cost_count,
    _loaded_at
)
SELECT
    r.line_item_usage_start_date AS usage_date,
    d.service_key,
    ROUND(SUM(COALESCE(CAST(r.line_item_unblended_cost AS DOUBLE), 0.0)), 2) AS daily_cost,
    COUNT(*) AS record_count,
    SUM(CASE WHEN r.line_item_unblended_cost IS NULL THEN 1 ELSE 0 END) AS null_cost_count,
    CAST(CURRENT_TIMESTAMP AS VARCHAR) AS _loaded_at
FROM raw_cur r
JOIN dim_service d
    ON r.line_item_product_code = d.product_code
   AND r.line_item_usage_type = d.usage_type
GROUP BY r.line_item_usage_start_date, d.service_key
ON CONFLICT (usage_date, service_key) DO UPDATE SET
    daily_cost = EXCLUDED.daily_cost,
    record_count = EXCLUDED.record_count,
    null_cost_count = EXCLUDED.null_cost_count,
    _loaded_at = EXCLUDED._loaded_at;
