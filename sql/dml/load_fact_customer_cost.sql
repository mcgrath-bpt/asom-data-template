-- Load fact_customer_cost: allocate daily service costs to active customers
-- Story: S009
-- Controls: C-04 (PII), C-06 (DQ), C-07 (Reproducibility), C-08 (Idempotency)
--
-- Cost allocation model: each service's daily cost is divided equally among
-- all customers active on that date (SCD2 temporal join).
-- INSERT ON CONFLICT ensures idempotent re-runs.

WITH service_daily AS (
    SELECT
        r.line_item_usage_start_date AS usage_date,
        d.service_key,
        ROUND(SUM(COALESCE(CAST(r.line_item_unblended_cost AS DOUBLE), 0.0)), 2) AS daily_cost,
        COUNT(*) AS record_count,
        SUM(CASE WHEN r.line_item_unblended_cost IS NULL THEN 1 ELSE 0 END) AS null_cost_count
    FROM raw_cur r
    JOIN dim_service d
        ON r.line_item_product_code = d.product_code
       AND r.line_item_usage_type = d.usage_type
    GROUP BY r.line_item_usage_start_date, d.service_key
),
active_customers AS (
    SELECT DISTINCT sd.usage_date, c.customer_key
    FROM service_daily sd
    JOIN dim_customer c
        ON c.effective_from <= sd.usage_date
       AND COALESCE(c.effective_to, '9999-12-31') > sd.usage_date
),
customer_count_per_date AS (
    SELECT usage_date, COUNT(*) AS active_count
    FROM active_customers
    GROUP BY usage_date
)
INSERT INTO fact_customer_cost (
    usage_date,
    customer_key,
    service_key,
    allocated_cost,
    record_count,
    null_cost_count,
    _loaded_at
)
SELECT
    sd.usage_date,
    ac.customer_key,
    sd.service_key,
    ROUND(sd.daily_cost / cc.active_count, 2) AS allocated_cost,
    sd.record_count,
    sd.null_cost_count,
    CAST(CURRENT_TIMESTAMP AS VARCHAR) AS _loaded_at
FROM service_daily sd
JOIN active_customers ac ON sd.usage_date = ac.usage_date
JOIN customer_count_per_date cc ON sd.usage_date = cc.usage_date
ON CONFLICT (usage_date, customer_key, service_key) DO UPDATE SET
    allocated_cost = EXCLUDED.allocated_cost,
    record_count = EXCLUDED.record_count,
    null_cost_count = EXCLUDED.null_cost_count,
    _loaded_at = EXCLUDED._loaded_at;
