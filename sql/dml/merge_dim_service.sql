-- Merge new service observations into dim_service (SCD Type 1)
-- Story: S005
-- Controls: C-06 (DQ), C-08 (Idempotency)
--
-- Logic:
--   INSERT new (product_code, usage_type) combos
--   UPDATE last_seen_date for existing services (Type 1 overwrite)
--
-- DuckDB does not support MERGE, so we use INSERT ON CONFLICT UPDATE.

INSERT INTO dim_service (
    service_key,
    product_code,
    usage_type,
    service_category,
    first_seen_date,
    last_seen_date,
    _loaded_at,
    _updated_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY product_code, usage_type)
        + COALESCE((SELECT MAX(service_key) FROM dim_service), 0) AS service_key,
    src.product_code,
    src.usage_type,
    CASE
        WHEN src.product_code IN ('AmazonEC2')           THEN 'Compute'
        WHEN src.product_code IN ('AmazonS3')            THEN 'Storage'
        WHEN src.product_code IN ('AmazonRDS', 'AmazonRedshift') THEN 'Database'
        WHEN src.product_code IN ('AWSLambda')           THEN 'Serverless'
        WHEN src.product_code IN ('AmazonCloudWatch')    THEN 'Monitoring'
        ELSE 'Other'
    END AS service_category,
    src.first_seen AS first_seen_date,
    src.last_seen AS last_seen_date,
    CAST(CURRENT_TIMESTAMP AS VARCHAR) AS _loaded_at,
    CAST(CURRENT_TIMESTAMP AS VARCHAR) AS _updated_at
FROM (
    SELECT
        line_item_product_code AS product_code,
        line_item_usage_type AS usage_type,
        MIN(line_item_usage_start_date) AS first_seen,
        MAX(line_item_usage_start_date) AS last_seen
    FROM raw_cur
    GROUP BY line_item_product_code, line_item_usage_type
) src
ON CONFLICT (product_code, usage_type) DO UPDATE SET
    last_seen_date = EXCLUDED.last_seen_date,
    _updated_at = EXCLUDED._updated_at;
