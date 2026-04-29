USE portfolio;

INSERT INTO pipeline_dq_results (asset_name, check_name, severity, passed, metric_value, threshold, checked_at, notes)
SELECT
    'staging_yellow_taxi',
    'cleaning_retention_pct',
    'warn',
    retention_pct >= 95,
    retention_pct,
    95.00,
    NOW(),
    CONCAT(raw_n, ' raw -> ', stg_n, ' staging')
FROM (
    SELECT
        (SELECT COUNT(*) FROM raw_yellow_taxi) AS raw_n,
        (SELECT COUNT(*) FROM staging_yellow_taxi) AS stg_n,
        ROUND(100.0 *
            (SELECT COUNT(*) FROM staging_yellow_taxi) /
            NULLIF((SELECT COUNT(*) FROM raw_yellow_taxi), 0), 2
        ) AS retention_pct
) t;

INSERT INTO pipeline_dq_results (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
SELECT
    'staging_yellow_taxi',
    'no_negative_fares',
    'error',
    n = 0,
    n,
    0,
    NOW()
FROM (SELECT COUNT(*) AS n FROM staging_yellow_taxi WHERE fare_amount < 0) t;

INSERT INTO pipeline_dq_results (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
SELECT
    'staging_yellow_taxi',
    'date_window_2024_01',
    'error',
    n = 0,
    n,
    0,
    NOW()
FROM (
    SELECT COUNT(*) AS n FROM staging_yellow_taxi
    WHERE pickup_date < '2024-01-01' OR pickup_date >= '2024-02-01'
) t;

INSERT INTO pipeline_dq_results (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
SELECT
    'staging_yellow_taxi',
    'trip_duration_in_range',
    'error',
    n = 0,
    n,
    0,
    NOW()
FROM (
    SELECT COUNT(*) AS n FROM staging_yellow_taxi
    WHERE trip_duration_min < 1 OR trip_duration_min > 240
) t;

INSERT INTO pipeline_dq_results (asset_name, check_name, severity, passed, metric_value, threshold, checked_at, notes)
SELECT
    'staging_olist_orders',
    'orders_retention_pct',
    'warn',
    retention_pct >= 99,
    retention_pct,
    99.00,
    NOW(),
    CONCAT(raw_n, ' raw -> ', stg_n, ' staging')
FROM (
    SELECT
        (SELECT COUNT(*) FROM raw_olist_orders) AS raw_n,
        (SELECT COUNT(*) FROM staging_olist_orders) AS stg_n,
        ROUND(100.0 *
            (SELECT COUNT(*) FROM staging_olist_orders) /
            NULLIF((SELECT COUNT(*) FROM raw_olist_orders), 0), 2
        ) AS retention_pct
) t;

INSERT INTO pipeline_dq_results (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
SELECT
    'staging_olist_order_items',
    'orphan_items_count',
    'error',
    n = 0,
    n,
    0,
    NOW()
FROM (
    SELECT COUNT(*) AS n
    FROM staging_olist_order_items oi
    LEFT JOIN staging_olist_orders o ON o.order_id = oi.order_id
    WHERE o.order_id IS NULL
) t;

INSERT INTO pipeline_dq_results (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
SELECT
    'staging_olist_reviews',
    'duplicate_reviews_per_order',
    'error',
    dup_count = 0,
    dup_count,
    0,
    NOW()
FROM (
    SELECT COUNT(*) AS dup_count FROM (
        SELECT order_id FROM staging_olist_reviews
        GROUP BY order_id HAVING COUNT(*) > 1
    ) d
) t;

INSERT INTO pipeline_dq_results (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
SELECT
    'staging_olist_payments',
    'no_zero_payments',
    'error',
    n = 0,
    n,
    0,
    NOW()
FROM (SELECT COUNT(*) AS n FROM staging_olist_payments WHERE payment_total <= 0) t;

INSERT INTO pipeline_dq_results (asset_name, check_name, severity, passed, metric_value, threshold, checked_at, notes)
SELECT
    'staging_olist_customers',
    'invalid_state_codes',
    'warn',
    bad_count = 0,
    bad_count,
    0,
    NOW(),
    CONCAT('Invalid: ', COALESCE(bad_list, 'none'))
FROM (
    SELECT
        SUM(CASE WHEN customer_state NOT IN (
            'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
            'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
        ) THEN 1 ELSE 0 END) AS bad_count,
        GROUP_CONCAT(DISTINCT CASE WHEN customer_state NOT IN (
            'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
            'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
        ) THEN customer_state END SEPARATOR ',') AS bad_list
    FROM staging_olist_customers
) t;

INSERT INTO pipeline_dq_results (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
SELECT
    'staging_olist_products',
    'missing_english_category',
    'info',
    n_missing < 100,
    n_missing,
    100,
    NOW()
FROM (SELECT COUNT(*) AS n_missing FROM staging_olist_products WHERE category_en IS NULL) t;

SELECT
    asset_name,
    check_name,
    severity,
    CASE WHEN passed THEN 'PASS' ELSE 'FAIL' END AS result,
    metric_value,
    threshold,
    notes,
    checked_at
FROM pipeline_dq_results
WHERE checked_at >= NOW() - INTERVAL 5 MINUTE
ORDER BY
    passed ASC,
    FIELD(severity, 'error', 'warn', 'info'),
    asset_name;
