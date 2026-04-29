-- =============================================================
-- 99_dq_checks.sql
-- Data Quality checks — write results to pipeline_dq_results table.
--
-- Each check measures one specific failure mode and records:
--   * passed / failed
--   * metric_value (actual count or %)
--   * threshold (the bar we expect)
--   * severity (info / warn / error)
--
-- Run AFTER staging cleaning. Powers the "pipeline monitoring" dashboard.
-- =============================================================

USE portfolio;

-- -------------------------------------------------------------
-- NYC Taxi DQ checks
-- -------------------------------------------------------------

-- Check 1: retention_pct >= 95% — most rows should survive cleaning.
-- If retention drops below 95%, cleaning rules may be too aggressive
-- OR raw data quality has degraded.
INSERT INTO pipeline_dq_results
    (asset_name, check_name, severity, passed, metric_value, threshold, checked_at, notes)
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
        (SELECT COUNT(*) FROM raw_yellow_taxi)     AS raw_n,
        (SELECT COUNT(*) FROM staging_yellow_taxi) AS stg_n,
        ROUND(100.0 *
            (SELECT COUNT(*) FROM staging_yellow_taxi) /
            NULLIF((SELECT COUNT(*) FROM raw_yellow_taxi), 0), 2
        ) AS retention_pct
) t;

-- Check 2: no negative fares in staging (should be 0)
INSERT INTO pipeline_dq_results
    (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
SELECT
    'staging_yellow_taxi',
    'no_negative_fares',
    'error',
    n = 0,
    n,
    0,
    NOW()
FROM (SELECT COUNT(*) AS n FROM staging_yellow_taxi WHERE fare_amount < 0) t;

-- Check 3: pickup_date all within January 2024
INSERT INTO pipeline_dq_results
    (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
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

-- Check 4: trip_duration sanity
INSERT INTO pipeline_dq_results
    (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
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


-- -------------------------------------------------------------
-- Olist DQ checks
-- -------------------------------------------------------------

-- Check 5: orders retention >= 99% (Olist data is much cleaner than taxi)
INSERT INTO pipeline_dq_results
    (asset_name, check_name, severity, passed, metric_value, threshold, checked_at, notes)
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
        (SELECT COUNT(*) FROM raw_olist_orders)     AS raw_n,
        (SELECT COUNT(*) FROM staging_olist_orders) AS stg_n,
        ROUND(100.0 *
            (SELECT COUNT(*) FROM staging_olist_orders) /
            NULLIF((SELECT COUNT(*) FROM raw_olist_orders), 0), 2
        ) AS retention_pct
) t;

-- Check 6: every order_item has a parent order (referential integrity)
INSERT INTO pipeline_dq_results
    (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
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

-- Check 7: reviews are unique per order (we deduped — should be 0 dupes)
INSERT INTO pipeline_dq_results
    (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
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

-- Check 8: payment_total > 0 for all rows
INSERT INTO pipeline_dq_results
    (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
SELECT
    'staging_olist_payments',
    'no_zero_payments',
    'error',
    n = 0,
    n,
    0,
    NOW()
FROM (SELECT COUNT(*) AS n FROM staging_olist_payments WHERE payment_total <= 0) t;

-- Check 9: all customer_state codes are valid 2-letter Brazilian state codes
-- Use a single-row aggregation to avoid mixing aggregate / non-aggregate columns
-- under MySQL 8's ONLY_FULL_GROUP_BY mode.
INSERT INTO pipeline_dq_results
    (asset_name, check_name, severity, passed, metric_value, threshold, checked_at, notes)
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

-- Check 10: products with no English category translation (informational)
INSERT INTO pipeline_dq_results
    (asset_name, check_name, severity, passed, metric_value, threshold, checked_at)
SELECT
    'staging_olist_products',
    'missing_english_category',
    'info',
    n_missing < 100,
    n_missing,
    100,
    NOW()
FROM (SELECT COUNT(*) AS n_missing FROM staging_olist_products WHERE category_en IS NULL) t;


-- -------------------------------------------------------------
-- Show all DQ results from this run
-- -------------------------------------------------------------
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
    -- Failures first, then by severity
    passed ASC,
    FIELD(severity, 'error', 'warn', 'info'),
    asset_name;
