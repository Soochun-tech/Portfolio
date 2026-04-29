-- =============================================================
-- 02_olist_seller_leak.sql
-- Business Question: Where is seller revenue leaking?
-- (Funnel: Late Delivery → Bad Review → Lost Repeat Purchase)
--
-- Tables Used:
--    staging_olist_orders, staging_olist_order_items
--    staging_olist_reviews, staging_olist_sellers
--    staging_olist_products, staging_olist_customers
-- =============================================================

USE portfolio;

WITH
seller_orders AS (
    SELECT
        oi.seller_id,
        s.seller_state,
        o.order_id,
        c.customer_unique_id,
        o.is_late,
        r.review_score,
        r.is_negative,
        SUM(oi.item_total) AS order_value
    FROM staging_olist_orders o
    JOIN staging_olist_order_items oi
        ON oi.order_id = o.order_id
    LEFT JOIN staging_olist_sellers s
        ON s.seller_id = oi.seller_id
    LEFT JOIN staging_olist_reviews r
        ON r.order_id = o.order_id
    LEFT JOIN staging_olist_customers c
        ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY
        oi.seller_id,
        s.seller_state,
        o.order_id,
        c.customer_unique_id,
        o.is_late,
        r.review_score,
        r.is_negative
),

seller_funnel AS (
    SELECT
        seller_id,
        seller_state,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(is_late = TRUE) AS late_orders,
        ROUND(SUM(is_late = TRUE) / COUNT(DISTINCT order_id), 4) AS late_rate,
        ROUND(AVG(review_score), 2) AS avg_review_score,
        SUM(is_negative = TRUE) AS negative_review_count,
        ROUND(SUM(is_negative = TRUE) / COUNT(DISTINCT order_id), 4) AS negative_review_rate,
        ROUND(AVG(order_value), 2) AS avg_order_value,
        SUM(order_value) AS gmv
    FROM seller_orders
    GROUP BY seller_id, seller_state
),

ranked AS (
    SELECT
        *,
        ROUND(
            late_orders * avg_order_value * negative_review_rate,
            2
        ) AS est_leak_usd,
        ROW_NUMBER() OVER (
            ORDER BY late_orders * avg_order_value * negative_review_rate DESC
        ) AS leak_rank
    FROM seller_funnel
    WHERE total_orders >= 50
)

SELECT *
FROM ranked
ORDER BY leak_rank
LIMIT 20;
