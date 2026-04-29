USE portfolio;

WITH
seller_orders AS (
    SELECT DISTINCT
        oi.seller_id,
        s.seller_state,
        o.order_id,
        c.customer_unique_id,
        o.purchase_date,
        o.is_late,
        r.review_score,
        r.is_negative,
        SUM(oi.item_total) OVER (PARTITION BY o.order_id) AS order_value
    FROM staging_olist_orders o
    JOIN staging_olist_order_items oi ON oi.order_id = o.order_id
    LEFT JOIN staging_olist_sellers   s ON s.seller_id = oi.seller_id
    LEFT JOIN staging_olist_reviews   r ON r.order_id = o.order_id
    LEFT JOIN staging_olist_customers c ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
),
customer_total_orders AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS total_orders_lifetime
    FROM staging_olist_orders o
    JOIN staging_olist_customers c USING (customer_id)
    GROUP BY c.customer_unique_id
),
seller_orders_enriched AS (
    SELECT
        so.*,
        cto.total_orders_lifetime,
        (cto.total_orders_lifetime > 1) AS is_repeat_customer
    FROM seller_orders so
    LEFT JOIN customer_total_orders cto USING (customer_unique_id)
),
seller_funnel AS (
    SELECT
        seller_id,
        seller_state,
        COUNT(DISTINCT order_id)                                      AS total_orders,
        COUNT(DISTINCT customer_unique_id)                            AS unique_customers,
        SUM(is_late = TRUE)                                           AS late_orders,
        ROUND(1.0 * SUM(is_late = TRUE)
                  / NULLIF(COUNT(DISTINCT order_id), 0), 4)           AS late_rate,
        ROUND(AVG(review_score), 2)                                   AS avg_review_score,
        SUM(is_negative = TRUE)                                       AS negative_review_count,
        ROUND(1.0 * SUM(is_negative = TRUE)
                  / NULLIF(SUM(review_score IS NOT NULL), 0), 4)      AS negative_review_rate,
        ROUND(
            1.0 * SUM(is_repeat_customer = TRUE AND is_late = TRUE)
                / NULLIF(SUM(is_late = TRUE), 0),
            4
        )                                                             AS late_customer_repeat_rate,
        ROUND(
            1.0 * SUM(is_repeat_customer = TRUE AND is_late = FALSE)
                / NULLIF(SUM(is_late = FALSE), 0),
            4
        )                                                             AS ontime_customer_repeat_rate,
        ROUND(SUM(order_value), 2)                                    AS gmv,
        ROUND(AVG(order_value), 2)                                    AS avg_order_value
    FROM seller_orders_enriched
    GROUP BY seller_id, seller_state
),
ranked AS (
    SELECT
        *,
        GREATEST(
            late_orders * avg_order_value
            * COALESCE(ontime_customer_repeat_rate - late_customer_repeat_rate, 0),
            0
        ) AS est_lost_revenue,
        RANK() OVER (
            ORDER BY GREATEST(
                late_orders * avg_order_value
                * COALESCE(ontime_customer_repeat_rate - late_customer_repeat_rate, 0),
                0
            ) DESC
        ) AS leak_rank,
        NTILE(4) OVER (
            ORDER BY GREATEST(
                late_orders * avg_order_value
                * COALESCE(ontime_customer_repeat_rate - late_customer_repeat_rate, 0),
                0
            ) DESC
        ) AS leak_quartile
    FROM seller_funnel
    WHERE total_orders >= 50
)
SELECT
    seller_id,
    seller_state,
    total_orders,
    late_orders,
    late_rate,
    avg_review_score,
    negative_review_rate,
    late_customer_repeat_rate,
    ontime_customer_repeat_rate,
    gmv,
    est_lost_revenue,
    leak_rank,
    leak_quartile
FROM ranked
ORDER BY leak_rank
LIMIT 25;
