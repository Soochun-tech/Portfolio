USE portfolio;

WITH
purchases AS (
    SELECT
        c.customer_unique_id,
        o.purchase_date,
        DATE(DATE_FORMAT(o.purchase_date, '%Y-%m-01')) AS purchase_month
    FROM staging_olist_orders o
    JOIN staging_olist_customers c USING (customer_id)
),
first_purchase AS (
    SELECT
        customer_unique_id,
        MIN(purchase_month) AS cohort_month
    FROM purchases
    GROUP BY customer_unique_id
),
purchase_with_cohort AS (
    SELECT
        p.customer_unique_id,
        fp.cohort_month,
        p.purchase_month,
        TIMESTAMPDIFF(MONTH, fp.cohort_month, p.purchase_month) AS months_since_first
    FROM purchases p
    JOIN first_purchase fp USING (customer_unique_id)
),
cohort_activity AS (
    SELECT
        cohort_month,
        months_since_first,
        COUNT(DISTINCT customer_unique_id) AS active_customers
    FROM purchase_with_cohort
    GROUP BY cohort_month, months_since_first
),
cohort_size AS (
    SELECT
        cohort_month,
        active_customers AS cohort_size
    FROM cohort_activity
    WHERE months_since_first = 0
)
SELECT
    ca.cohort_month,
    cs.cohort_size,
    ca.months_since_first,
    ca.active_customers,
    ROUND(1.0 * ca.active_customers / cs.cohort_size, 4) AS retention_rate
FROM cohort_activity ca
JOIN cohort_size cs USING (cohort_month)
WHERE ca.months_since_first <= 6
  AND cs.cohort_size >= 100
ORDER BY ca.cohort_month, ca.months_since_first;

WITH
purchases AS (
    SELECT c.customer_unique_id, o.purchase_date,
           DATE(DATE_FORMAT(o.purchase_date, '%Y-%m-01')) AS purchase_month
    FROM staging_olist_orders o JOIN staging_olist_customers c USING (customer_id)
),
first_purchase AS (
    SELECT customer_unique_id, MIN(purchase_month) AS cohort_month
    FROM purchases GROUP BY customer_unique_id
),
purchase_with_cohort AS (
    SELECT p.customer_unique_id, fp.cohort_month,
           TIMESTAMPDIFF(MONTH, fp.cohort_month, p.purchase_month) AS m
    FROM purchases p JOIN first_purchase fp USING (customer_unique_id)
),
cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT customer_unique_id) AS sz
    FROM purchase_with_cohort WHERE m = 0 GROUP BY cohort_month
)
SELECT
    pc.cohort_month,
    cs.sz AS cohort_size,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN m=0 THEN pc.customer_unique_id END)/cs.sz, 1) AS m0,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN m=1 THEN pc.customer_unique_id END)/cs.sz, 1) AS m1,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN m=2 THEN pc.customer_unique_id END)/cs.sz, 1) AS m2,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN m=3 THEN pc.customer_unique_id END)/cs.sz, 1) AS m3,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN m=4 THEN pc.customer_unique_id END)/cs.sz, 1) AS m4,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN m=5 THEN pc.customer_unique_id END)/cs.sz, 1) AS m5,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN m=6 THEN pc.customer_unique_id END)/cs.sz, 1) AS m6
FROM purchase_with_cohort pc
JOIN cohort_size cs USING (cohort_month)
WHERE cs.sz >= 100
GROUP BY pc.cohort_month, cs.sz
ORDER BY pc.cohort_month;
