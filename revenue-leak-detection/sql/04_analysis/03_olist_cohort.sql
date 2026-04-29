-- =============================================================
-- 03_olist_cohort.sql
-- 비즈니스 질문: 첫 구매 월 코호트별로 N개월 후 재구매율은?
--
-- 가이드: sql/04_analysis/README.md 의 "문제 3" 참고
-- 사용 테이블:
--   staging_olist_orders
--   staging_olist_customers   -- customer_unique_id (재구매 식별)
-- =============================================================

USE portfolio;
WITH
purchases AS (
    SELECT
        c.customer_unique_id,
        o.purchase_date,
        DATE_FORMAT(o.purchase_date, '%Y-%m-01') AS purchase_month
    FROM staging_olist_orders o
    JOIN staging_olist_customers c
        ON c.customer_id = o.customer_id
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
    JOIN first_purchase fp
        ON p.customer_unique_id = fp.customer_unique_id
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
    ROUND(ca.active_customers / cs.cohort_size, 4) AS retention_rate
FROM cohort_activity ca
JOIN cohort_size cs
    ON ca.cohort_month = cs.cohort_month
WHERE ca.months_since_first <= 6
ORDER BY ca.cohort_month, ca.months_since_first;

