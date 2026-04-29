-- =============================================================
-- 03_olist_cohort_solution.sql  (정답지)
-- 첫 구매 월 코호트별 N개월 후 재구매율
--
-- 비즈니스 이야기:
--   "1월에 처음 산 고객 1,000명 중 3개월 후에도 사는 고객 비율은?"
--   E-commerce에서 가장 중요한 retention 지표. 평균 1~3% 정도가 정상
--   (e-commerce 특성상 일회성 구매가 많음).
--
-- 핵심 SQL 기법:
--   * customer_unique_id (재구매 식별 키 — customer_id는 주문마다 새로 생성됨)
--   * MIN OVER PARTITION BY 또는 그냥 GROUP BY MIN()으로 코호트 정의
--   * TIMESTAMPDIFF(MONTH, ...) 로 경과 월 계산
--   * 5단계 CTE로 단계별 정리
-- =============================================================

USE portfolio;

WITH
-- =========================================================
-- 1단계: customer_unique_id ↔ 구매 매핑
--   - 우리 staging_olist_orders는 customer_id (주문별 변하는 키)만 있음
--   - staging_olist_customers에 customer_unique_id (사람 기준 키) 있음
--   - JOIN해서 "누가 언제 샀나"를 얻음
-- =========================================================
purchases AS (
    SELECT
        c.customer_unique_id,
        o.purchase_date,
        DATE(DATE_FORMAT(o.purchase_date, '%Y-%m-01')) AS purchase_month
    FROM staging_olist_orders o
    JOIN staging_olist_customers c USING (customer_id)
),
-- =========================================================
-- 2단계: 각 customer의 첫 구매 월 = "코호트"
-- =========================================================
first_purchase AS (
    SELECT
        customer_unique_id,
        MIN(purchase_month) AS cohort_month
    FROM purchases
    GROUP BY customer_unique_id
),
-- =========================================================
-- 3단계: 각 구매에 코호트 + 경과월수 부여
-- =========================================================
purchase_with_cohort AS (
    SELECT
        p.customer_unique_id,
        fp.cohort_month,
        p.purchase_month,
        TIMESTAMPDIFF(MONTH, fp.cohort_month, p.purchase_month) AS months_since_first
    FROM purchases p
    JOIN first_purchase fp USING (customer_unique_id)
),
-- =========================================================
-- 4단계: 코호트 × 경과월별 활성 고객 수
-- =========================================================
cohort_activity AS (
    SELECT
        cohort_month,
        months_since_first,
        COUNT(DISTINCT customer_unique_id) AS active_customers
    FROM purchase_with_cohort
    GROUP BY cohort_month, months_since_first
),
-- =========================================================
-- 5단계: 코호트 사이즈 (0개월차 = 첫 구매 시점)
-- =========================================================
cohort_size AS (
    SELECT
        cohort_month,
        active_customers AS cohort_size
    FROM cohort_activity
    WHERE months_since_first = 0
)
-- =========================================================
-- 최종: 리텐션 매트릭스 (long format)
-- =========================================================
SELECT
    ca.cohort_month,
    cs.cohort_size,
    ca.months_since_first,
    ca.active_customers,
    ROUND(1.0 * ca.active_customers / cs.cohort_size, 4) AS retention_rate
FROM cohort_activity ca
JOIN cohort_size cs USING (cohort_month)
WHERE ca.months_since_first <= 6
  AND cs.cohort_size >= 100      -- 코호트 너무 작으면 노이즈 (월별 유의미한 코호트만)
ORDER BY ca.cohort_month, ca.months_since_first;

-- =========================================================
-- (스트레치) Wide format pivot
--   long → wide 변환. Tableau 같은 BI 도구에서는 long이 편하지만
--   엑셀/슬라이드 보고용으로는 wide가 한눈에 보기 좋음.
-- =========================================================
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
