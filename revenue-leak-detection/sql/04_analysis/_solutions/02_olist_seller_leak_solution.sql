-- =============================================================
-- 02_olist_seller_leak_solution.sql  (정답지)
-- "셀러 매출이 어디서 새고 있나?"
--
-- 비즈니스 이야기:
--   late delivery → bad review → customer doesn't return → LTV loss.
--   이 funnel을 셀러별로 집계해서 "어느 셀러가 가장 많이 새는지" 찾는다.
--   상위 leak 셀러에게 logistics support / coaching → 매출 회복.
--
-- 핵심 SQL 기법:
--   * 6개 테이블 LEFT JOIN
--   * SUM(... = TRUE) 패턴으로 boolean을 count
--   * AVG(CASE WHEN ... END) 패턴으로 conditional avg
--   * NTILE / RANK 윈도우 함수
--   * customer_unique_id로 진짜 재구매 (다음 주문) 식별
-- =============================================================

USE portfolio;

WITH
-- =========================================================
-- 1단계: 셀러 × 주문 단위 평탄화
--   - delivered인 주문만 (cancelled / unavailable 등 제외)
--   - JOIN으로 review, customer_unique_id, seller_state, category까지 한번에
--   - order_value는 SUM OVER로 ITEM 레벨에서 ORDER 레벨로 끌어올림
-- =========================================================
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
-- =========================================================
-- 2단계: 고객별 "이 셀러 산 이후 다른 주문 있나" — 재구매 식별
--   - 진짜 LTV는 같은 customer_unique_id가 (이 셀러 구매 후) 다시 사는지
--   - 우린 단순화: 그 고객이 plat에서 다른 주문을 했는지 만 봄
-- =========================================================
customer_total_orders AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS total_orders_lifetime
    FROM staging_olist_orders o
    JOIN staging_olist_customers c USING (customer_id)
    GROUP BY c.customer_unique_id
),
-- =========================================================
-- 3단계: 셀러 × 주문 × 재구매 여부 enrichment
-- =========================================================
seller_orders_enriched AS (
    SELECT
        so.*,
        cto.total_orders_lifetime,
        (cto.total_orders_lifetime > 1) AS is_repeat_customer
    FROM seller_orders so
    LEFT JOIN customer_total_orders cto USING (customer_unique_id)
),
-- =========================================================
-- 4단계: 셀러별 funnel 지표 집계
--   - SUM(boolean) = COUNT 자연스러운 패턴
--   - 비율은 1.0 * x / NULLIF(y, 0) 으로 divide-by-zero 방지
-- =========================================================
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
        -- 재구매 비율: late를 받은 고객 vs 정시 고객의 재구매율 차이
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
-- =========================================================
-- 5단계: 손실 추정 + 랭킹
--   - 추정 손실액 = 늦은 주문 수 × AOV × (정시 재구매율 - 늦은 재구매율)
--     = 만약 늦지 않았더라면 추가로 발생했을 매출
--   - 음수면 0으로 cap (이상치)
-- =========================================================
ranked AS (
    SELECT
        *,
        GREATEST(
            late_orders * avg_order_value
            * COALESCE(ontime_customer_repeat_rate - late_customer_repeat_rate, 0),
            0
        ) AS est_lost_revenue,
        -- 셀러 전체 중 손실액 ranking
        RANK() OVER (
            ORDER BY GREATEST(
                late_orders * avg_order_value
                * COALESCE(ontime_customer_repeat_rate - late_customer_repeat_rate, 0),
                0
            ) DESC
        ) AS leak_rank,
        -- 4분위 (1 = 상위 25% 누수 셀러)
        NTILE(4) OVER (
            ORDER BY GREATEST(
                late_orders * avg_order_value
                * COALESCE(ontime_customer_repeat_rate - late_customer_repeat_rate, 0),
                0
            ) DESC
        ) AS leak_quartile
    FROM seller_funnel
    WHERE total_orders >= 50      -- 노이즈 셀러 제외 (소량 주문)
)
-- =========================================================
-- 최종 출력
-- =========================================================
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

-- =========================================================
-- 활용:
--   * Top 10 셀러는 logistics intervention 대상 (가장 큰 손실)
--   * late_rate > 0.3 + 음수 review_rate > 0.2 셀러는 즉시 코칭
--   * SP주(상파울로) 같은 큰 주에 leak 누적되는지 확인 → 물류 hub 점검
--
-- 면접에서 어필:
--   "특정 셀러 X에서 estimated annual leak $5,400 발견. 정시 배송 시
--    재구매율이 12% 더 높았는데, 잦은 지연 때문에 잠재 LTV가 새고 있었음.
--    물류 우선순위 조정으로 회복 가능."
-- =========================================================
