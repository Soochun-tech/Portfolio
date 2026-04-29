-- =============================================================
-- 01_taxi_deadhead_solution.sql  (정답지)
-- "어느 zone이 deadhead 손실의 주범인가?"
--
-- 비즈니스 이야기:
--   매출은 dropoff에서 발생하지만, dropoff 후에 다음 pickup까지의
--   빈 차량 운행이 deadhead loss다. dropoff > pickup인 zone은
--   "차가 남는" zone — 다른 zone으로 빈 차로 이동해야 함.
--   Fleet 운영자에게 "오전엔 어느 zone에 차를 미리 배치해야 하나"를
--   알려주는 데이터.
-- =============================================================

USE portfolio;

WITH
-- =========================================================
-- 1단계: 1월 한 달 zone별 pickup 합계
--   - mart_taxi_daily_zone는 zone × date 이미 집계 → 한 번 더 SUM
-- =========================================================
zone_pickups AS (
    SELECT
        pickup_zone_id              AS zone_id,
        SUM(trip_count)             AS pickup_count,
        SUM(total_revenue)          AS pickup_revenue,
        SUM(airport_pickup_count)   AS airport_pickups
    FROM mart_taxi_daily_zone
    GROUP BY pickup_zone_id
),
-- =========================================================
-- 2단계: 1월 한 달 zone별 dropoff 합계
--   - mart_taxi_daily_zone_pair에서 dropoff_zone_id 기준 집계
-- =========================================================
zone_dropoffs AS (
    SELECT
        dropoff_zone_id        AS zone_id,
        SUM(trip_count)        AS dropoff_count,
        SUM(total_revenue)     AS dropoff_revenue
    FROM mart_taxi_daily_zone_pair
    GROUP BY dropoff_zone_id
),
-- =========================================================
-- 3단계: zone lookup + pickup/dropoff 합치기 + surplus 계산
--   - LEFT JOIN으로 양쪽에 없는 zone도 보존 (예: 공항)
--   - surplus > 0 = "차가 남는" (dropoff > pickup) → deadhead 발생원
-- =========================================================
zone_combined AS (
    SELECT
        zl.locationid                              AS zone_id,
        zl.zone                                    AS zone_name,
        zl.borough,
        zl.service_zone,
        COALESCE(zp.pickup_count, 0)               AS pickup_count,
        COALESCE(zd.dropoff_count, 0)              AS dropoff_count,
        COALESCE(zd.dropoff_count, 0)
          - COALESCE(zp.pickup_count, 0)           AS surplus,
        -- 비율: dropoff 대비 surplus의 비중. 1.0 = pickup 0건 (모두 떠나야 함)
        CASE
            WHEN COALESCE(zd.dropoff_count, 0) > 0
            THEN ROUND(
                1.0 * (COALESCE(zd.dropoff_count, 0) - COALESCE(zp.pickup_count, 0))
                    / zd.dropoff_count,
                4
            )
        END                                        AS surplus_ratio,
        COALESCE(zp.pickup_revenue,  0)            AS pickup_revenue,
        COALESCE(zd.dropoff_revenue, 0)            AS dropoff_revenue
    FROM raw_taxi_zone_lookup zl
    LEFT JOIN zone_pickups  zp ON zp.zone_id = zl.locationid
    LEFT JOIN zone_dropoffs zd ON zd.zone_id = zl.locationid
)
-- =========================================================
-- 4단계: 윈도우 함수로 ranking + percentile
--   - RANK(): 동률 같은 순위 부여 (1, 1, 3 ...)
--   - DENSE_RANK(): 동률이어도 다음 순위 1씩 (1, 1, 2 ...)
--   - PERCENT_RANK(): 0.0 ~ 1.0 정규화된 백분위
--   - NTILE(4): 4분위 분할 — 상위 25%, 25~50% ... 라벨링
-- =========================================================
SELECT
    zone_name,
    borough,
    service_zone,
    pickup_count,
    dropoff_count,
    surplus,
    surplus_ratio,
    -- deadhead 심각도 ranking (surplus 큰 것부터)
    RANK()         OVER (ORDER BY surplus DESC) AS surplus_rank,
    -- 상위 N%인지 (0=가장 심각, 1=가장 안 심각)
    ROUND(
        PERCENT_RANK() OVER (ORDER BY surplus DESC),
        4
    ) AS surplus_percentile,
    -- 4분위로 라벨 (1 = 상위 25% deadhead 발생원)
    NTILE(4) OVER (ORDER BY surplus DESC) AS deadhead_quartile,
    -- borough 안에서의 ranking (지역 내 비교용)
    RANK() OVER (PARTITION BY borough ORDER BY surplus DESC) AS rank_within_borough
FROM zone_combined
WHERE dropoff_count + pickup_count > 0          -- 한 건도 없는 zone 제외
ORDER BY surplus DESC
LIMIT 30;

-- =========================================================
-- 활용:
--   * Top 10 zone에 차량 보충 — "공항/맨해튼 이스트사이드는 출근시간에 차 부족"
--   * surplus_ratio > 0.5 zone은 "절반 이상이 빠져나가야 하는" 심각 zone
--   * borough 내 1위와 2위 zone의 surplus 차이로 우선순위 결정
-- =========================================================
