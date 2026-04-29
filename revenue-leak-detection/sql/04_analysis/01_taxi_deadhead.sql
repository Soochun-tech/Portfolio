-- =============================================================
-- 01_taxi_deadhead.sql
-- 비즈니스 질문: 어느 zone이 deadhead 손실의 주범인가?
--
-- 가이드: sql/04_analysis/README.md 의 "문제 1" 참고
-- 사용 테이블:
--   mart_taxi_daily_zone        -- 일별 × pickup zone
--   mart_taxi_daily_zone_pair   -- 일별 × pickup × dropoff
--   raw_taxi_zone_lookup        -- zone 이름
-- =============================================================

USE portfolio;
WITH
-- 1단계: zone별 1월 전체 pickup/dropoff 합계
zone_totals AS (
    SELECT pickup_zone_id              AS zone_id,
        SUM(trip_count)             AS pickup_count,
        SUM(total_revenue)          AS pickup_revenue,
        SUM(airport_pickup_count)   AS airport_pickups
	From mart_taxi_daily_zone
    GROUP BY pickup_zone_id
),
-- 2단계: deadhead surplus/ratio 계산
dropoff_totals AS (
    SELECT
        dropoff_zone_id AS zone_id,
        SUM(trip_count) AS dropoff_count
    FROM mart_taxi_daily_zone_pair
    GROUP BY dropoff_zone_id
)

-- 3단계: 윈도우 함수로 ranking
SELECT
    z.zone_id,
    zl.zone AS zone_name,
    zl.borough,

    z.pickup_count,
    d.dropoff_count,

    d.dropoff_count - z.pickup_count AS surplus,

    ROUND(
        (d.dropoff_count - z.pickup_count)
        / NULLIF(z.pickup_count, 0),
        2
    ) AS surplus_ratio,

    ROW_NUMBER() OVER (
        ORDER BY d.dropoff_count - z.pickup_count DESC
    ) AS surplus_rank

FROM zone_totals z
JOIN dropoff_totals d
    ON z.zone_id = d.zone_id
LEFT JOIN raw_taxi_zone_lookup zl
    ON z.zone_id = zl.locationid
ORDER BY surplus DESC
LIMIT 20;

;
