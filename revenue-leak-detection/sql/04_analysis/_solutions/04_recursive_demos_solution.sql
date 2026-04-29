-- =============================================================
-- 04_recursive_demos_solution.sql  (정답지)
-- 재귀 CTE + 윈도우 함수 데모
-- =============================================================

USE portfolio;

-- =============================================================
-- 4-1. 재귀 CTE — 1월 31일치 날짜 시퀀스
-- =============================================================
-- 핵심: 재귀 CTE의 두 부분
--   * Anchor (시작점): SELECT DATE '2024-01-01'
--   * Recursive (재귀): 이전 row의 d + 1일
--   * 종료조건: WHERE d < '2024-01-31' (없으면 무한 루프)
--
-- 활용: dashboard에서 "trip 0건인 날"도 빈 row로 표시할 때 유용.
WITH RECURSIVE date_series AS (
    SELECT DATE '2024-01-01' AS d
    UNION ALL
    SELECT d + INTERVAL 1 DAY
    FROM date_series
    WHERE d < '2024-01-31'
)
SELECT * FROM date_series;
-- 결과: 31 rows (2024-01-01 ~ 2024-01-31)


-- =============================================================
-- 4-2. 누적 trips/매출 + 7일 이동 평균 + DoD %
-- =============================================================
-- 핵심 윈도우 함수 4종:
--   * SUM() OVER (ORDER BY ...) — 누적합 (running total)
--   * AVG() OVER (... ROWS BETWEEN N PRECEDING AND CURRENT ROW) — 이동 평균
--   * LAG(col) OVER (...) — 이전 row 값
--   * 변화율: (cur - prev) / prev * 100
WITH daily_total AS (
    SELECT
        partition_date,
        SUM(trip_count)              AS daily_trips,
        SUM(total_revenue)           AS daily_revenue
    FROM mart_taxi_daily_zone
    GROUP BY partition_date
)
SELECT
    partition_date,
    daily_trips,
    daily_revenue,
    -- 누적합 — ORDER BY 만 있으면 자동으로 1번행~현재행까지
    SUM(daily_trips)   OVER (ORDER BY partition_date) AS cumulative_trips,
    SUM(daily_revenue) OVER (ORDER BY partition_date) AS cumulative_revenue,
    -- 7일 이동 평균 — ROWS BETWEEN 6 PRECEDING AND CURRENT ROW = 자기 포함 7개
    ROUND(
        AVG(daily_trips) OVER (
            ORDER BY partition_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        0
    ) AS trips_7day_ma,
    -- 전일 대비 변화율 (DoD %)
    ROUND(
        100.0 * (daily_trips - LAG(daily_trips) OVER (ORDER BY partition_date))
              / LAG(daily_trips) OVER (ORDER BY partition_date),
        2
    ) AS dod_trips_pct
FROM daily_total
ORDER BY partition_date;


-- =============================================================
-- 4-3. (스트레치) LEFT JOIN으로 빈 날도 0으로 표시되는 시계열
-- =============================================================
-- 활용: 진짜 운영 데이터에서는 데이터가 없는 날에도 row가 필요할 때 (대시보드).
-- 우리 데이터는 1월 31일 다 채워져있어서 큰 차이 없지만 패턴은 유용.
WITH RECURSIVE date_series AS (
    SELECT DATE '2024-01-01' AS d
    UNION ALL
    SELECT d + INTERVAL 1 DAY FROM date_series WHERE d < '2024-01-31'
),
daily_total AS (
    SELECT
        partition_date,
        SUM(trip_count)    AS daily_trips,
        SUM(total_revenue) AS daily_revenue
    FROM mart_taxi_daily_zone
    GROUP BY partition_date
)
SELECT
    ds.d AS partition_date,
    COALESCE(dt.daily_trips,   0) AS daily_trips,
    COALESCE(dt.daily_revenue, 0) AS daily_revenue
FROM date_series ds
LEFT JOIN daily_total dt ON dt.partition_date = ds.d
ORDER BY ds.d;
