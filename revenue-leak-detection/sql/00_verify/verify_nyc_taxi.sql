-- =============================================================
-- verify_nyc_taxi.sql
-- NYC Taxi raw 적재 검증
--
-- 실행 순서:
--   1. 검증 쿼리 (1~4번) — 데이터 들어갔는지 확인
--   2. 인덱스 재생성 (5번) — 적재 끝났으니 인덱스 복원
-- =============================================================

USE portfolio;

-- 1. 전체 행 수 — 2024-01 한 달이면 ~3M 행 (약 2.96M 예상)
SELECT 'raw_yellow_taxi' AS tbl, COUNT(*) AS rows_n FROM raw_yellow_taxi
UNION ALL SELECT 'raw_taxi_zone_lookup', COUNT(*) FROM raw_taxi_zone_lookup;

-- 2. 월별 분포 — 1월(2024-01) 행이 대부분이어야 함
SELECT
    DATE_FORMAT(tpep_pickup_datetime, '%Y-%m') AS pickup_month,
    COUNT(*) AS trips
FROM raw_yellow_taxi
GROUP BY pickup_month
ORDER BY trips DESC
LIMIT 10;

-- 3. 샘플 데이터 5개
SELECT
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount,
    pulocationid,
    dolocationid
FROM raw_yellow_taxi
LIMIT 5;

-- 4. Zone lookup — 265개여야 함
SELECT * FROM raw_taxi_zone_lookup LIMIT 5;

-- =============================================================
-- 5. 인덱스 재생성 — 적재가 끝났으니 다시 만들기
--    (분석 쿼리 속도 위해 필요)
-- =============================================================

ALTER TABLE raw_yellow_taxi
  ADD INDEX idx_pickup (tpep_pickup_datetime),
  ADD INDEX idx_pu_zone_dt (pulocationid, tpep_pickup_datetime);

-- 인덱스 확인
SHOW INDEX FROM raw_yellow_taxi;
