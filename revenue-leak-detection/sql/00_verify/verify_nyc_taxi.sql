USE portfolio;

SELECT 'raw_yellow_taxi' AS tbl, COUNT(*) AS rows_n FROM raw_yellow_taxi
UNION ALL SELECT 'raw_taxi_zone_lookup', COUNT(*) FROM raw_taxi_zone_lookup;

SELECT
    DATE_FORMAT(tpep_pickup_datetime, '%Y-%m') AS pickup_month,
    COUNT(*) AS trips
FROM raw_yellow_taxi
GROUP BY pickup_month
ORDER BY trips DESC
LIMIT 10;

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

SELECT * FROM raw_taxi_zone_lookup LIMIT 5;

ALTER TABLE raw_yellow_taxi
  ADD INDEX idx_pickup (tpep_pickup_datetime),
  ADD INDEX idx_pu_zone_dt (pulocationid, tpep_pickup_datetime);

SHOW INDEX FROM raw_yellow_taxi;
