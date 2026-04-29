USE portfolio;

WITH raw_total AS (
    SELECT COUNT(*) AS n FROM raw_yellow_taxi
)
SELECT
    rule_name,
    rows_failing,
    ROUND(100.0 * rows_failing / (SELECT n FROM raw_total), 2) AS pct_failing
FROM (
    SELECT 'R1_outside_jan_2024' AS rule_name,
        SUM(tpep_pickup_datetime < '2024-01-01' OR tpep_pickup_datetime >= '2024-02-01') AS rows_failing
    FROM raw_yellow_taxi

    UNION ALL SELECT 'R2_dropoff_before_pickup',
        SUM(tpep_dropoff_datetime <= tpep_pickup_datetime)
    FROM raw_yellow_taxi
    WHERE tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2024-02-01'

    UNION ALL SELECT 'R3_duration_out_of_range',
        SUM(TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) NOT BETWEEN 1 AND 240)
    FROM raw_yellow_taxi
    WHERE tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2024-02-01'
      AND tpep_dropoff_datetime > tpep_pickup_datetime

    UNION ALL SELECT 'R4_distance_zero_or_too_far',
        SUM(trip_distance <= 0 OR trip_distance > 100)
    FROM raw_yellow_taxi
    WHERE tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2024-02-01'

    UNION ALL SELECT 'R5_fare_or_total_nonpositive',
        SUM(fare_amount <= 0 OR total_amount <= 0)
    FROM raw_yellow_taxi
    WHERE tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2024-02-01'

    UNION ALL SELECT 'R6_passenger_out_of_range',
        SUM(passenger_count IS NULL OR passenger_count NOT BETWEEN 1 AND 6)
    FROM raw_yellow_taxi
    WHERE tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2024-02-01'

    UNION ALL SELECT 'R7_invalid_zone',
        SUM(pulocationid NOT BETWEEN 1 AND 265 OR dolocationid NOT BETWEEN 1 AND 265)
    FROM raw_yellow_taxi
    WHERE tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2024-02-01'
) per_rule
ORDER BY rows_failing DESC;
