-- =============================================================
-- 01_clean_yellow_taxi.sql
-- raw_yellow_taxi → staging_yellow_taxi
--
-- Cleaning rules (each one will also be logged to pipeline_dq_results):
--   R1. pickup_datetime BETWEEN 2024-01-01 AND 2024-01-31
--       (Day 1 verification found 18 trips outside this range — clearly bad data)
--   R2. pickup_datetime < dropoff_datetime  (chronological)
--   R3. trip_duration BETWEEN 1 minute AND 4 hours
--   R4. trip_distance BETWEEN 0 (exclusive) AND 100 miles
--   R5. fare_amount > 0  AND  total_amount > 0  (no comp / no negatives)
--   R6. passenger_count: NORMALIZE only (don't drop). NYC TLC has ~6% rows
--       with 0/NULL passenger_count due to driver entry errors, but the trip
--       itself is still valid for revenue/route analysis. We keep these rows
--       but set passenger_count to NULL so analysis can filter explicitly.
--   R7. pulocationid AND dolocationid BETWEEN 1 AND 265 (valid TLC zones)
--
-- LaGuardia, JFK, Newark zones (for is_airport_pickup/dropoff flags):
--   132 = JFK, 138 = LaGuardia, 1 = Newark Airport (EWR)
-- =============================================================

USE portfolio;

-- Make this script idempotent: clear staging before re-running.
TRUNCATE TABLE staging_yellow_taxi;

-- -------------------------------------------------------------
-- Single-pass clean + transform + insert.
-- Wrapped in a CTE for readability — each filter is a labelled step.
-- -------------------------------------------------------------
INSERT INTO staging_yellow_taxi (
    vendorid, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
    pulocationid, dolocationid, payment_type, ratecodeid,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, congestion_surcharge, airport_fee, total_amount,
    pickup_date, pickup_hour, trip_duration_min, fare_per_mile,
    is_airport_pickup, is_airport_dropoff
)
WITH
-- Step 1. Date-window filter (kills 2009/2002/Dec 2023 outliers we found in Day 1)
in_window AS (
    SELECT *
    FROM raw_yellow_taxi
    WHERE tpep_pickup_datetime >= '2024-01-01'
      AND tpep_pickup_datetime <  '2024-02-01'
),
-- Step 2. Chronological order + sane duration
chrono_ok AS (
    SELECT *,
           TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS dur_min
    FROM in_window
    WHERE tpep_dropoff_datetime > tpep_pickup_datetime
      AND TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) BETWEEN 1 AND 240
),
-- Step 3. Distance + money + zone validity
-- (Note: passenger_count is NORMALIZED in step 4, not filtered here.)
fully_valid AS (
    SELECT *
    FROM chrono_ok
    WHERE trip_distance > 0
      AND trip_distance <= 100
      AND fare_amount > 0
      AND total_amount > 0
      AND pulocationid BETWEEN 1 AND 265
      AND dolocationid BETWEEN 1 AND 265
)
-- Step 4. Project + add derived columns
SELECT
    vendorid,
    tpep_pickup_datetime  AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    -- Normalize passenger_count: 0/NULL/out-of-range → NULL ("unknown")
    CASE WHEN passenger_count BETWEEN 1 AND 6 THEN passenger_count END AS passenger_count,
    trip_distance,
    pulocationid,
    dolocationid,
    payment_type,
    ratecodeid,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    DATE(tpep_pickup_datetime)  AS pickup_date,
    HOUR(tpep_pickup_datetime)  AS pickup_hour,
    dur_min                     AS trip_duration_min,
    -- guard against divide-by-zero (we already filtered distance > 0, but be safe)
    ROUND(fare_amount / NULLIF(trip_distance, 0), 2) AS fare_per_mile,
    pulocationid IN (1, 132, 138) AS is_airport_pickup,
    dolocationid IN (1, 132, 138) AS is_airport_dropoff
FROM fully_valid;

-- -------------------------------------------------------------
-- Verify results
-- -------------------------------------------------------------
SELECT
    (SELECT COUNT(*) FROM raw_yellow_taxi)     AS raw_rows,
    (SELECT COUNT(*) FROM staging_yellow_taxi) AS staging_rows,
    (SELECT COUNT(*) FROM raw_yellow_taxi)
        - (SELECT COUNT(*) FROM staging_yellow_taxi) AS dropped_rows,
    ROUND(
        100.0 * (SELECT COUNT(*) FROM staging_yellow_taxi)
              / (SELECT COUNT(*) FROM raw_yellow_taxi),
        2
    ) AS retention_pct;
