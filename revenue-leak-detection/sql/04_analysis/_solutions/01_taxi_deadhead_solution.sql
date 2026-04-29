USE portfolio;

WITH
zone_pickups AS (
    SELECT
        pickup_zone_id              AS zone_id,
        SUM(trip_count)             AS pickup_count,
        SUM(total_revenue)          AS pickup_revenue,
        SUM(airport_pickup_count)   AS airport_pickups
    FROM mart_taxi_daily_zone
    GROUP BY pickup_zone_id
),
zone_dropoffs AS (
    SELECT
        dropoff_zone_id        AS zone_id,
        SUM(trip_count)        AS dropoff_count,
        SUM(total_revenue)     AS dropoff_revenue
    FROM mart_taxi_daily_zone_pair
    GROUP BY dropoff_zone_id
),
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
SELECT
    zone_name,
    borough,
    service_zone,
    pickup_count,
    dropoff_count,
    surplus,
    surplus_ratio,
    RANK()         OVER (ORDER BY surplus DESC) AS surplus_rank,
    ROUND(
        PERCENT_RANK() OVER (ORDER BY surplus DESC),
        4
    ) AS surplus_percentile,
    NTILE(4) OVER (ORDER BY surplus DESC) AS deadhead_quartile,
    RANK() OVER (PARTITION BY borough ORDER BY surplus DESC) AS rank_within_borough
FROM zone_combined
WHERE dropoff_count + pickup_count > 0
ORDER BY surplus DESC
LIMIT 30;
