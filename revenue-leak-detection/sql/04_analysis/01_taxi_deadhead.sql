USE portfolio;
WITH
zone_totals AS (
    SELECT pickup_zone_id              AS zone_id,
        SUM(trip_count)             AS pickup_count,
        SUM(total_revenue)          AS pickup_revenue,
        SUM(airport_pickup_count)   AS airport_pickups
	From mart_taxi_daily_zone
    GROUP BY pickup_zone_id
),
dropoff_totals AS (
    SELECT
        dropoff_zone_id AS zone_id,
        SUM(trip_count) AS dropoff_count
    FROM mart_taxi_daily_zone_pair
    GROUP BY dropoff_zone_id
)

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
