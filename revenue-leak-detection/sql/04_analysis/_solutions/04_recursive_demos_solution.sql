USE portfolio;

WITH RECURSIVE date_series AS (
    SELECT DATE '2024-01-01' AS d
    UNION ALL
    SELECT d + INTERVAL 1 DAY
    FROM date_series
    WHERE d < '2024-01-31'
)
SELECT * FROM date_series;

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
    SUM(daily_trips)   OVER (ORDER BY partition_date) AS cumulative_trips,
    SUM(daily_revenue) OVER (ORDER BY partition_date) AS cumulative_revenue,
    ROUND(
        AVG(daily_trips) OVER (
            ORDER BY partition_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        0
    ) AS trips_7day_ma,
    ROUND(
        100.0 * (daily_trips - LAG(daily_trips) OVER (ORDER BY partition_date))
              / LAG(daily_trips) OVER (ORDER BY partition_date),
        2
    ) AS dod_trips_pct
FROM daily_total
ORDER BY partition_date;

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
