"""
NYC Taxi mart assets — daily-partitioned.

Each materialization processes ONE day's worth of staging_yellow_taxi data
and writes to mart_taxi_daily_zone (+ mart_taxi_daily_zone_pair).

Why daily partitions:
  * staging_yellow_taxi has ~3M rows/month — querying a single day is ~100k.
  * Re-materializing one bad day is cheap; no need to recompute the month.
  * Backfill: Dagster can fan out 31 partitions in parallel.
"""
from __future__ import annotations

from dagster import (
    DailyPartitionsDefinition,
    asset,
)
from sqlalchemy import text

from ..resources import MySQLResource
from ._runlog import run_logged

# January 2024 partition window (matches our raw data scope)
TAXI_PARTITIONS = DailyPartitionsDefinition(
    start_date="2024-01-01",
    end_offset=0,
    end_date="2024-02-01",
)


# -------------------------------------------------------------
# mart_taxi_daily_zone — one row per (date, pickup_zone)
# -------------------------------------------------------------
@asset(
    partitions_def=TAXI_PARTITIONS,
    group_name="taxi_mart",
    description="Daily aggregations per pickup zone — for deadhead / demand analysis.",
)
def mart_taxi_daily_zone(context, mysql: MySQLResource) -> int:
    partition_date = context.partition_key  # e.g. "2024-01-15"
    asset_name = "mart_taxi_daily_zone"

    with run_logged(mysql, asset_name, partition_date) as run:
        # Idempotent: clear this partition before re-inserting
        run.conn.execute(
            text("DELETE FROM mart_taxi_daily_zone WHERE partition_date = :d"),
            {"d": partition_date},
        )

        result = run.conn.execute(
            text(
                """
                INSERT INTO mart_taxi_daily_zone (
                    partition_date, pickup_zone_id,
                    trip_count,
                    total_fare, total_revenue, avg_fare, avg_total,
                    total_distance_mi, avg_distance_mi, avg_duration_min,
                    avg_tip_pct, airport_pickup_count
                )
                SELECT
                    pickup_date                                AS partition_date,
                    pulocationid                               AS pickup_zone_id,
                    COUNT(*)                                   AS trip_count,
                    ROUND(SUM(fare_amount), 2)                 AS total_fare,
                    ROUND(SUM(total_amount), 2)                AS total_revenue,
                    ROUND(AVG(fare_amount), 2)                 AS avg_fare,
                    ROUND(AVG(total_amount), 2)                AS avg_total,
                    ROUND(SUM(trip_distance), 2)               AS total_distance_mi,
                    ROUND(AVG(trip_distance), 3)               AS avg_distance_mi,
                    ROUND(AVG(trip_duration_min), 2)           AS avg_duration_min,
                    -- Cap per-trip tip% at 100 to neutralize bad-data outliers
                    -- (e.g. fare_amount=$0.01 with $5 tip → 50000%, distorts avg).
                    ROUND(AVG(
                        CASE WHEN fare_amount > 0
                             THEN LEAST(100.0 * tip_amount / fare_amount, 100.0)
                        END
                    ), 2)                                      AS avg_tip_pct,
                    SUM(is_airport_pickup)                     AS airport_pickup_count
                FROM staging_yellow_taxi
                WHERE pickup_date = :d
                GROUP BY pickup_date, pulocationid
                """
            ),
            {"d": partition_date},
        )
        rows = result.rowcount
        run.set_rows_written(rows)

    context.log.info(f"[{asset_name}] {partition_date} → {rows} zone-rows")
    return rows


# -------------------------------------------------------------
# mart_taxi_daily_zone_pair — one row per (date, pickup_zone, dropoff_zone)
# -------------------------------------------------------------
@asset(
    partitions_def=TAXI_PARTITIONS,
    group_name="taxi_mart",
    description="Daily aggregations per pickup-dropoff zone pair — for deadhead remediation.",
)
def mart_taxi_daily_zone_pair(context, mysql: MySQLResource) -> int:
    partition_date = context.partition_key
    asset_name = "mart_taxi_daily_zone_pair"

    with run_logged(mysql, asset_name, partition_date) as run:
        run.conn.execute(
            text("DELETE FROM mart_taxi_daily_zone_pair WHERE partition_date = :d"),
            {"d": partition_date},
        )

        result = run.conn.execute(
            text(
                """
                INSERT INTO mart_taxi_daily_zone_pair (
                    partition_date, pickup_zone_id, dropoff_zone_id,
                    trip_count, total_revenue, avg_distance_mi, avg_duration_min
                )
                SELECT
                    pickup_date                       AS partition_date,
                    pulocationid                      AS pickup_zone_id,
                    dolocationid                      AS dropoff_zone_id,
                    COUNT(*)                          AS trip_count,
                    ROUND(SUM(total_amount), 2)       AS total_revenue,
                    ROUND(AVG(trip_distance), 3)      AS avg_distance_mi,
                    ROUND(AVG(trip_duration_min), 2)  AS avg_duration_min
                FROM staging_yellow_taxi
                WHERE pickup_date = :d
                GROUP BY pickup_date, pulocationid, dolocationid
                """
            ),
            {"d": partition_date},
        )
        rows = result.rowcount
        run.set_rows_written(rows)

    context.log.info(f"[{asset_name}] {partition_date} → {rows} pair-rows")
    return rows


# Asset checks deferred to Day 7 (pipeline monitoring) — Dagster 1.7's
# asset_check + partitioned-asset combination is experimental and we
# already have row-level DQ via pipeline_dq_results.
