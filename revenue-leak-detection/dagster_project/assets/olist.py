from __future__ import annotations

from dagster import (
    DailyPartitionsDefinition,
    asset,
)
from sqlalchemy import text

from ..resources import MySQLResource
from ._runlog import run_logged

OLIST_PARTITIONS = DailyPartitionsDefinition(
    start_date="2017-01-01",
    end_date="2018-12-01",
)

@asset(
    partitions_def=OLIST_PARTITIONS,
    group_name="olist_mart",
    description="Daily seller-level KPIs — revenue, late rate, review score.",
)
def mart_olist_daily_seller(context, mysql: MySQLResource) -> int:
    partition_date = context.partition_key
    asset_name = "mart_olist_daily_seller"

    with run_logged(mysql, asset_name, partition_date) as run:
        run.conn.execute(
            text("DELETE FROM mart_olist_daily_seller WHERE partition_date = :d"),
            {"d": partition_date},
        )

        result = run.conn.execute(
            text(
                """
                INSERT INTO mart_olist_daily_seller (partition_date, seller_id, seller_state, order_count, item_count, gmv, avg_order_value, delivered_count, late_count, late_rate, avg_review_score, negative_review_count)
                SELECT
                    o.purchase_date                              AS partition_date,
                    oi.seller_id,
                    s.seller_state,
                    COUNT(DISTINCT o.order_id)                   AS order_count,
                    COUNT(*)                                     AS item_count,
                    ROUND(SUM(oi.item_total), 2)                 AS gmv,
                    ROUND(SUM(oi.item_total) / NULLIF(COUNT(DISTINCT o.order_id), 0), 2)
                                                                 AS avg_order_value,
                    SUM(o.order_status = 'delivered')            AS delivered_count,
                    SUM(o.is_late = TRUE)                        AS late_count,
                    ROUND(
                        SUM(o.is_late = TRUE) /
                        NULLIF(SUM(o.order_status = 'delivered'), 0),
                        4
                    )                                            AS late_rate,
                    ROUND(AVG(r.review_score), 2)                AS avg_review_score,
                    SUM(r.is_negative = TRUE)                    AS negative_review_count
                FROM staging_olist_orders o
                JOIN staging_olist_order_items oi ON oi.order_id = o.order_id
                LEFT JOIN staging_olist_sellers s ON s.seller_id = oi.seller_id
                LEFT JOIN staging_olist_reviews r ON r.order_id = o.order_id
                WHERE o.purchase_date = :d
                GROUP BY o.purchase_date, oi.seller_id, s.seller_state
                """
            ),
            {"d": partition_date},
        )
        rows = result.rowcount
        run.set_rows_written(rows)

    context.log.info(f"[{asset_name}] {partition_date} → {rows} seller-rows")
    return rows

@asset(
    partitions_def=OLIST_PARTITIONS,
    group_name="olist_mart",
    description="Daily platform KPIs — one row per day for top-line dashboard tiles.",
)
def mart_olist_daily_summary(context, mysql: MySQLResource) -> int:
    partition_date = context.partition_key
    asset_name = "mart_olist_daily_summary"

    with run_logged(mysql, asset_name, partition_date) as run:
        run.conn.execute(
            text("DELETE FROM mart_olist_daily_summary WHERE partition_date = :d"),
            {"d": partition_date},
        )

        result = run.conn.execute(
            text(
                """
                INSERT INTO mart_olist_daily_summary (partition_date, order_count, unique_customer_count, gmv, avg_order_value, delivered_count, late_count, late_rate, avg_review_score)
                SELECT
                    o.purchase_date                          AS partition_date,
                    COUNT(DISTINCT o.order_id)               AS order_count,
                    COUNT(DISTINCT o.customer_id)            AS unique_customer_count,
                    ROUND(COALESCE(SUM(oi.item_total), 0), 2) AS gmv,
                    ROUND(
                        COALESCE(SUM(oi.item_total), 0) /
                        NULLIF(COUNT(DISTINCT o.order_id), 0),
                        2
                    )                                        AS avg_order_value,
                    SUM(o.order_status = 'delivered')        AS delivered_count,
                    SUM(o.is_late = TRUE)                    AS late_count,
                    ROUND(
                        SUM(o.is_late = TRUE) /
                        NULLIF(SUM(o.order_status = 'delivered'), 0),
                        4
                    )                                        AS late_rate,
                    ROUND(AVG(r.review_score), 2)            AS avg_review_score
                FROM staging_olist_orders o
                LEFT JOIN staging_olist_order_items oi ON oi.order_id = o.order_id
                LEFT JOIN staging_olist_reviews r     ON r.order_id  = o.order_id
                WHERE o.purchase_date = :d
                GROUP BY o.purchase_date
                """
            ),
            {"d": partition_date},
        )
        rows = result.rowcount
        run.set_rows_written(rows)

    context.log.info(f"[{asset_name}] {partition_date} → {rows} summary-rows")
    return rows
