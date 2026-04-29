"""
Export pre-aggregated CSV files for Tableau Public.

Why CSV instead of direct connect:
    Tableau Public (free) doesn't support live database connections.
    We pre-aggregate the data we need into CSV files that get uploaded
    to Tableau Public alongside the workbook.

Files produced (under data/exports/):
    Taxi dashboard:
      taxi_zone_summary.csv      — 1 row per zone, full month aggregation
      taxi_daily_kpi.csv         — 1 row per day, platform totals
      taxi_top_routes.csv        — top 100 (pickup -> dropoff) pairs by trips
      taxi_zone_lookup.csv       — zone metadata (borough, service_zone)

    Olist dashboard:
      olist_seller_leak.csv      — seller leak analysis result (top 100)
      olist_daily_kpi.csv        — 1 row per day platform KPI
      olist_cohort_retention.csv — cohort retention matrix
      olist_state_summary.csv    — seller_state level summary for choropleth

Usage:
    python scripts/export_for_tableau.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def get_engine():
    load_dotenv(ROOT / ".env")
    user = os.environ.get("MYSQL_USER", "analyst")
    pw = os.environ.get("MYSQL_PASSWORD", "analystpw")
    host = os.environ.get("MYSQL_HOST", "localhost")
    port = os.environ.get("MYSQL_PORT", "3306")
    db = os.environ.get("MYSQL_DATABASE", "portfolio")
    url = f"mysql+mysqlconnector://{user}:{pw}@{host}:{port}/{db}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)


# =============================================================
# TAXI EXPORTS
# =============================================================

TAXI_ZONE_SUMMARY_SQL = """
WITH
zone_pickups AS (
    SELECT
        pickup_zone_id            AS zone_id,
        SUM(trip_count)           AS pickup_count,
        SUM(total_revenue)        AS pickup_revenue,
        SUM(airport_pickup_count) AS airport_pickups,
        ROUND(AVG(avg_fare), 2)   AS avg_fare,
        ROUND(AVG(avg_duration_min), 2) AS avg_duration_min,
        ROUND(AVG(avg_tip_pct), 2) AS avg_tip_pct
    FROM mart_taxi_daily_zone
    GROUP BY pickup_zone_id
),
zone_dropoffs AS (
    SELECT
        dropoff_zone_id    AS zone_id,
        SUM(trip_count)    AS dropoff_count,
        SUM(total_revenue) AS dropoff_revenue
    FROM mart_taxi_daily_zone_pair
    GROUP BY dropoff_zone_id
)
SELECT
    zl.locationid                  AS zone_id,
    zl.zone                        AS zone_name,
    zl.borough,
    zl.service_zone,
    COALESCE(zp.pickup_count, 0)   AS pickup_count,
    COALESCE(zd.dropoff_count, 0)  AS dropoff_count,
    COALESCE(zd.dropoff_count, 0) - COALESCE(zp.pickup_count, 0) AS surplus,
    CASE
        WHEN COALESCE(zd.dropoff_count, 0) > 0
        THEN ROUND(
            1.0 * (COALESCE(zd.dropoff_count, 0) - COALESCE(zp.pickup_count, 0))
                / zd.dropoff_count, 4
        )
    END                            AS surplus_ratio,
    COALESCE(zp.pickup_revenue, 0) AS pickup_revenue,
    zp.avg_fare,
    zp.avg_duration_min,
    zp.avg_tip_pct,
    COALESCE(zp.airport_pickups, 0) AS airport_pickups
FROM raw_taxi_zone_lookup zl
LEFT JOIN zone_pickups  zp ON zp.zone_id = zl.locationid
LEFT JOIN zone_dropoffs zd ON zd.zone_id = zl.locationid
WHERE COALESCE(zp.pickup_count, 0) + COALESCE(zd.dropoff_count, 0) > 0
ORDER BY surplus DESC
"""

TAXI_DAILY_KPI_SQL = """
SELECT
    partition_date,
    DAYNAME(partition_date)         AS day_of_week,
    DAYOFWEEK(partition_date) IN (1, 7) AS is_weekend,
    SUM(trip_count)                 AS daily_trips,
    ROUND(SUM(total_revenue), 2)    AS daily_revenue,
    ROUND(AVG(avg_fare), 2)         AS avg_fare,
    ROUND(AVG(avg_duration_min), 2) AS avg_duration_min,
    SUM(airport_pickup_count)       AS airport_pickups
FROM mart_taxi_daily_zone
GROUP BY partition_date
ORDER BY partition_date
"""

TAXI_TOP_ROUTES_SQL = """
SELECT
    p.pickup_zone_id,
    p.dropoff_zone_id,
    pul.zone           AS pickup_zone_name,
    pul.borough        AS pickup_borough,
    dol.zone           AS dropoff_zone_name,
    dol.borough        AS dropoff_borough,
    SUM(p.trip_count)  AS trips,
    ROUND(SUM(p.total_revenue), 2)  AS revenue,
    ROUND(AVG(p.avg_distance_mi), 3) AS avg_distance_mi,
    ROUND(AVG(p.avg_duration_min), 2) AS avg_duration_min
FROM mart_taxi_daily_zone_pair p
LEFT JOIN raw_taxi_zone_lookup pul ON pul.locationid = p.pickup_zone_id
LEFT JOIN raw_taxi_zone_lookup dol ON dol.locationid = p.dropoff_zone_id
GROUP BY p.pickup_zone_id, p.dropoff_zone_id, pul.zone, pul.borough,
         dol.zone, dol.borough
ORDER BY trips DESC
LIMIT 100
"""

TAXI_ZONE_LOOKUP_SQL = """
SELECT
    locationid AS zone_id,
    zone       AS zone_name,
    borough,
    service_zone
FROM raw_taxi_zone_lookup
ORDER BY locationid
"""


# =============================================================
# OLIST EXPORTS
# =============================================================

OLIST_SELLER_LEAK_SQL = """
WITH
seller_orders AS (
    SELECT
        oi.seller_id,
        s.seller_state,
        o.order_id,
        c.customer_unique_id,
        o.is_late,
        r.review_score,
        r.is_negative,
        SUM(oi.item_total) AS order_value
    FROM staging_olist_orders o
    JOIN staging_olist_order_items oi ON oi.order_id = o.order_id
    LEFT JOIN staging_olist_sellers   s ON s.seller_id = oi.seller_id
    LEFT JOIN staging_olist_reviews   r ON r.order_id = o.order_id
    LEFT JOIN staging_olist_customers c ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY
        oi.seller_id, s.seller_state, o.order_id, c.customer_unique_id,
        o.is_late, r.review_score, r.is_negative
),
seller_funnel AS (
    SELECT
        seller_id,
        seller_state,
        COUNT(DISTINCT order_id)            AS total_orders,
        SUM(is_late = TRUE)                 AS late_orders,
        ROUND(1.0 * SUM(is_late = TRUE)
              / NULLIF(COUNT(DISTINCT order_id), 0), 4) AS late_rate,
        ROUND(AVG(review_score), 2)         AS avg_review_score,
        SUM(is_negative = TRUE)             AS negative_review_count,
        ROUND(1.0 * SUM(is_negative = TRUE)
              / NULLIF(COUNT(DISTINCT order_id), 0), 4) AS negative_review_rate,
        ROUND(AVG(order_value), 2)          AS avg_order_value,
        ROUND(SUM(order_value), 2)          AS gmv
    FROM seller_orders
    GROUP BY seller_id, seller_state
)
SELECT
    *,
    ROUND(late_orders * avg_order_value * negative_review_rate, 2) AS est_leak_usd,
    ROW_NUMBER() OVER (
        ORDER BY late_orders * avg_order_value * negative_review_rate DESC
    ) AS leak_rank
FROM seller_funnel
WHERE total_orders >= 50
ORDER BY est_leak_usd DESC
LIMIT 100
"""

OLIST_DAILY_KPI_SQL = """
SELECT
    o.purchase_date                   AS partition_date,
    DAYNAME(o.purchase_date)          AS day_of_week,
    COUNT(DISTINCT o.order_id)        AS order_count,
    COUNT(DISTINCT o.customer_id)     AS unique_customers,
    ROUND(SUM(oi.item_total), 2)      AS gmv,
    ROUND(SUM(oi.item_total)
          / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS avg_order_value,
    SUM(o.order_status = 'delivered') AS delivered_count,
    SUM(o.is_late = TRUE)             AS late_count,
    ROUND(SUM(o.is_late = TRUE)
          / NULLIF(SUM(o.order_status = 'delivered'), 0), 4) AS late_rate,
    ROUND(AVG(r.review_score), 2)     AS avg_review_score
FROM staging_olist_orders o
LEFT JOIN staging_olist_order_items oi ON oi.order_id = o.order_id
LEFT JOIN staging_olist_reviews r      ON r.order_id  = o.order_id
GROUP BY o.purchase_date
ORDER BY o.purchase_date
"""

OLIST_COHORT_SQL = """
WITH
purchases AS (
    SELECT
        c.customer_unique_id,
        DATE(DATE_FORMAT(o.purchase_date, '%Y-%m-01')) AS purchase_month
    FROM staging_olist_orders o
    JOIN staging_olist_customers c ON c.customer_id = o.customer_id
),
first_purchase AS (
    SELECT customer_unique_id, MIN(purchase_month) AS cohort_month
    FROM purchases GROUP BY customer_unique_id
),
purchase_with_cohort AS (
    SELECT
        p.customer_unique_id,
        fp.cohort_month,
        TIMESTAMPDIFF(MONTH, fp.cohort_month, p.purchase_month) AS months_since_first
    FROM purchases p JOIN first_purchase fp USING (customer_unique_id)
),
cohort_activity AS (
    SELECT
        cohort_month,
        months_since_first,
        COUNT(DISTINCT customer_unique_id) AS active_customers
    FROM purchase_with_cohort
    GROUP BY cohort_month, months_since_first
),
cohort_size AS (
    SELECT cohort_month, active_customers AS cohort_size
    FROM cohort_activity WHERE months_since_first = 0
)
SELECT
    ca.cohort_month,
    cs.cohort_size,
    ca.months_since_first,
    ca.active_customers,
    ROUND(1.0 * ca.active_customers / cs.cohort_size, 4) AS retention_rate
FROM cohort_activity ca
JOIN cohort_size cs USING (cohort_month)
WHERE ca.months_since_first <= 12
  AND cs.cohort_size >= 100
ORDER BY ca.cohort_month, ca.months_since_first
"""

OLIST_STATE_SUMMARY_SQL = """
SELECT
    s.seller_state,
    COUNT(DISTINCT s.seller_id)                     AS sellers,
    COUNT(DISTINCT o.order_id)                      AS orders,
    ROUND(SUM(oi.item_total), 2)                    AS gmv,
    SUM(o.is_late = TRUE)                           AS late_orders,
    ROUND(SUM(o.is_late = TRUE)
          / NULLIF(COUNT(DISTINCT o.order_id), 0), 4) AS late_rate,
    ROUND(AVG(r.review_score), 2)                   AS avg_review_score
FROM staging_olist_sellers s
JOIN staging_olist_order_items oi ON oi.seller_id = s.seller_id
JOIN staging_olist_orders o ON o.order_id = oi.order_id
LEFT JOIN staging_olist_reviews r ON r.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY s.seller_state
ORDER BY gmv DESC
"""


# =============================================================
# RUNNER
# =============================================================

EXPORTS = [
    ("taxi_zone_summary",       TAXI_ZONE_SUMMARY_SQL),
    ("taxi_daily_kpi",          TAXI_DAILY_KPI_SQL),
    ("taxi_top_routes",         TAXI_TOP_ROUTES_SQL),
    ("taxi_zone_lookup",        TAXI_ZONE_LOOKUP_SQL),
    ("olist_seller_leak",       OLIST_SELLER_LEAK_SQL),
    ("olist_daily_kpi",         OLIST_DAILY_KPI_SQL),
    ("olist_cohort_retention",  OLIST_COHORT_SQL),
    ("olist_state_summary",     OLIST_STATE_SUMMARY_SQL),
]


def main() -> int:
    engine = get_engine()
    print(f"[i] Exporting to {EXPORT_DIR}")
    print()

    for name, sql in EXPORTS:
        out_path = EXPORT_DIR / f"{name}.csv"
        try:
            df = pd.read_sql(sql, engine)
        except Exception as e:
            print(f"  [!] {name:30s}  FAILED: {e}")
            continue
        df.to_csv(out_path, index=False)
        print(f"  [+] {name:30s}  {len(df):>7,} rows  →  {out_path.name}")

    print()
    print(f"[✓] Done. Files saved under {EXPORT_DIR}")
    print(f"    Open them in Tableau Public via 'Connect to Data → Text File'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
