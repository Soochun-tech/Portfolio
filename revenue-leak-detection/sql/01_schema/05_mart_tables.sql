-- =============================================================
-- 05_mart_tables.sql
-- Mart layer DDL — pre-aggregated, dashboard-ready data.
--
-- Design choices:
--   * Grain is COARSER than staging (daily × dimension), so dashboards
--     query 100s of rows per day, not millions of trips.
--   * No vendor / payment_type / passenger_count breakdown — those would
--     blow up cardinality without changing the business answer.
--   * Aggregates are SUMs / COUNTs / AVGs that any dashboard tile needs.
--   * `partition_date` PK so Dagster can re-materialize a single day idempotently.
--   * Indexed on the dimension we'll filter dashboards by.
-- =============================================================

USE portfolio;

-- -------------------------------------------------------------
-- mart_taxi_daily_zone
-- One row per (pickup_date, pickup_zone). Used for:
--   * Deadhead analysis — which zones have lots of dropoffs but few pickups?
--   * Demand patterns — daily heatmap by zone
--   * Revenue per zone
-- -------------------------------------------------------------
DROP TABLE IF EXISTS mart_taxi_daily_zone;
CREATE TABLE mart_taxi_daily_zone (
    partition_date       DATE         NOT NULL,
    pickup_zone_id       SMALLINT     NOT NULL,
    -- Volume
    trip_count           INT          NOT NULL,
    -- Revenue
    total_fare           DECIMAL(14,2) NOT NULL,
    total_revenue        DECIMAL(14,2) NOT NULL,    -- includes tips, tolls, surcharges
    avg_fare             DECIMAL(10,2) NOT NULL,
    avg_total            DECIMAL(10,2) NOT NULL,
    -- Distance / duration
    total_distance_mi    DECIMAL(14,2) NOT NULL,
    avg_distance_mi      DECIMAL(10,3) NOT NULL,
    avg_duration_min     DECIMAL(10,2) NOT NULL,
    -- Tip behavior (tip_pct = tip_amount / fare_amount, capped at 100% in mart asset
    -- to neutralize bad-data outliers like fare=$0.01 with $5 tip → 50000%).
    -- DECIMAL(7,2) gives headroom even if cap logic ever changes.
    avg_tip_pct          DECIMAL(7,2),
    -- Airport flag at zone level
    airport_pickup_count INT          NOT NULL,
    -- Bookkeeping
    materialized_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (partition_date, pickup_zone_id),
    KEY idx_zone (pickup_zone_id),
    KEY idx_date (partition_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- -------------------------------------------------------------
-- mart_taxi_daily_zone_pair
-- One row per (pickup_date, pickup_zone, dropoff_zone). Used for:
--   * "Where do trips ENDING in zone X come FROM?" — deadhead remediation
--   * Top routes per day
-- More granular than mart_taxi_daily_zone but still much smaller than staging.
-- -------------------------------------------------------------
DROP TABLE IF EXISTS mart_taxi_daily_zone_pair;
CREATE TABLE mart_taxi_daily_zone_pair (
    partition_date     DATE         NOT NULL,
    pickup_zone_id     SMALLINT     NOT NULL,
    dropoff_zone_id    SMALLINT     NOT NULL,
    trip_count         INT          NOT NULL,
    total_revenue      DECIMAL(14,2) NOT NULL,
    avg_distance_mi    DECIMAL(10,3) NOT NULL,
    avg_duration_min   DECIMAL(10,2) NOT NULL,
    materialized_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (partition_date, pickup_zone_id, dropoff_zone_id),
    KEY idx_dropoff (dropoff_zone_id, partition_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- -------------------------------------------------------------
-- mart_olist_daily_seller
-- One row per (purchase_date, seller_id). Used for:
--   * Seller revenue ranking
--   * On-time delivery rate per seller per day
--   * Review score trends
-- -------------------------------------------------------------
DROP TABLE IF EXISTS mart_olist_daily_seller;
CREATE TABLE mart_olist_daily_seller (
    partition_date     DATE         NOT NULL,
    seller_id          VARCHAR(40)  NOT NULL,
    seller_state       CHAR(2),
    -- Volume
    order_count        INT          NOT NULL,
    item_count         INT          NOT NULL,
    -- Revenue
    gmv                DECIMAL(14,2) NOT NULL,    -- gross merchandise value (price + freight)
    avg_order_value    DECIMAL(10,2) NOT NULL,
    -- Quality (only computed on delivered orders with reviews)
    delivered_count    INT          NOT NULL,
    late_count         INT          NOT NULL,
    late_rate          DECIMAL(5,4),               -- late_count / delivered_count
    avg_review_score   DECIMAL(3,2),
    negative_review_count INT       NOT NULL,    -- review_score <= 2
    -- Bookkeeping
    materialized_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (partition_date, seller_id),
    KEY idx_state (seller_state, partition_date),
    KEY idx_date (partition_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- -------------------------------------------------------------
-- mart_olist_daily_summary
-- Top-level KPI mart — one row per day for the whole platform.
-- Used for the dashboard's "headline" tiles.
-- -------------------------------------------------------------
DROP TABLE IF EXISTS mart_olist_daily_summary;
CREATE TABLE mart_olist_daily_summary (
    partition_date         DATE         PRIMARY KEY,
    order_count            INT          NOT NULL,
    unique_customer_count  INT          NOT NULL,
    gmv                    DECIMAL(14,2) NOT NULL,
    avg_order_value        DECIMAL(10,2) NOT NULL,
    delivered_count        INT          NOT NULL,
    late_count             INT          NOT NULL,
    late_rate              DECIMAL(5,4),
    avg_review_score       DECIMAL(3,2),
    materialized_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
