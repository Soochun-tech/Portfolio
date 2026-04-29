USE portfolio;

DROP TABLE IF EXISTS mart_taxi_daily_zone;
CREATE TABLE mart_taxi_daily_zone (
    partition_date       DATE         NOT NULL,
    pickup_zone_id       SMALLINT     NOT NULL,
    trip_count           INT          NOT NULL,
    total_fare           DECIMAL(14,2) NOT NULL,
    total_revenue        DECIMAL(14,2) NOT NULL,
    avg_fare             DECIMAL(10,2) NOT NULL,
    avg_total            DECIMAL(10,2) NOT NULL,
    total_distance_mi    DECIMAL(14,2) NOT NULL,
    avg_distance_mi      DECIMAL(10,3) NOT NULL,
    avg_duration_min     DECIMAL(10,2) NOT NULL,
    avg_tip_pct          DECIMAL(7,2),
    airport_pickup_count INT          NOT NULL,
    materialized_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (partition_date, pickup_zone_id),
    KEY idx_zone (pickup_zone_id),
    KEY idx_date (partition_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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

DROP TABLE IF EXISTS mart_olist_daily_seller;
CREATE TABLE mart_olist_daily_seller (
    partition_date     DATE         NOT NULL,
    seller_id          VARCHAR(40)  NOT NULL,
    seller_state       CHAR(2),
    order_count        INT          NOT NULL,
    item_count         INT          NOT NULL,
    gmv                DECIMAL(14,2) NOT NULL,
    avg_order_value    DECIMAL(10,2) NOT NULL,
    delivered_count    INT          NOT NULL,
    late_count         INT          NOT NULL,
    late_rate          DECIMAL(5,4),
    avg_review_score   DECIMAL(3,2),
    negative_review_count INT       NOT NULL,
    materialized_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (partition_date, seller_id),
    KEY idx_state (seller_state, partition_date),
    KEY idx_date (partition_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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
