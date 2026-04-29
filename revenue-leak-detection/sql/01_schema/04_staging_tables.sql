-- =============================================================
-- 04_staging_tables.sql
-- Staging layer DDL — cleaned, typed, slightly enriched data
--
-- Design choices:
--   * 1:1 grain with raw (no aggregation yet — that goes to mart layer)
--   * Add a few low-cost derived columns that 90% of analysis queries need
--     (trip_duration_minutes, delivery_delay_days, etc.) so we don't compute
--     them on every read.
--   * Indexed on the columns we'll filter/group by most: pickup_date,
--     pulocationid, customer_id, order_id.
--   * surrogate trip_id PK so we can dedupe + reference if needed.
-- =============================================================

USE portfolio;

-- -------------------------------------------------------------
-- staging_yellow_taxi
-- One row per cleaned trip. Source: raw_yellow_taxi
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging_yellow_taxi;
CREATE TABLE staging_yellow_taxi (
    trip_id              BIGINT       PRIMARY KEY AUTO_INCREMENT,
    -- Core trip facts
    vendorid             TINYINT      NOT NULL,
    pickup_datetime      DATETIME     NOT NULL,
    dropoff_datetime     DATETIME     NOT NULL,
    -- passenger_count: ~6% of NYC TLC trips have 0 or NULL here.
    -- We don't drop these rows because revenue/route fields are still valid;
    -- we just preserve NULL so analysis can ignore unknowns.
    passenger_count      TINYINT      NULL,
    trip_distance        DECIMAL(10,3) NOT NULL,
    pulocationid         SMALLINT     NOT NULL,
    dolocationid         SMALLINT     NOT NULL,
    payment_type         TINYINT,
    ratecodeid           TINYINT,
    -- Money breakdown
    fare_amount          DECIMAL(10,2) NOT NULL,
    extra                DECIMAL(10,2),
    mta_tax              DECIMAL(10,2),
    tip_amount           DECIMAL(10,2),
    tolls_amount         DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    airport_fee          DECIMAL(10,2),
    total_amount         DECIMAL(10,2) NOT NULL,
    -- Derived columns (cheap to compute once, queried many times)
    pickup_date          DATE         NOT NULL,
    pickup_hour          TINYINT      NOT NULL,
    trip_duration_min    DECIMAL(10,2) NOT NULL,
    fare_per_mile        DECIMAL(10,2),
    is_airport_pickup    BOOLEAN      NOT NULL DEFAULT FALSE,
    is_airport_dropoff   BOOLEAN      NOT NULL DEFAULT FALSE,
    -- Bookkeeping
    loaded_at            DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Indexes for common query patterns
    KEY idx_pickup_date  (pickup_date),
    KEY idx_pu_zone_date (pulocationid, pickup_date),
    KEY idx_do_zone_date (dolocationid, pickup_date),
    KEY idx_pickup_hour  (pickup_date, pickup_hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- -------------------------------------------------------------
-- staging_olist_orders
-- One row per cleaned order, with delivery delay computed.
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging_olist_orders;
CREATE TABLE staging_olist_orders (
    order_id                    VARCHAR(40)  PRIMARY KEY,
    customer_id                 VARCHAR(40)  NOT NULL,
    order_status                VARCHAR(20)  NOT NULL,
    order_purchase_ts           DATETIME     NOT NULL,
    order_approved_ts           DATETIME,
    order_delivered_carrier_ts  DATETIME,
    order_delivered_customer_ts DATETIME,
    order_estimated_delivery    DATETIME     NOT NULL,
    -- Derived columns
    purchase_date               DATE         NOT NULL,
    delivery_delay_days         INT,         -- (delivered - estimated). Negative = on time / early.
    is_late                     BOOLEAN,     -- delivery_delay_days > 0
    days_to_deliver             INT,         -- delivered_customer - purchase
    -- Bookkeeping
    loaded_at                   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_customer (customer_id),
    KEY idx_purchase_date (purchase_date),
    KEY idx_status (order_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- -------------------------------------------------------------
-- staging_olist_order_items
-- One row per (order_id, order_item_id) pair.
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging_olist_order_items;
CREATE TABLE staging_olist_order_items (
    order_id              VARCHAR(40)  NOT NULL,
    order_item_id         INT          NOT NULL,
    product_id            VARCHAR(40)  NOT NULL,
    seller_id             VARCHAR(40)  NOT NULL,
    shipping_limit_date   DATETIME,
    price                 DECIMAL(10,2) NOT NULL,
    freight_value         DECIMAL(10,2) NOT NULL,
    item_total            DECIMAL(10,2) NOT NULL,    -- price + freight_value
    PRIMARY KEY (order_id, order_item_id),
    KEY idx_product (product_id),
    KEY idx_seller (seller_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- -------------------------------------------------------------
-- staging_olist_payments
-- Aggregated to one row per order_id (a single order can have multiple
-- payment methods like installments + voucher; we sum to total + keep
-- the dominant method as "primary").
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging_olist_payments;
CREATE TABLE staging_olist_payments (
    order_id              VARCHAR(40)  PRIMARY KEY,
    primary_payment_type  VARCHAR(20)  NOT NULL,    -- highest-value method
    payment_total         DECIMAL(10,2) NOT NULL,
    payment_installments  TINYINT      NOT NULL,
    payment_method_count  TINYINT      NOT NULL     -- how many methods used
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- -------------------------------------------------------------
-- staging_olist_reviews
-- Deduped — one row per order_id (latest review wins if multiple).
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging_olist_reviews;
CREATE TABLE staging_olist_reviews (
    order_id                VARCHAR(40)  PRIMARY KEY,
    review_id               VARCHAR(40)  NOT NULL,
    review_score            TINYINT      NOT NULL,
    review_creation_date    DATE,
    review_answer_ts        DATETIME,
    has_comment             BOOLEAN      NOT NULL DEFAULT FALSE,
    -- Derived
    is_negative             BOOLEAN      NOT NULL DEFAULT FALSE,    -- score <= 2
    is_positive             BOOLEAN      NOT NULL DEFAULT FALSE,    -- score >= 4
    KEY idx_score (review_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- -------------------------------------------------------------
-- staging_olist_customers
-- Standardized state codes, customer-level.
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging_olist_customers;
CREATE TABLE staging_olist_customers (
    customer_id            VARCHAR(40)  PRIMARY KEY,
    customer_unique_id     VARCHAR(40)  NOT NULL,    -- repeat-customer key
    customer_zip_prefix    VARCHAR(10),
    customer_city          VARCHAR(100),
    customer_state         CHAR(2)      NOT NULL,
    KEY idx_unique (customer_unique_id),
    KEY idx_state (customer_state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- -------------------------------------------------------------
-- staging_olist_products
-- Standardized + English category names joined in.
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging_olist_products;
CREATE TABLE staging_olist_products (
    product_id            VARCHAR(40)  PRIMARY KEY,
    category_pt           VARCHAR(60),                -- original Portuguese
    category_en           VARCHAR(60),                -- joined from translation table
    name_length           INT,
    description_length    INT,
    photo_count           INT,
    weight_g              INT,
    length_cm             INT,
    height_cm             INT,
    width_cm              INT,
    KEY idx_category_en (category_en)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- -------------------------------------------------------------
-- staging_olist_sellers
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging_olist_sellers;
CREATE TABLE staging_olist_sellers (
    seller_id             VARCHAR(40)  PRIMARY KEY,
    seller_zip_prefix     VARCHAR(10),
    seller_city           VARCHAR(100),
    seller_state          CHAR(2)      NOT NULL,
    KEY idx_state (seller_state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
