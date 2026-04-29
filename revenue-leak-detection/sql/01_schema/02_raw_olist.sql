USE portfolio;

DROP TABLE IF EXISTS raw_olist_orders;
CREATE TABLE raw_olist_orders (
    order_id                       VARCHAR(40),
    customer_id                    VARCHAR(40),
    order_status                   VARCHAR(20),
    order_purchase_timestamp       DATETIME,
    order_approved_at              DATETIME,
    order_delivered_carrier_date   DATETIME,
    order_delivered_customer_date  DATETIME,
    order_estimated_delivery_date  DATETIME,
    KEY idx_order (order_id),
    KEY idx_customer (customer_id),
    KEY idx_purchase_dt (order_purchase_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS raw_olist_order_items;
CREATE TABLE raw_olist_order_items (
    order_id            VARCHAR(40),
    order_item_id       INT,
    product_id          VARCHAR(40),
    seller_id           VARCHAR(40),
    shipping_limit_date DATETIME,
    price               DECIMAL(10,2),
    freight_value       DECIMAL(10,2),
    KEY idx_order (order_id),
    KEY idx_product (product_id),
    KEY idx_seller (seller_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS raw_olist_order_payments;
CREATE TABLE raw_olist_order_payments (
    order_id              VARCHAR(40),
    payment_sequential    INT,
    payment_type          VARCHAR(20),
    payment_installments  INT,
    payment_value         DECIMAL(10,2),
    KEY idx_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS raw_olist_order_reviews;
CREATE TABLE raw_olist_order_reviews (
    review_id                VARCHAR(40),
    order_id                 VARCHAR(40),
    review_score             TINYINT,
    review_comment_title     VARCHAR(255),
    review_comment_message   TEXT,
    review_creation_date     DATETIME,
    review_answer_timestamp  DATETIME,
    KEY idx_order (order_id),
    KEY idx_review (review_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS raw_olist_customers;
CREATE TABLE raw_olist_customers (
    customer_id              VARCHAR(40),
    customer_unique_id       VARCHAR(40),
    customer_zip_code_prefix VARCHAR(10),
    customer_city            VARCHAR(100),
    customer_state           CHAR(2),
    KEY idx_customer (customer_id),
    KEY idx_unique (customer_unique_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS raw_olist_sellers;
CREATE TABLE raw_olist_sellers (
    seller_id              VARCHAR(40),
    seller_zip_code_prefix VARCHAR(10),
    seller_city            VARCHAR(100),
    seller_state           CHAR(2),
    KEY idx_seller (seller_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS raw_olist_products;
CREATE TABLE raw_olist_products (
    product_id                  VARCHAR(40),
    product_category_name       VARCHAR(100),
    product_name_lenght         INT,
    product_description_lenght  INT,
    product_photos_qty          INT,
    product_weight_g            INT,
    product_length_cm           INT,
    product_height_cm           INT,
    product_width_cm            INT,
    KEY idx_product (product_id),
    KEY idx_category (product_category_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS raw_olist_geolocation;
CREATE TABLE raw_olist_geolocation (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat             DECIMAL(11,8),
    geolocation_lng             DECIMAL(11,8),
    geolocation_city            VARCHAR(100),
    geolocation_state           CHAR(2),
    KEY idx_zip (geolocation_zip_code_prefix)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS raw_olist_category_translation;
CREATE TABLE raw_olist_category_translation (
    product_category_name         VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
