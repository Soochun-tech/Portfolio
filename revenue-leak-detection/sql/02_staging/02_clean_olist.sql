
USE portfolio;
TRUNCATE TABLE staging_olist_orders;

INSERT INTO staging_olist_orders (
    order_id, customer_id, order_status,
    order_purchase_ts, order_approved_ts,
    order_delivered_carrier_ts, order_delivered_customer_ts,
    order_estimated_delivery,
    purchase_date, delivery_delay_days, is_late, days_to_deliver
)
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    DATE(order_purchase_timestamp) AS purchase_date,
    -- delay = (actual delivered - estimated). Negative = on time/early. NULL if not delivered.
    CASE
        WHEN order_delivered_customer_date IS NOT NULL
        THEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date)
    END AS delivery_delay_days,
    CASE
        WHEN order_delivered_customer_date IS NULL THEN NULL
        WHEN order_delivered_customer_date > order_estimated_delivery_date THEN TRUE
        ELSE FALSE
    END AS is_late,
    CASE
        WHEN order_delivered_customer_date IS NOT NULL
        THEN DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)
    END AS days_to_deliver
FROM raw_olist_orders
WHERE
    -- Drop internally inconsistent rows: status says delivered but no delivery date
    NOT (order_status = 'delivered' AND order_delivered_customer_date IS NULL)
    -- Drop chronologically broken rows (delivered before purchased)
    AND NOT (order_delivered_customer_date IS NOT NULL
             AND order_delivered_customer_date < order_purchase_timestamp);

TRUNCATE TABLE staging_olist_order_items;

INSERT INTO staging_olist_order_items (
    order_id, order_item_id, product_id, seller_id,
    shipping_limit_date, price, freight_value, item_total
)
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.shipping_limit_date,
    oi.price,
    oi.freight_value,
    ROUND(oi.price + oi.freight_value, 2) AS item_total
FROM raw_olist_order_items oi
-- Only keep items whose order survived staging cleaning
WHERE EXISTS (SELECT 1 FROM staging_olist_orders o WHERE o.order_id = oi.order_id)
  AND oi.price >= 0
  AND oi.freight_value >= 0;

TRUNCATE TABLE staging_olist_payments;

INSERT INTO staging_olist_payments (
    order_id, primary_payment_type, payment_total,
    payment_installments, payment_method_count
)
WITH
-- Sum value per (order, payment_type)
per_order_method AS (
    SELECT
        order_id,
        payment_type,
        SUM(payment_value)        AS method_value,
        MAX(payment_installments) AS installments
    FROM raw_olist_order_payments
    WHERE payment_value > 0           
    GROUP BY order_id, payment_type
),

ranked AS (
    SELECT
        order_id,
        payment_type,
        method_value,
        installments,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY method_value DESC) AS rn
    FROM per_order_method
),
-- Aggregate to order level
order_totals AS (
    SELECT
        order_id,
        SUM(method_value)   AS payment_total,
        COUNT(*)            AS payment_method_count,
        MAX(installments)   AS max_installments
    FROM per_order_method
    GROUP BY order_id
)
SELECT
    o.order_id,
    r.payment_type           AS primary_payment_type,
    ROUND(o.payment_total, 2) AS payment_total,
    o.max_installments       AS payment_installments,
    o.payment_method_count
FROM order_totals o
JOIN ranked r ON r.order_id = o.order_id AND r.rn = 1
-- Only orders that survived staging
WHERE EXISTS (SELECT 1 FROM staging_olist_orders so WHERE so.order_id = o.order_id);

TRUNCATE TABLE staging_olist_reviews;

INSERT INTO staging_olist_reviews (
    order_id, review_id, review_score,
    review_creation_date, review_answer_ts,
    has_comment, is_negative, is_positive
)
WITH ranked_reviews AS (
    SELECT
        order_id,
        review_id,
        review_score,
        review_creation_date,
        review_answer_timestamp,
        review_comment_message,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY COALESCE(review_answer_timestamp, review_creation_date) DESC,
                     review_id DESC         
        ) AS rn
    FROM raw_olist_order_reviews
    WHERE review_score BETWEEN 1 AND 5     
)
SELECT
    order_id,
    review_id,
    review_score,
    review_creation_date,
    review_answer_timestamp,
    (review_comment_message IS NOT NULL AND TRIM(review_comment_message) <> '') AS has_comment,
    review_score <= 2 AS is_negative,
    review_score >= 4 AS is_positive
FROM ranked_reviews
WHERE rn = 1
  AND EXISTS (SELECT 1 FROM staging_olist_orders so WHERE so.order_id = ranked_reviews.order_id);

TRUNCATE TABLE staging_olist_customers;

INSERT INTO staging_olist_customers (
    customer_id, customer_unique_id, customer_zip_prefix,
    customer_city, customer_state
)
SELECT
    customer_id,
    customer_unique_id,
    LPAD(customer_zip_code_prefix, 5, '0') AS customer_zip_prefix,  -- some zips lose leading zero
    -- City: trim + Title case-ish (just trim+lower for Brazilian convention)
    LOWER(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state
FROM raw_olist_customers
WHERE customer_state IS NOT NULL
  AND CHAR_LENGTH(TRIM(customer_state)) = 2;


TRUNCATE TABLE staging_olist_sellers;

INSERT INTO staging_olist_sellers (seller_id, seller_zip_prefix, seller_city, seller_state)
SELECT seller_id,LPAD(seller_zip_code_prefix, 5, '0') AS seller_zip_prefix,
       LOWER(TRIM(seller_city)) AS seller_city,
       UPPER(TRIM(seller_state)) AS seller_state

    FROM raw_olist_sellers
WHERE seller_state IS NOT NULL
  AND CHAR_LENGTH(TRIM(seller_state)) = 2;

TRUNCATE TABLE staging_olist_products;

INSERT INTO staging_olist_products ( product_id, category_pt, category_en,name_length, description_length, photo_count,weight_g, length_cm, height_cm, width_cm)
SELECT
    p.product_id,
    p.product_category_name                AS category_pt,
    t.product_category_name_english        AS category_en,
    p.product_name_lenght                  AS name_length,
    p.product_description_lenght           AS description_length,
    p.product_photos_qty                   AS photo_count,
    p.product_weight_g                     AS weight_g,
    p.product_length_cm                    AS length_cm,
    p.product_height_cm                    AS height_cm,
    p.product_width_cm                     AS width_cm
FROM raw_olist_products p
LEFT JOIN raw_olist_category_translation t
       ON t.product_category_name = p.product_category_name
WHERE p.product_category_name IS NOT NULL;

SELECT 'orders'        AS tbl, (SELECT COUNT(*) FROM raw_olist_orders)         AS raw_n, (SELECT COUNT(*) FROM staging_olist_orders)         AS stg_n
UNION ALL SELECT 'order_items',  (SELECT COUNT(*) FROM raw_olist_order_items),    (SELECT COUNT(*) FROM staging_olist_order_items)
UNION ALL SELECT 'payments',     (SELECT COUNT(*) FROM raw_olist_order_payments), (SELECT COUNT(*) FROM staging_olist_payments)
UNION ALL SELECT 'reviews',      (SELECT COUNT(*) FROM raw_olist_order_reviews),  (SELECT COUNT(*) FROM staging_olist_reviews)
UNION ALL SELECT 'customers',    (SELECT COUNT(*) FROM raw_olist_customers),      (SELECT COUNT(*) FROM staging_olist_customers)
UNION ALL SELECT 'sellers',      (SELECT COUNT(*) FROM raw_olist_sellers),        (SELECT COUNT(*) FROM staging_olist_sellers)
UNION ALL SELECT 'products',     (SELECT COUNT(*) FROM raw_olist_products),       (SELECT COUNT(*) FROM staging_olist_products);
