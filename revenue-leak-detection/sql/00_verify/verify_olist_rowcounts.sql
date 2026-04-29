USE portfolio;

SELECT 'raw_olist_orders'              AS tbl, COUNT(*) AS rows_n FROM raw_olist_orders
UNION ALL SELECT 'raw_olist_order_items',           COUNT(*) FROM raw_olist_order_items
UNION ALL SELECT 'raw_olist_order_payments',        COUNT(*) FROM raw_olist_order_payments
UNION ALL SELECT 'raw_olist_order_reviews',         COUNT(*) FROM raw_olist_order_reviews
UNION ALL SELECT 'raw_olist_customers',             COUNT(*) FROM raw_olist_customers
UNION ALL SELECT 'raw_olist_sellers',               COUNT(*) FROM raw_olist_sellers
UNION ALL SELECT 'raw_olist_products',              COUNT(*) FROM raw_olist_products
UNION ALL SELECT 'raw_olist_geolocation',           COUNT(*) FROM raw_olist_geolocation
UNION ALL SELECT 'raw_olist_category_translation',  COUNT(*) FROM raw_olist_category_translation;
