-- =============================================================
-- verify_olist_rowcounts.sql
-- Olist 9개 raw 테이블 행 수 검증
-- 기대값:
--   raw_olist_orders               =     99,441
--   raw_olist_order_items          =    112,650
--   raw_olist_order_payments       =    103,886
--   raw_olist_order_reviews        =     99,224
--   raw_olist_customers            =     99,441
--   raw_olist_sellers              =      3,095
--   raw_olist_products             =     32,951
--   raw_olist_geolocation          =  1,000,163
--   raw_olist_category_translation =         71
--   합계                           =  1,550,922
-- =============================================================

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
