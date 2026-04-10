WITH stg_fact_sales_order__rename_col AS (
  SELECT DISTINCT
  ip AS ip_address,
  user_agent,
  resolution,
  user_id_db AS user_id,
  device_id,
  store_id,
  order_id,
  email_address,
  SAFE_CAST(cart_products.product_id AS INT) AS product_id,
  cart_products.amount AS order_qty,
  SAFE_CAST(cart_products.price AS NUMERIC) AS item_price,
  cart_products.currency AS currency_code,
  DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', local_time)) AS order_local_time,
  time_stamp AS order_timestamp
FROM {{source('glamira_src', 'glamira_raw')}},
UNNEST(cart_products) AS cart_products
WHERE collection = 'checkout_success'
),

stg_fact_sales_order__deduplicated AS (
  SELECT DISTINCT * 
  FROM stg_fact_sales_order__rename_col
)

SELECT * 
FROM stg_fact_sales_order__deduplicated