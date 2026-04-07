SELECT
  ip AS ip_address,
  user_agent,
  resolution,
  user_id_db AS user_id,
  device_id,
  store_id,
  order_id,
  cart_products.product_id AS product_id,
  cart_products.amount AS order_qty,
  cart_products.price AS order_total_amount,
  cart_products.currency AS currency_code,
  SAFE_CAST(local_time AS DATE) AS order_local_time,
  time_stamp AS order_timestamp
FROM `glowing-market-485009-u8.raw_layer.glamira_raw`,
UNNEST(cart_products) AS cart_products
WHERE collection = 'checkout_success'