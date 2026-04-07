SELECT 
  product_id,
  name AS product_name,
  category AS category_id,
  category_name,
  SAFE_CAST(price AS NUMERIC) AS unit_price,
  SAFE_CAST(min_price AS NUMERIC) AS min_price,
  SAFE_CAST(max_price AS NUMERIC) AS max_price,
  qty AS product_qty,
  TRIM(REGEXP_REPLACE(min_price_format, r'[\d,\.]', '')) as currency_symbol
FROM raw_layer.product_info