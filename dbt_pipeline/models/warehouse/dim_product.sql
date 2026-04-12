SELECT 
    product_key,
    product_id,
    product_name,
    category_id,
    category_name,
    unit_price_usd,
    min_price_usd,
    max_price_usd
FROM {{ref('int_dim_product')}}