SELECT DISTINCT
    FARM_FINGERPRINT(CAST(product_id AS STRING)) AS product_key,
    product_id,
    product_name,
    category_id,
    category_name,
    unit_price,
    min_price,
    max_price,
    currency_symbol
FROM {{ ref('stg_product_info') }}