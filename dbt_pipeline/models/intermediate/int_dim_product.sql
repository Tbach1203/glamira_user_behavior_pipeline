WITH int_dim_product__gen_key AS (
    SELECT 
        FARM_FINGERPRINT(CAST(product_id AS STRING)) AS product_key,
        product_id,
        product_name,
        category_id,
        category_name,
        unit_price,
        min_price,
        max_price,
        currency_code
    FROM {{ ref('stg_dim_product') }}
)

SELECT * FROM int_dim_product__gen_key

