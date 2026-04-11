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
),

int_dim_product__default_row AS (
    SELECT
        -1 AS product_key,
        -1 AS product_id,
        'Unknown' AS product_name,
        -1 AS category_id,
        'Unknown' AS category_name,
        0.00 AS unit_price,
        0.00 AS min_price,
        0.00 AS max_price,
        'Unknown' AS currency_code
),

int_dim_product__final AS (
    SELECT * FROM int_dim_product__gen_key
    UNION ALL
    SELECT * FROM int_dim_product__default_row
)

SELECT * FROM int_dim_product__final

