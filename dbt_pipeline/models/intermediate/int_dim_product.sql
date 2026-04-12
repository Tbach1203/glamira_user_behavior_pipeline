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

int_dim_product__exchange_rate AS (
    SELECT
        idp.*,
        SAFE_CAST(ROUND(idp.unit_price * SAFE_CAST(e.rate_to_usd AS NUMERIC), 2)AS NUMERIC) AS unit_price_usd,
        SAFE_CAST(ROUND(idp.min_price  * SAFE_CAST(e.rate_to_usd AS NUMERIC), 2)AS NUMERIC) AS min_price_usd,
        SAFE_CAST(ROUND(idp.max_price  * SAFE_CAST(e.rate_to_usd AS NUMERIC), 2)AS NUMERIC) AS max_price_usd
    FROM int_dim_product__gen_key idp 
    LEFT JOIN {{ ref('exchange_rates') }} e
    ON idp.currency_code = e.currency_code
),

int_dim_product__default_row AS (
    SELECT
        -1 AS product_key,
        -1 AS product_id,
        'Unknown' AS product_name,
        -1 AS category_id,
        'Unknown' AS category_name,
        CAST(0.00 AS NUMERIC) AS unit_price,
        CAST(0.00 AS NUMERIC) AS min_price,
        CAST(0.00 AS NUMERIC) AS max_price,
        'Unknown' AS currency_code,
        CAST(0.00 AS NUMERIC) AS unit_price_usd,
        CAST(0.00 AS NUMERIC) AS min_price_usd,
        CAST(0.00 AS NUMERIC) AS max_price_usd
        
),

int_dim_product__final AS (
    SELECT * FROM int_dim_product__exchange_rate
    UNION ALL
    SELECT * FROM int_dim_product__default_row
)

SELECT * FROM int_dim_product__final

