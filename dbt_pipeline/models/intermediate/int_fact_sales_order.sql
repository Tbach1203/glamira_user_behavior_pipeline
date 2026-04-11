WITH int_fact_sales_order__gen_key AS (
    SELECT
        FARM_FINGERPRINT(order_id || CAST(product_id AS STRING)) as fact_sales_order_key, *
    FROM {{ref ('stg_fact_sales_order')}}
)

SELECT * FROM int_fact_sales_order__gen_key