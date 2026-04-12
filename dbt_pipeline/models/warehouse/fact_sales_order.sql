WITH fact_sales_order__join AS (
    SELECT 
        f.fact_sales_order_key,
        f.order_id,
        COALESCE(p.product_key,  -1) AS product_key,
        COALESCE(c.customer_key, -1) AS customer_key,
        COALESCE(l.location_key, -1) AS location_key,
        COALESCE(d.date_key,     -1) AS date_key,
        f.store_id,

        -- Timestamps
        f.order_local_time,
        f.order_timestamp,
        f.currency_code,

        -- Measures
        f.order_qty,
        f.item_price_usd * f.order_qty AS line_total_price,
        SUM(f.item_price_usd * f.order_qty) OVER (PARTITION BY f.order_id) AS order_total_amount,
        
    FROM {{ref('int_fact_sales_order')}} f
    LEFT JOIN {{ref('dim_customer')}} c 
        ON f.user_id = c.user_id
    
    LEFT JOIN {{ref('dim_product')}} p 
        ON f.product_id = p.product_id
    
    LEFT JOIN {{ref('dim_date')}} d 
        ON f.order_local_time = d.full_date

    LEFT JOIN {{ref('int_dim_location')}} l 
        ON f.ip_address = l.ip_address
)

SELECT * FROM fact_sales_order__join