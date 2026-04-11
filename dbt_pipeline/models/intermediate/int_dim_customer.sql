WITH int_dim_customer__deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id) AS rn
    FROM {{ ref('stg_fact_sales_order') }}
),

int_dim_customer__gen_key AS (
    SELECT
        FARM_FINGERPRINT(user_id) AS customer_key,
        user_id,
        device_id,
        user_agent,
        resolution,
        email_address,
        ip_address
    FROM int_dim_customer__deduplicated
    WHERE rn = 1
)

SELECT * FROM int_dim_customer__gen_key

