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
        email_address
    FROM int_dim_customer__deduplicated
    WHERE rn = 1
),

int_dim_customer__default_row AS (
    SELECT
        -1 AS customer_key,   
        'Unknown' AS user_id,
        'Unknown' AS device_id,
        'Unknown' AS user_agent,
        'Unknown' AS resolution,
        'Unknown' AS email_address
),

int_dim_customer__final AS (
    SELECT *
    FROM int_dim_customer__gen_key 
    UNION ALL
    SELECT * FROM int_dim_customer__default_row
)

SELECT * FROM int_dim_customer__final

