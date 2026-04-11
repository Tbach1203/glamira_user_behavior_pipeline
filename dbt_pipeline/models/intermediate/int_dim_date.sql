WITH int_dim_date__deduplicated AS (
    SELECT DISTINCT
        order_local_time AS full_date,
    FROM {{ ref('stg_fact_sales_order')}}
),

int_dim_date__gen_key AS (
    SELECT 
        FARM_FINGERPRINT(CAST(full_date AS STRING)) AS date_key,
        full_date, 
        EXTRACT(DAY FROM full_date) AS day,
        EXTRACT(MONTH FROM full_date) AS month,
        EXTRACT(QUARTER FROM full_date) AS quarter,
        EXTRACT(YEAR FROM full_date) AS year,
        FORMAT_DATE('%A', full_date) AS day_name,
        FORMAT_DATE('%B', full_date) AS month_name,
        EXTRACT(DAYOFWEEK FROM full_date) IN (1,7) AS is_weekend,
    FROM int_dim_date__deduplicated
),

int_dim_date__default_row AS (
    SELECT
        -1 AS date_key,
        CAST(NULL AS DATE) AS full_date,
        -1 AS day,
        -1 AS month,
        -1 AS quarter,
        -1 AS year,
        'Unknown' AS day_name,
        'Unknown' AS month_name,
        FALSE AS is_weekend
),

int_dim_date__final AS (
    SELECT * FROM int_dim_date__gen_key
    UNION ALL
    SELECT * FROM int_dim_date__default_row
)

SELECT * FROM int_dim_date__final


