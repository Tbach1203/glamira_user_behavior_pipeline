WITH distinct_date AS (
    SELECT DISTINCT
        order_local_time AS full_date,
    FROM {{ ref('stg_fact_sales_order')}}
)

SELECT 
    TO_HEX(MD5(CAST(full_date AS STRING))) AS date_key,
    full_date, 
    EXTRACT(DAY FROM full_date) AS day,
    EXTRACT(MONTH FROM full_date) AS month,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    EXTRACT(YEAR FROM full_date) AS year,
    FORMAT_DATE('%A', full_date) AS day_name,
    FORMAT_DATE('%B', full_date) AS month_name,
    EXTRACT(DAYOFWEEK FROM full_date) IN (1,7) AS is_weekend,
FROM distinct_date
ORDER BY full_date