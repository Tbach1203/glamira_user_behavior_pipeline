SELECT DISTINCT
    order_local_time AS full_date,
FROM {{ ref('stg_glamira_raw')}}