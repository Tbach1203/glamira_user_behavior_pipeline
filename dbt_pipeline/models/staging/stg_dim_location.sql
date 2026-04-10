WITH stg_dim_location__rename_col AS (
    SELECT
        ip AS ip_address,
        country AS country_name,
        region AS region_name,
        city AS city_name
    FROM {{source ('glamira_src', 'ip_locations')}}
),

stg_dim_location__deduplicated AS (
    SELECT DISTINCT *
    FROM stg_dim_location__rename_col
)

SELECT *
FROM stg_dim_location__deduplicated


