WITH int_dim_location_gen_key AS (
    SELECT
        FARM_FINGERPRINT(country_name||region_name||city_name) AS location_key, *
    FROM {{ref('stg_dim_location')}}
),

int_dim_location__default_row AS (
    SELECT
        -1 AS location_key,   
        'Unknown' AS country_name,
        'Unknown' AS region_name,
        'Unknown' AS city_name,
        'Unknown' AS ip_address,
),

int_dim_location__final AS (
    SELECT DISTINCT
        location_key,
        country_name,
        region_name,
        city_name,
        ip_address
    FROM int_dim_location_gen_key
    UNION ALL
    SELECT * FROM int_dim_location__default_row
)

SELECT * FROM int_dim_location__final

