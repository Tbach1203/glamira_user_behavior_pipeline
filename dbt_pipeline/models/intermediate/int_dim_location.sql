WITH int_dim_location_gen_key AS (
    SELECT DISTINCT
        FARM_FINGERPRINT(country_name||region_name||city_name) AS location_key, *
    FROM {{ref('stg_dim_location')}}
)

SELECT * FROM int_dim_location_gen_key