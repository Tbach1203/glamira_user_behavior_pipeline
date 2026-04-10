SELECT *,
    FARM_FINGERPRINT(country_name||region_name||city_name) AS location_key
FROM {{ref('stg_dim_location')}}