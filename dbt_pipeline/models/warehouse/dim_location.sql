SELECT DISTINCT
    location_key,
    country_name,
    region_name,
    city_name
FROM {{ref('int_dim_location')}} 





