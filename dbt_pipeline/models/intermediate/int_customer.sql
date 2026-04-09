WITH deduped_customer AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
        ) AS rn
    FROM {{ ref('stg_glamira_raw') }}
)

SELECT
    TO_HEX(MD5(CAST(user_id AS STRING))) AS customer_key,
    user_id,
    device_id,
    user_agent,
    resolution,
    email_address,
    ip_address
FROM deduped_customer
WHERE rn = 1