WITH stg_fact_sales_order__rename_col AS (
  SELECT
    ip AS ip_address,
    user_agent,
    resolution,
    COALESCE(user_id_db,CONCAT('guest_', device_id)) AS user_id, 
    device_id,
    store_id,
    order_id,
    email_address,
    SAFE_CAST(cart_products.product_id AS INT) AS product_id,
    cart_products.amount AS order_qty,
    cart_products.price AS item_price_raw,
    cart_products.currency AS currency_code_raw,
    DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', local_time)) AS order_local_time,
    time_stamp AS order_timestamp
  FROM {{source('glamira_src', 'glamira_raw')}},
  UNNEST(cart_products) AS cart_products
  WHERE collection = 'checkout_success'
),

stg_fact_sales_order__deduplicated AS (
  SELECT DISTINCT * 
  FROM stg_fact_sales_order__rename_col
),

stg_fact_sales_order__clean_price AS (
  SELECT
    * EXCEPT (item_price_raw),
    item_price_raw,
    NULLIF(
      SAFE_CAST(
        CASE
          WHEN REGEXP_CONTAINS(TRIM(item_price_raw), r'٫')
            THEN REGEXP_REPLACE(TRIM(item_price_raw), r'٫', '.')
          WHEN REGEXP_CONTAINS(TRIM(item_price_raw), r"\d'\d")
            THEN REGEXP_REPLACE(TRIM(item_price_raw), r"'", '')
          WHEN REGEXP_CONTAINS(TRIM(item_price_raw), r'\d,\d{2}$')
            THEN REGEXP_REPLACE(REGEXP_REPLACE(TRIM(item_price_raw), r'\.', ''), r',', '.')
          ELSE REGEXP_REPLACE(TRIM(item_price_raw), r',', '')
        END
        AS NUMERIC
      ),
      0
    ) AS item_price
  FROM stg_fact_sales_order__deduplicated
),

stg_fact_sales_order__normalize_currency AS (
  SELECT
    * EXCEPT (currency_code_raw),
    currency_code_raw,
    CASE TRIM(currency_code_raw)
      WHEN '€'       THEN 'EUR'   
      WHEN '£'       THEN 'GBP'   
      WHEN '¥'       THEN 'JPY'   
      WHEN '￥'      THEN 'JPY'   
      WHEN '₺'       THEN 'TRY'   
      WHEN '₹'       THEN 'INR'  
      WHEN '₱'       THEN 'PHP'   
      WHEN '₫'       THEN 'VND'  
      WHEN '₲'       THEN 'PYG'  
      WHEN '₡'       THEN 'CRC'   
      WHEN 'zł'      THEN 'PLN'   
      WHEN 'Ft'      THEN 'HUF'   
      WHEN 'Kč'      THEN 'CZK'   
      WHEN 'kn'      THEN 'HRK'   
      WHEN 'лв.'     THEN 'BGN'   
      WHEN 'лв'      THEN 'BGN'   
      WHEN 'CHF'     THEN 'CHF'  
      WHEN 'CLP'     THEN 'CLP'   
      WHEN 'UYU'     THEN 'UYU'   
      WHEN 'د.ك.‏'   THEN 'KWD'
      WHEN 'kr'      THEN 'SEK'

      -- Prefix patterns: "CODE $" or "CODE symbol"
      WHEN 'SGD $'   THEN 'SGD'
      WHEN 'USD $'   THEN 'USD'
      WHEN 'CAD $'   THEN 'CAD'
      WHEN 'MXN $'   THEN 'MXN'
      WHEN 'AU $'    THEN 'AUD'
      WHEN 'NZD $'   THEN 'NZD'
      WHEN 'HKD $'   THEN 'HKD'
      WHEN 'COP $'   THEN 'COP'
      WHEN 'DOP $'   THEN 'DOP'  
      WHEN 'R$'      THEN 'BRL'
      WHEN 'Lei'     THEN 'RON'   
      WHEN 'BOB Bs'  THEN 'BOB'   
      WHEN 'PEN S/.' THEN 'PEN'   
      WHEN 'CRC ₡'   THEN 'CRC'  
      WHEN 'GTQ Q'   THEN 'GTQ'   
      WHEN 'din.'    THEN 'RSD'   

      -- Generic $ → USD 
      WHEN '$'       THEN 'USD'
      WHEN ''        THEN 'UNKNOWN'
      ELSE NULL  
    END AS currency_code
  FROM stg_fact_sales_order__clean_price
),

stg_fact_sales_order__with_exchange_rate AS (
  SELECT
    s.* EXCEPT (currency_code),
    s.currency_code,
    ROUND(s.item_price * e.rate_to_usd, 2) AS item_price_usd
  FROM stg_fact_sales_order__normalize_currency s
  LEFT JOIN {{ ref('exchange_rates') }} e
    ON s.currency_code = e.currency_code
)

SELECT 
  order_id,
  product_id,
  user_id,
  ip_address,
  user_agent,
  resolution,
  device_id,
  store_id,
  email_address,
  order_local_time,
  order_timestamp,
  currency_code,
  order_qty,
  item_price,
  item_price_usd
FROM stg_fact_sales_order__with_exchange_rate