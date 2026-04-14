WITH stg_fact_sales_order__rename_col AS (
  SELECT
    ip AS ip_address,
    user_agent,
    resolution,
    -- Nếu user_id NULL → dùng device_id làm đại diện
    COALESCE(NULLIF(TRIM(user_id_db), ''), CONCAT('guest_', device_id)) AS user_id, 
    device_id,
    store_id,
    order_id,
    COALESCE(NULLIF(TRIM(email_address), ''), 'Unknown') AS email_address,
    SAFE_CAST(cart_products.product_id AS INT) AS product_id,
    cart_products.amount AS order_qty,
    cart_products.price AS item_price_raw,
    cart_products.currency AS currency_code_raw,
    REGEXP_EXTRACT(current_url,r'https?://(?:www\.)?([^/]+)') AS domain, 
    DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', local_time)) AS order_local_time,
    time_stamp AS order_timestamp
  FROM {{source('glamira_src', 'glamira_raw')}},
  UNNEST(cart_products) AS cart_products
  WHERE collection = 'checkout_success'
),

stg_fact_sales_order__deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY
          CASE
            WHEN TRIM(item_price_raw) NOT IN ('0,00', '0.00') AND TRIM(item_price_raw) != '' THEN 0 ELSE 1 END ASC) 
            AS rn
    FROM stg_fact_sales_order__rename_col
),

stg_fact_sales_order__clean_price AS (
  SELECT
    * EXCEPT (item_price_raw, rn),
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
      ),0) AS item_price, 
    CASE
      WHEN TRIM(item_price_raw) IN ('0,00', '0.00', '') THEN 'zero_price' ELSE 'valid'
      END AS item_price_status
  FROM stg_fact_sales_order__deduplicated
  WHERE rn = 1 
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
      ELSE NULL  
    END AS currency_code_from_symbol
  FROM stg_fact_sales_order__clean_price
),

stg_fact_sales_order__currency_resolved AS (
    SELECT
        f.* EXCEPT (currency_code_from_symbol),
        COALESCE(f.currency_code_from_symbol, er.currency_code, 'USD') AS currency_code,
        CASE
            WHEN f.currency_code_from_symbol IS NOT NULL THEN 'from_symbol'
            WHEN er.currency_code IS NOT NULL THEN 'from_domain'
            ELSE 'default_usd'
        END AS currency_source

    FROM stg_fact_sales_order__normalize_currency f
    LEFT JOIN {{ ref('exchange_rates') }} er
        ON f.domain = er.domain
),

stg_fact_sales_order__exchange_rate AS (
  SELECT
    f.*,
    SAFE_CAST(ROUND(f.item_price * e.rate_to_usd, 2) AS NUMERIC) AS item_price_usd
  FROM stg_fact_sales_order__currency_resolved f
  LEFT JOIN {{ ref('exchange_rates') }} e
    ON f.currency_code = e.currency_code
)

SELECT *
FROM stg_fact_sales_order__exchange_rate