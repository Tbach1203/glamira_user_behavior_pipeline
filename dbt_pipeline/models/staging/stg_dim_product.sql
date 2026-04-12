WITH stg_dim_product__rename_col AS (
  SELECT
    product_id,
    name AS product_name,
    SAFE_CAST(category AS INT) AS category_id,
    category_name,
    SAFE_CAST(price AS NUMERIC) AS unit_price,
    SAFE_CAST(min_price AS NUMERIC) AS min_price,
    SAFE_CAST(max_price AS NUMERIC) AS max_price,
    qty AS product_qty,
    min_price_format
FROM {{source('glamira_src', 'product_info')}}
),

stg_dim_product__deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY product_id) AS rn
    FROM stg_dim_product__rename_col
),

stg_dim_product__nomarlize_currency AS (
  SELECT
    * EXCEPT(rn),
    TRIM(UPPER(REGEXP_REPLACE(min_price_format, r'[\d,\.\s\'\'/]', ''))) AS raw_symbol
  FROM stg_dim_product__deduplicated
  WHERE rn = 1
),

stg_dim_product__mapped_currency AS (
  SELECT
    *,
    CASE raw_symbol
      WHEN 'USD'   THEN 'USD'
      WHEN 'MXN'   THEN 'MXN'
      WHEN 'CLP'   THEN 'CLP'
      WHEN 'AED'   THEN 'AED'
      WHEN 'AZN'   THEN 'AZN'
      WHEN 'BOB'   THEN 'BOB'
      WHEN 'CHF'   THEN 'CHF'
      WHEN 'ZAR'   THEN 'ZAR'
      WHEN 'RON'   THEN 'RON'
      WHEN 'COP$'  THEN 'COP' 

      WHEN '€'     THEN 'EUR'
      WHEN '£'     THEN 'GBP'
      WHEN '₫'     THEN 'VND'
      WHEN '₱'     THEN 'PHP'
      WHEN '₲'     THEN 'PYG'
      WHEN '₡'     THEN 'CRC'
      WHEN '฿'     THEN 'THB'
      WHEN 'R$'    THEN 'BRL'
      WHEN 'RM'    THEN 'MYR'
      WHEN 'AU$'   THEN 'AUD'
      WHEN 'CA$'   THEN 'CAD'
      WHEN 'SG$'   THEN 'SGD'
      WHEN 'RD$'   THEN 'DOP'
      WHEN '$U'    THEN 'UYU'
      WHEN 'COP'   THEN 'COP'

      WHEN 'LEI'   THEN 'RON'
      WHEN 'ZL'    THEN 'PLN'  
      WHEN 'KC'    THEN 'CZK' 
      WHEN 'FT'    THEN 'HUF'
      WHEN 'DIN'   THEN 'RSD'
      WHEN 'Q'     THEN 'GTQ'
      WHEN 'RP'    THEN 'IDR'
      WHEN 'PENS'  THEN 'PEN'
      WHEN 'LEKE'  THEN 'ALL' 

      WHEN 'KR'    THEN NULL   
      WHEN '¥'     THEN NULL   
      WHEN 'L'     THEN NULL   

      ELSE 'UNKNOWN'
    END AS currency_code
  FROM stg_dim_product__nomarlize_currency
),

stg_dim_product__final AS (
  SELECT
    * EXCEPT(currency_code),
    CASE
      WHEN currency_code IS NULL AND raw_symbol = 'KR'          THEN 'SEK' 
      WHEN currency_code IS NULL AND raw_symbol IN ('¥', '￥')  THEN 'JPY' 
      WHEN currency_code IS NULL AND raw_symbol = 'L'           THEN 'UNKNOWN'
      ELSE currency_code
    END AS currency_code
  FROM stg_dim_product__mapped_currency
)

SELECT * FROM stg_dim_product__final

