{{ config(
    materialized='incremental',
    unique_key='PRODUCT_ID',
    incremental_strategy='merge'
) }}

SELECT
    p.PRODUCT_ID,
    p.PRODUCT_CODE,
    p.PRODUCT_NAME,
    p.PRODUCT_CATEGORY,
    p.PRODUCT_SUBCATEGORY,
    p.BRAND,
    p.MANUFACTURER,
    p.UNIT_PRICE,
    p.PRODUCT_STATUS,
    p.DATE_CREATED AS SRC_SYS_DATE_CREATED,
    p.DATE_UPDATED AS SRC_SYS_DATE_UPDATED,
    CURRENT_TIMESTAMP() AS DATE_CREATED,
    CURRENT_TIMESTAMP() AS DATE_UPDATED,
    'sales' AS SOURCE_SYSTEM
FROM {{ source('raw_masterdata', 'PRODUCT_RAW') }} p

{% if is_incremental() %}
WHERE p.DATE_UPDATED > (SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, DATE_UPDATED)) FROM {{ this }})
   OR p.DATE_UPDATED > (SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, DATE_UPDATED)) FROM {{ this }})
{% endif %}
