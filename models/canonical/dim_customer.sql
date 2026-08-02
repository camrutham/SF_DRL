{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    incremental_strategy='merge'
) }}

SELECT
    c.CUSTOMER_ID,
    c.CUSTOMER_NUMBER,
    c.CUSTOMER_NAME,
    c.CUSTOMER_TYPE,
    c.INDUSTRY,
    c.ADDRESS,
    c.CITY,
    c.STATE,
    c.COUNTRY,
    c.EMAIL,
    c.PHONE,
    c.DATE_CREATED AS SRC_SYS_DATE_CREATED,
    c.DATE_UPDATED AS SRC_SYS_DATE_UPDATED,
    CURRENT_TIMESTAMP() AS DATE_CREATED,
    CURRENT_TIMESTAMP() AS DATE_UPDATED,
    'sales' AS SOURCE_SYSTEM
FROM {{ source('raw_masterdata', 'CUSTOMER_RAW') }} c

{% if is_incremental() %}
WHERE c.DATE_UPDATED > (SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, DATE_UPDATED)) FROM {{ this }})
   OR c.DATE_UPDATED > (SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, DATE_UPDATED)) FROM {{ this }})
{% endif %}
