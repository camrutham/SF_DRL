{{ config(
    materialized='incremental',
    unique_key='CONTRACT_ID',
    incremental_strategy='merge'
) }}

SELECT
    c.CONTRACT_ID,
    c.CONTRACT_NUMBER,
    c.CONTRACT_NAME,
    c.CUSTOMER_ID,
    c.CONTRACT_TYPE,
    c.START_DATE,
    c.END_DATE,
    c.CONTRACT_VALUE,
    c.CONTRACT_STATUS,
    c.SALES_REGION,
    c.ACCOUNT_MANAGER,
    c.DATE_CREATED AS SRC_SYS_DATE_CREATED,
    c.DATE_UPDATED AS SRC_SYS_DATE_UPDATED,
    CURRENT_TIMESTAMP() AS DATE_CREATED,
    CURRENT_TIMESTAMP() AS DATE_UPDATED,
    'sales' AS SOURCE_SYSTEM
FROM {{ source('raw_masterdata', 'CONTRACT_RAW') }} c

{% if is_incremental() %}
WHERE c.DATE_UPDATED > (SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, DATE_UPDATED)) FROM {{ this }})
   OR c.DATE_UPDATED > (SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, DATE_UPDATED)) FROM {{ this }})
{% endif %}
