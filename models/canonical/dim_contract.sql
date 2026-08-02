{{ config(
    materialized='incremental',
    unique_key='CONTRACT_ID',
    incremental_strategy='merge',
    alias='CONTRACT_DIM'
) }}

SELECT
    -- Surrogate / business keys
    CAST(c.CONTRACT_ID          AS NUMBER(38,0))    AS SRC_SYS_ID,
    CAST(c.CONTRACT_ID          AS VARCHAR)         AS CONTRACT_ID,
    CAST(c.CONTRACT_NUMBER      AS VARCHAR)         AS CONTRACT_NUMBER,
    CAST(c.CONTRACT_NAME        AS VARCHAR)         AS CONTRACT_NAME,
    CAST(NULL                   AS VARCHAR)         AS CONTRACT_NAME_NORMALIZED,
    CAST(c.CUSTOMER_ID          AS NUMBER(38,0))    AS CUSTOMER_ID,
    CAST(c.CONTRACT_TYPE        AS VARCHAR)         AS CONTRACT_TYPE,
    CAST(c.START_DATE           AS DATE)            AS START_DATE,
    CAST(c.END_DATE             AS DATE)            AS END_DATE,
    CAST(c.CONTRACT_VALUE       AS NUMBER(18,2))    AS CONTRACT_VALUE,
    CAST(c.CONTRACT_STATUS      AS VARCHAR)         AS CONTRACT_STATUS,
    CAST(c.SALES_REGION         AS VARCHAR)         AS SALES_REGION,
    CAST(c.ACCOUNT_MANAGER      AS VARCHAR)         AS ACCOUNT_MANAGER,

    -- Source audit
    CAST(c.DATE_CREATED         AS TIMESTAMP_NTZ)   AS SRC_SYS_DATE_CREATED,
    CAST(c.DATE_UPDATED         AS TIMESTAMP_NTZ)   AS SRC_SYS_DATE_UPDATED,

    -- SCD Type 2 fields (set on initial load; updated by merge on change)
    CAST(CURRENT_DATE()         AS DATE)            AS EFFECTIVE_START_DATE,
    CAST('9999-12-31'           AS DATE)            AS EFFECTIVE_END_DATE,
    TRUE                                            AS LATEST_FLAG,

    -- ETL audit
    CAST(CURRENT_TIMESTAMP()    AS TIMESTAMP_NTZ)   AS DATE_CREATED,
    CAST(CURRENT_TIMESTAMP()    AS TIMESTAMP_NTZ)   AS DATE_UPDATED,
    CAST('sales'                AS VARCHAR)         AS SOURCE_SYSTEM

FROM {{ source('raw_masterdata', 'CONTRACT_RAW') }} c

{% if is_incremental() %}
WHERE c.DATE_UPDATED > COALESCE(
    (SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, DATE_UPDATED)) FROM {{ this }}),
    '1900-01-01'::TIMESTAMP_NTZ
)
{% endif %}
