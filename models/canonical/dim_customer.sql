{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    incremental_strategy='merge',
    alias='CUSTOMER_DIM'
) }}

SELECT
    -- Surrogate / business keys
    CAST(c.CUSTOMER_ID          AS NUMBER(38,0))    AS SRC_SYS_ID,
    CAST(c.CUSTOMER_ID          AS VARCHAR)         AS CUSTOMER_ID,
    CAST(c.CUSTOMER_NUMBER      AS VARCHAR)         AS CUSTOMER_NUMBER,
    CAST(c.CUSTOMER_NAME        AS VARCHAR)         AS CUSTOMER_NAME,
    CAST(NULL                   AS VARCHAR)         AS CUSTOMER_NAME_NORMALIZED,
    CAST(c.CUSTOMER_TYPE        AS VARCHAR)         AS CUSTOMER_TYPE,
    CAST(c.INDUSTRY             AS VARCHAR)         AS INDUSTRY,
    CAST(c.ADDRESS              AS VARCHAR)         AS ADDRESS,
    CAST(c.CITY                 AS VARCHAR)         AS CITY,
    CAST(c.STATE                AS VARCHAR)         AS STATE,
    CAST(c.COUNTRY              AS VARCHAR)         AS COUNTRY,
    CAST(c.EMAIL                AS VARCHAR)         AS EMAIL,
    CAST(c.PHONE                AS VARCHAR)         AS PHONE,

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

FROM {{ source('raw_masterdata', 'CUSTOMER_RAW') }} c

{% if is_incremental() %}
WHERE c.DATE_UPDATED > COALESCE(
    (SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, DATE_UPDATED)) FROM {{ this }}),
    '1900-01-01'::TIMESTAMP_NTZ
)
{% endif %}
