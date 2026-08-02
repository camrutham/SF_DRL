{{ config(
    materialized='incremental',
    unique_key='PRODUCT_ID',
    incremental_strategy='merge',
    alias='PRODUCT_DIM'
) }}

SELECT
    -- Surrogate / business keys
    CAST(p.PRODUCT_ID           AS NUMBER(38,0))    AS SRC_SYS_ID,
    CAST(p.PRODUCT_ID           AS VARCHAR)         AS PRODUCT_ID,
    CAST(p.PRODUCT_CODE         AS VARCHAR)         AS PRODUCT_CODE,
    CAST(p.PRODUCT_NAME         AS VARCHAR)         AS PRODUCT_NAME,
    CAST(NULL                   AS VARCHAR)         AS PRODUCT_NAME_NORMALIZED,
    CAST(p.PRODUCT_CATEGORY     AS VARCHAR)         AS PRODUCT_CATEGORY,
    CAST(p.PRODUCT_SUBCATEGORY  AS VARCHAR)         AS PRODUCT_SUBCATEGORY,
    CAST(p.BRAND                AS VARCHAR)         AS BRAND,
    CAST(p.MANUFACTURER         AS VARCHAR)         AS MANUFACTURER,
    CAST(p.UNIT_PRICE           AS NUMBER(18,2))    AS UNIT_PRICE,
    CAST(p.PRODUCT_STATUS       AS VARCHAR)         AS PRODUCT_STATUS,

    -- Source audit
    CAST(p.DATE_CREATED         AS TIMESTAMP_NTZ)   AS SRC_SYS_DATE_CREATED,
    CAST(p.DATE_UPDATED         AS TIMESTAMP_NTZ)   AS SRC_SYS_DATE_UPDATED,

    -- SCD Type 2 fields (set on initial load; updated by merge on change)
    CAST(CURRENT_DATE()         AS DATE)            AS EFFECTIVE_START_DATE,
    CAST('9999-12-31'           AS DATE)            AS EFFECTIVE_END_DATE,
    TRUE                                            AS LATEST_FLAG,

    -- ETL audit
    CAST(CURRENT_TIMESTAMP()    AS TIMESTAMP_NTZ)   AS DATE_CREATED,
    CAST(CURRENT_TIMESTAMP()    AS TIMESTAMP_NTZ)   AS DATE_UPDATED,
    CAST('sales'                AS VARCHAR)         AS SOURCE_SYSTEM

FROM {{ source('raw_masterdata', 'PRODUCT_RAW') }} p

{% if is_incremental() %}
WHERE p.DATE_UPDATED > COALESCE(
    (SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, DATE_UPDATED)) FROM {{ this }}),
    '1900-01-01'::TIMESTAMP_NTZ
)
{% endif %}
