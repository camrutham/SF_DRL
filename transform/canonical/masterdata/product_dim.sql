-- =====================================================================
-- transform/canonical/masterdata/product_dim.sql
-- Product Dimension SCD Type 2 Transformation (Stream CDC DML)
-- Target Table: {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM
-- Source Stream: {DATABASE}.{SCHEMA_RAW}.PRODUCT_RAW_STREAM
--
-- Load Behavior:
--   - Incremental Load (Stream CDC): Consumes only inserted/updated delta
--     records from PRODUCT_RAW_STREAM (METADATA$ACTION = 'INSERT').
--   - Full Refresh: Truncates target table, queries base table, and resets
--     the stream offset.
-- =====================================================================

MERGE INTO {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM tgt
USING (
    SELECT
        p.PRODUCT_ID                                AS SRC_SYS_ID,
        p.PRODUCT_ID::VARCHAR                       AS PRODUCT_ID,
        p.PRODUCT_NUMBER,
        p.PRODUCT_NAME,
        NULL                                        AS PRODUCT_NAME_NORMALIZED,
        p.PRODUCT_CATEGORY,
        p.PRODUCT_SUBCATEGORY,
        p.BRAND,
        p.UNIT_PRICE,
        p.UNIT_COST,
        p.PRODUCT_STATUS,
        p.DATE_CREATED                              AS SRC_SYS_DATE_CREATED,
        p.DATE_UPDATED                              AS SRC_SYS_DATE_UPDATED,
        CURRENT_DATE()                              AS EFFECTIVE_START_DATE,
        '9999-12-31'::DATE                          AS EFFECTIVE_END_DATE,
        TRUE                                        AS LATEST_FLAG,
        CURRENT_TIMESTAMP()                         AS DATE_CREATED,
        CURRENT_TIMESTAMP()                         AS DATE_UPDATED,
        'sales'                                     AS SOURCE_SYSTEM
    FROM {DATABASE}.{SCHEMA_RAW}.PRODUCT_RAW_STREAM p
    WHERE p.METADATA$ACTION = 'INSERT'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.PRODUCT_ID ORDER BY p.DATE_UPDATED DESC) = 1
) src
ON tgt.SRC_SYS_ID = src.SRC_SYS_ID
WHEN MATCHED THEN
    UPDATE SET
        tgt.PRODUCT_ID              = src.PRODUCT_ID,
        tgt.PRODUCT_NUMBER          = src.PRODUCT_NUMBER,
        tgt.PRODUCT_NAME            = src.PRODUCT_NAME,
        tgt.PRODUCT_NAME_NORMALIZED = src.PRODUCT_NAME_NORMALIZED,
        tgt.PRODUCT_CATEGORY        = src.PRODUCT_CATEGORY,
        tgt.PRODUCT_SUBCATEGORY     = src.PRODUCT_SUBCATEGORY,
        tgt.BRAND                   = src.BRAND,
        tgt.UNIT_PRICE              = src.UNIT_PRICE,
        tgt.UNIT_COST               = src.UNIT_COST,
        tgt.PRODUCT_STATUS          = src.PRODUCT_STATUS,
        tgt.SRC_SYS_DATE_UPDATED     = src.SRC_SYS_DATE_UPDATED,
        tgt.DATE_UPDATED             = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_ID, PRODUCT_ID, PRODUCT_NUMBER, PRODUCT_NAME,
        PRODUCT_NAME_NORMALIZED, PRODUCT_CATEGORY, PRODUCT_SUBCATEGORY,
        BRAND, UNIT_PRICE, UNIT_COST, PRODUCT_STATUS, SRC_SYS_DATE_CREATED,
        SRC_SYS_DATE_UPDATED, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE,
        LATEST_FLAG, DATE_CREATED, DATE_UPDATED, SOURCE_SYSTEM
    )
    VALUES (
        src.SRC_SYS_ID, src.PRODUCT_ID, src.PRODUCT_NUMBER, src.PRODUCT_NAME,
        src.PRODUCT_NAME_NORMALIZED, src.PRODUCT_CATEGORY, src.PRODUCT_SUBCATEGORY,
        src.BRAND, src.UNIT_PRICE, src.UNIT_COST, src.PRODUCT_STATUS,
        src.SRC_SYS_DATE_CREATED, src.SRC_SYS_DATE_UPDATED, src.EFFECTIVE_START_DATE,
        src.EFFECTIVE_END_DATE, src.LATEST_FLAG, src.DATE_CREATED, src.DATE_UPDATED,
        src.SOURCE_SYSTEM
    );
