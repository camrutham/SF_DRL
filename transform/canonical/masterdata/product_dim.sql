-- =====================================================================
-- transform/canonical/masterdata/product_dim.sql
-- Product Dimension SCD Type 2 Transformation (Pure DML)
-- Target Table: {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM
-- Source Table: {DATABASE}.{SCHEMA_RAW}.PRODUCT_RAW
--
-- Load Behavior:
--   - Full Load: When target is empty (or truncated), COALESCE defaults
--     to '1900-01-01', loading and inserting all source records.
--   - Incremental Load: When target has data, filters records where
--     DATE_UPDATED > MAX(target date), updating matched rows and inserting new.
-- =====================================================================

MERGE INTO {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM tgt
USING (
    SELECT
        p.PRODUCT_ID                                AS SRC_SYS_ID,
        p.PRODUCT_ID::VARCHAR                       AS PRODUCT_ID,
        p.PRODUCT_CODE,
        p.PRODUCT_NAME,
        NULL                                        AS PRODUCT_NAME_NORMALIZED,
        p.PRODUCT_CATEGORY,
        p.PRODUCT_SUBCATEGORY,
        p.BRAND,
        p.MANUFACTURER,
        p.UNIT_PRICE,
        p.PRODUCT_STATUS,
        p.DATE_CREATED                              AS SRC_SYS_DATE_CREATED,
        p.DATE_UPDATED                              AS SRC_SYS_DATE_UPDATED,
        CURRENT_DATE()                              AS EFFECTIVE_START_DATE,
        '9999-12-31'::DATE                          AS EFFECTIVE_END_DATE,
        TRUE                                        AS LATEST_FLAG,
        CURRENT_TIMESTAMP()                         AS DATE_CREATED,
        CURRENT_TIMESTAMP()                         AS DATE_UPDATED,
        'sales'                                     AS SOURCE_SYSTEM
    FROM {DATABASE}.{SCHEMA_RAW}.PRODUCT_RAW p
    WHERE p.DATE_UPDATED > COALESCE(
        (SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, DATE_UPDATED)) FROM {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM),
        '1900-01-01'::TIMESTAMP_NTZ
    )
) src
ON tgt.SRC_SYS_ID = src.SRC_SYS_ID
WHEN MATCHED THEN
    UPDATE SET
        tgt.PRODUCT_ID               = src.PRODUCT_ID,
        tgt.PRODUCT_CODE             = src.PRODUCT_CODE,
        tgt.PRODUCT_NAME             = src.PRODUCT_NAME,
        tgt.PRODUCT_NAME_NORMALIZED  = src.PRODUCT_NAME_NORMALIZED,
        tgt.PRODUCT_CATEGORY         = src.PRODUCT_CATEGORY,
        tgt.PRODUCT_SUBCATEGORY      = src.PRODUCT_SUBCATEGORY,
        tgt.BRAND                    = src.BRAND,
        tgt.MANUFACTURER             = src.MANUFACTURER,
        tgt.UNIT_PRICE               = src.UNIT_PRICE,
        tgt.PRODUCT_STATUS           = src.PRODUCT_STATUS,
        tgt.SRC_SYS_DATE_UPDATED     = src.SRC_SYS_DATE_UPDATED,
        tgt.DATE_UPDATED             = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_ID, PRODUCT_ID, PRODUCT_CODE, PRODUCT_NAME,
        PRODUCT_NAME_NORMALIZED, PRODUCT_CATEGORY, PRODUCT_SUBCATEGORY,
        BRAND, MANUFACTURER, UNIT_PRICE, PRODUCT_STATUS,
        SRC_SYS_DATE_CREATED, SRC_SYS_DATE_UPDATED, EFFECTIVE_START_DATE,
        EFFECTIVE_END_DATE, LATEST_FLAG, DATE_CREATED, DATE_UPDATED, SOURCE_SYSTEM
    )
    VALUES (
        src.SRC_SYS_ID, src.PRODUCT_ID, src.PRODUCT_CODE, src.PRODUCT_NAME,
        src.PRODUCT_NAME_NORMALIZED, src.PRODUCT_CATEGORY, src.PRODUCT_SUBCATEGORY,
        src.BRAND, src.MANUFACTURER, src.UNIT_PRICE, src.PRODUCT_STATUS,
        src.SRC_SYS_DATE_CREATED, src.SRC_SYS_DATE_UPDATED, src.EFFECTIVE_START_DATE,
        src.EFFECTIVE_END_DATE, src.LATEST_FLAG, src.DATE_CREATED, src.DATE_UPDATED, src.SOURCE_SYSTEM
    );
