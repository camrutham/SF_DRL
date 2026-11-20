-- =====================================================================
-- transform/canonical/masterdata/product_dim.sql
-- Product Dimension SCD Type 2 Transformation (Stream CDC DML)
-- Target Table: {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM
-- Source Stream: {DATABASE}.{SCHEMA_RAW}.PRODUCT_RAW_STREAM
--
-- Load Behavior (SCD Type 2):
--   - Incremental Load:
--       1. Detects changed records against active rows (LATEST_FLAG = TRUE).
--       2. Expires old version: sets LATEST_FLAG = FALSE and
--          EFFECTIVE_END_DATE = CURRENT_DATE().
--       3. Inserts new active version with LATEST_FLAG = TRUE,
--          EFFECTIVE_START_DATE = CURRENT_DATE(), EFFECTIVE_END_DATE = '9999-12-31'.
--       4. Inserts brand new products with LATEST_FLAG = TRUE.
--   - Full Refresh: Truncates target table, queries base table, and resets
--     the stream offset.
-- =====================================================================

MERGE INTO {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM tgt
USING (
    WITH raw_stream AS (
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
            p.DATE_CREATED,
            p.DATE_UPDATED
        FROM {DATABASE}.{SCHEMA_RAW}.PRODUCT_RAW_STREAM p
        WHERE p.METADATA$ACTION = 'INSERT'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY p.PRODUCT_ID ORDER BY p.DATE_UPDATED DESC) = 1
    ),
    changed_records AS (
        SELECT
            s.*
        FROM raw_stream s
        JOIN {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM cur
            ON s.PRODUCT_ID = cur.PRODUCT_ID
           AND cur.LATEST_FLAG = TRUE
        WHERE (
            s.PRODUCT_CODE          IS DISTINCT FROM cur.PRODUCT_CODE OR
            s.PRODUCT_NAME          IS DISTINCT FROM cur.PRODUCT_NAME OR
            s.PRODUCT_CATEGORY      IS DISTINCT FROM cur.PRODUCT_CATEGORY OR
            s.PRODUCT_SUBCATEGORY   IS DISTINCT FROM cur.PRODUCT_SUBCATEGORY OR
            s.BRAND                 IS DISTINCT FROM cur.BRAND OR
            s.MANUFACTURER          IS DISTINCT FROM cur.MANUFACTURER OR
            s.UNIT_PRICE            IS DISTINCT FROM cur.UNIT_PRICE OR
            s.PRODUCT_STATUS        IS DISTINCT FROM cur.PRODUCT_STATUS
        )
    ),
    staged_records AS (
        -- 1. Expire existing active record
        SELECT
            c.PRODUCT_ID AS MERGE_KEY,
            c.*
        FROM changed_records c

        UNION ALL

        -- 2. Insert new active record (brand new records OR newly changed records)
        SELECT
            NULL AS MERGE_KEY,
            s.*
        FROM raw_stream s
        LEFT JOIN {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM cur
            ON s.PRODUCT_ID = cur.PRODUCT_ID
           AND cur.LATEST_FLAG = TRUE
        WHERE cur.PRODUCT_ID IS NULL
           OR s.PRODUCT_ID IN (SELECT PRODUCT_ID FROM changed_records)
    )
    SELECT
        MERGE_KEY,
        PRODUCT_ID                                  AS SRC_SYS_ID,
        PRODUCT_ID::VARCHAR                         AS PRODUCT_ID,
        PRODUCT_CODE,
        PRODUCT_NAME,
        NULL                                        AS PRODUCT_NAME_NORMALIZED,
        PRODUCT_CATEGORY,
        PRODUCT_SUBCATEGORY,
        BRAND,
        MANUFACTURER,
        UNIT_PRICE,
        PRODUCT_STATUS,
        DATE_CREATED                                AS SRC_SYS_DATE_CREATED,
        DATE_UPDATED                                AS SRC_SYS_DATE_UPDATED,
        CURRENT_DATE()                              AS EFFECTIVE_START_DATE,
        '9999-12-31'::DATE                          AS EFFECTIVE_END_DATE,
        TRUE                                        AS LATEST_FLAG,
        CURRENT_TIMESTAMP()                         AS DATE_CREATED,
        CURRENT_TIMESTAMP()                         AS DATE_UPDATED,
        'sales'                                     AS SOURCE_SYSTEM
    FROM staged_records
) src
ON tgt.PRODUCT_ID = src.MERGE_KEY
   AND tgt.LATEST_FLAG = TRUE
WHEN MATCHED THEN
    -- Expire previous version
    UPDATE SET
        tgt.LATEST_FLAG        = FALSE,
        tgt.EFFECTIVE_END_DATE = CURRENT_DATE(),
        tgt.DATE_UPDATED       = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    -- Insert new active version
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
        src.EFFECTIVE_END_DATE, src.LATEST_FLAG, src.DATE_CREATED, src.DATE_UPDATED,
        src.SOURCE_SYSTEM
    );
