-- =====================================================================
-- transform/canonical/masterdata/product_dim.sql
-- Product Dimension SCD Type 2 Transformation (Stream CDC & Historical Batch DML)
-- Target Table: {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM
-- Source Stream: {DATABASE}.{SCHEMA_RAW}.PRODUCT_RAW_STREAM
--
-- Load Behavior (SCD Type 2):
--   - Full Load / Historical Batch:
--       Uses window functions (ROW_NUMBER & LEAD) over PRODUCT_CODE:
--       * Latest record (version_rank = 1) -> LATEST_FLAG = TRUE, EFFECTIVE_END_DATE = '9999-12-31'
--       * Historical records (version_rank > 1) -> LATEST_FLAG = FALSE,
--         EFFECTIVE_END_DATE = COALESCE(next_start - 1, DATE_UPDATED)
--   - Incremental Load (Stream CDC):
--       1. Detects changed records against active rows in target (LATEST_FLAG = TRUE).
--       2. Expires previous version: sets LATEST_FLAG = FALSE and
--          EFFECTIVE_END_DATE = CURRENT_DATE().
--       3. Inserts new active version with LATEST_FLAG = TRUE,
--          EFFECTIVE_START_DATE = CURRENT_DATE(), EFFECTIVE_END_DATE = '9999-12-31'.
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
            p.DATE_UPDATED,
            -- Determine version order across multiple records for the same product code
            ROW_NUMBER() OVER (
                PARTITION BY p.PRODUCT_CODE
                ORDER BY p.DATE_UPDATED DESC, p.DATE_CREATED DESC
            ) AS version_rank,
            -- Look ahead to next version start date to calculate effective end date for historical rows
            LEAD(p.DATE_CREATED::DATE) OVER (
                PARTITION BY p.PRODUCT_CODE
                ORDER BY p.DATE_UPDATED ASC, p.DATE_CREATED ASC
            ) AS next_version_start_date
        FROM {DATABASE}.{SCHEMA_RAW}.PRODUCT_RAW_STREAM p
        WHERE p.METADATA$ACTION = 'INSERT'
    ),
    -- For incremental runs: check if incoming active version represents a change against target active version
    changed_records AS (
        SELECT
            s.*
        FROM raw_stream s
        JOIN {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM cur
            ON s.PRODUCT_CODE = cur.PRODUCT_CODE
           AND cur.LATEST_FLAG = TRUE
        WHERE s.version_rank = 1
          AND (
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
        -- 1. Expire existing active record in target (Incremental only)
        SELECT
            c.PRODUCT_CODE AS MERGE_KEY,
            c.PRODUCT_ID,
            c.PRODUCT_CODE,
            c.PRODUCT_NAME,
            c.PRODUCT_CATEGORY,
            c.PRODUCT_SUBCATEGORY,
            c.BRAND,
            c.MANUFACTURER,
            c.UNIT_PRICE,
            c.PRODUCT_STATUS,
            c.DATE_CREATED,
            c.DATE_UPDATED,
            c.DATE_CREATED::DATE AS CALC_START_DATE,
            CURRENT_DATE() AS CALC_END_DATE,
            FALSE AS CALC_LATEST_FLAG
        FROM changed_records c

        UNION ALL

        -- 2. Insert records:
        -- Full Load: Inserts all historical records (LATEST_FLAG = FALSE) + latest record (LATEST_FLAG = TRUE)
        -- Incremental: Inserts brand new products OR newly updated active version
        SELECT
            NULL AS MERGE_KEY,
            s.PRODUCT_ID,
            s.PRODUCT_CODE,
            s.PRODUCT_NAME,
            s.PRODUCT_CATEGORY,
            s.PRODUCT_SUBCATEGORY,
            s.BRAND,
            s.MANUFACTURER,
            s.UNIT_PRICE,
            s.PRODUCT_STATUS,
            s.DATE_CREATED,
            s.DATE_UPDATED,
            -- Effective Start Date
            s.DATE_CREATED::DATE AS CALC_START_DATE,
            -- Effective End Date
            CASE
                WHEN s.version_rank = 1 THEN '9999-12-31'::DATE
                ELSE COALESCE(s.next_version_start_date - 1, s.DATE_UPDATED::DATE)
            END AS CALC_END_DATE,
            -- Latest Flag
            CASE
                WHEN s.version_rank = 1 THEN TRUE
                ELSE FALSE
            END AS CALC_LATEST_FLAG
        FROM raw_stream s
        LEFT JOIN {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM cur
            ON s.PRODUCT_CODE = cur.PRODUCT_CODE
           AND cur.LATEST_FLAG = TRUE
        WHERE cur.PRODUCT_CODE IS NULL -- Brand new product or Full Load (target is empty)
           OR s.version_rank > 1        -- Historical records in batch (always insert as history)
           OR s.PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM changed_records) -- Updated active record
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
        CALC_START_DATE                             AS EFFECTIVE_START_DATE,
        CALC_END_DATE                               AS EFFECTIVE_END_DATE,
        CALC_LATEST_FLAG                            AS LATEST_FLAG,
        CURRENT_TIMESTAMP()                         AS DATE_CREATED,
        CURRENT_TIMESTAMP()                         AS DATE_UPDATED,
        'sales'                                     AS SOURCE_SYSTEM
    FROM staged_records
) src
ON tgt.PRODUCT_CODE = src.MERGE_KEY
   AND tgt.LATEST_FLAG = TRUE
WHEN MATCHED THEN
    -- Expire previous version
    UPDATE SET
        tgt.LATEST_FLAG        = FALSE,
        tgt.EFFECTIVE_END_DATE = CURRENT_DATE(),
        tgt.DATE_UPDATED       = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    -- Insert new record (historical or active)
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
