-- =====================================================================
-- transform/canonical/masterdata/contract_dim.sql
-- Contract Dimension SCD Type 2 Transformation (Stream CDC DML)
-- Target Table: {DATABASE}.{SCHEMA_CANONICAL}.CONTRACT_DIM
-- Source Stream: {DATABASE}.{SCHEMA_RAW}.CONTRACT_RAW_STREAM
--
-- Load Behavior (SCD Type 2):
--   - Incremental Load:
--       1. Detects changed records against active rows (LATEST_FLAG = TRUE).
--       2. Expires old version: sets LATEST_FLAG = FALSE and
--          EFFECTIVE_END_DATE = CURRENT_DATE().
--       3. Inserts new active version with LATEST_FLAG = TRUE,
--          EFFECTIVE_START_DATE = CURRENT_DATE(), EFFECTIVE_END_DATE = '9999-12-31'.
--       4. Inserts brand new contracts with LATEST_FLAG = TRUE.
--   - Full Refresh: Truncates target table, queries base table, and resets
--     the stream offset.
-- =====================================================================

MERGE INTO {DATABASE}.{SCHEMA_CANONICAL}.CONTRACT_DIM tgt
USING (
    WITH raw_stream AS (
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
            c.DATE_CREATED,
            c.DATE_UPDATED
        FROM {DATABASE}.{SCHEMA_RAW}.CONTRACT_RAW_STREAM c
        WHERE c.METADATA$ACTION = 'INSERT'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY c.CONTRACT_ID ORDER BY c.DATE_UPDATED DESC) = 1
    ),
    changed_records AS (
        SELECT
            s.*
        FROM raw_stream s
        JOIN {DATABASE}.{SCHEMA_CANONICAL}.CONTRACT_DIM cur
            ON s.CONTRACT_ID = cur.CONTRACT_ID
           AND cur.LATEST_FLAG = TRUE
        WHERE (
            s.CONTRACT_NUMBER       IS DISTINCT FROM cur.CONTRACT_NUMBER OR
            s.CONTRACT_NAME         IS DISTINCT FROM cur.CONTRACT_NAME OR
            s.CUSTOMER_ID           IS DISTINCT FROM cur.CUSTOMER_ID OR
            s.CONTRACT_TYPE         IS DISTINCT FROM cur.CONTRACT_TYPE OR
            s.START_DATE            IS DISTINCT FROM cur.START_DATE OR
            s.END_DATE              IS DISTINCT FROM cur.END_DATE OR
            s.CONTRACT_VALUE        IS DISTINCT FROM cur.CONTRACT_VALUE OR
            s.CONTRACT_STATUS       IS DISTINCT FROM cur.CONTRACT_STATUS OR
            s.SALES_REGION          IS DISTINCT FROM cur.SALES_REGION OR
            s.ACCOUNT_MANAGER       IS DISTINCT FROM cur.ACCOUNT_MANAGER
        )
    ),
    staged_records AS (
        -- 1. Expire existing active record
        SELECT
            c.CONTRACT_ID AS MERGE_KEY,
            c.*
        FROM changed_records c

        UNION ALL

        -- 2. Insert new active record (brand new records OR newly changed records)
        SELECT
            NULL AS MERGE_KEY,
            s.*
        FROM raw_stream s
        LEFT JOIN {DATABASE}.{SCHEMA_CANONICAL}.CONTRACT_DIM cur
            ON s.CONTRACT_ID = cur.CONTRACT_ID
           AND cur.LATEST_FLAG = TRUE
        WHERE cur.CONTRACT_ID IS NULL
           OR s.CONTRACT_ID IN (SELECT CONTRACT_ID FROM changed_records)
    )
    SELECT
        MERGE_KEY,
        CONTRACT_ID                                 AS SRC_SYS_ID,
        CONTRACT_ID::VARCHAR                        AS CONTRACT_ID,
        CONTRACT_NUMBER,
        CONTRACT_NAME,
        NULL                                        AS CONTRACT_NAME_NORMALIZED,
        CUSTOMER_ID,
        CONTRACT_TYPE,
        START_DATE,
        END_DATE,
        CONTRACT_VALUE,
        CONTRACT_STATUS,
        SALES_REGION,
        ACCOUNT_MANAGER,
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
ON tgt.CONTRACT_ID = src.MERGE_KEY
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
        SRC_SYS_ID, CONTRACT_ID, CONTRACT_NUMBER, CONTRACT_NAME,
        CONTRACT_NAME_NORMALIZED, CUSTOMER_ID, CONTRACT_TYPE,
        START_DATE, END_DATE, CONTRACT_VALUE, CONTRACT_STATUS,
        SALES_REGION, ACCOUNT_MANAGER, SRC_SYS_DATE_CREATED,
        SRC_SYS_DATE_UPDATED, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE,
        LATEST_FLAG, DATE_CREATED, DATE_UPDATED, SOURCE_SYSTEM
    )
    VALUES (
        src.SRC_SYS_ID, src.CONTRACT_ID, src.CONTRACT_NUMBER, src.CONTRACT_NAME,
        src.CONTRACT_NAME_NORMALIZED, src.CUSTOMER_ID, src.CONTRACT_TYPE,
        src.START_DATE, src.END_DATE, src.CONTRACT_VALUE, src.CONTRACT_STATUS,
        src.SALES_REGION, src.ACCOUNT_MANAGER, src.SRC_SYS_DATE_CREATED,
        src.SRC_SYS_DATE_UPDATED, src.EFFECTIVE_START_DATE, src.EFFECTIVE_END_DATE,
        src.LATEST_FLAG, src.DATE_CREATED, src.DATE_UPDATED, src.SOURCE_SYSTEM
    );
