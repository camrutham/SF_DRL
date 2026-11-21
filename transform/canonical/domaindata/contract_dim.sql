-- =====================================================================
-- transform/canonical/domaindata/contract_dim.sql
-- Contract Dimension SCD Type 2 Transformation (Stream CDC & Historical Batch DML)
-- Target Table: {DATABASE}.{SCHEMA_CANONICAL}.CONTRACT_DIM
-- Source Stream: {DATABASE}.{SCHEMA_RAW}.CONTRACT_RAW_STREAM
--
-- Load Behavior (SCD Type 2):
--   - Full Load / Historical Batch:
--       Uses window functions (ROW_NUMBER & LEAD) over CONTRACT_NUMBER:
--       * Latest record (version_rank = 1) -> LATEST_FLAG = TRUE, EFFECTIVE_END_DATE = '9999-12-31'
--       * Historical records (version_rank > 1) -> LATEST_FLAG = FALSE,
--         EFFECTIVE_END_DATE = COALESCE(END_DATE, next_start - 1, DATE_UPDATED)
--   - Incremental Load (Stream CDC):
--       1. Detects changed records against active rows in target (LATEST_FLAG = TRUE).
--       2. Expires previous version: sets LATEST_FLAG = FALSE and
--          EFFECTIVE_END_DATE = CURRENT_DATE().
--       3. Inserts new active version with LATEST_FLAG = TRUE,
--          EFFECTIVE_START_DATE = CURRENT_DATE(), EFFECTIVE_END_DATE = '9999-12-31'.
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
            c.DATE_UPDATED,
            -- Determine version order across multiple records for the same contract
            ROW_NUMBER() OVER (
                PARTITION BY c.CONTRACT_NUMBER
                ORDER BY c.DATE_UPDATED DESC, COALESCE(c.START_DATE, '1900-01-01'::DATE) DESC
            ) AS version_rank,
            -- Look ahead to next version start date to calculate effective end date for historical rows
            LEAD(c.START_DATE) OVER (
                PARTITION BY c.CONTRACT_NUMBER
                ORDER BY c.DATE_UPDATED ASC, COALESCE(c.START_DATE, '1900-01-01'::DATE) ASC
            ) AS next_version_start_date
        FROM {DATABASE}.{SCHEMA_RAW}.CONTRACT_RAW_STREAM c
        WHERE c.METADATA$ACTION = 'INSERT'
    ),
    -- For incremental runs: check if incoming active version represents a change against target active version
    changed_records AS (
        SELECT
            s.*
        FROM raw_stream s
        JOIN {DATABASE}.{SCHEMA_CANONICAL}.CONTRACT_DIM cur
            ON s.CONTRACT_NUMBER = cur.CONTRACT_NUMBER
           AND cur.LATEST_FLAG = TRUE
        WHERE s.version_rank = 1
          AND (
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
        -- 1. Expire existing active record in target (Incremental only)
        SELECT
            c.CONTRACT_NUMBER AS MERGE_KEY,
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
            c.DATE_UPDATED,
            c.START_DATE AS CALC_START_DATE,
            CURRENT_DATE() AS CALC_END_DATE,
            FALSE AS CALC_LATEST_FLAG
        FROM changed_records c

        UNION ALL

        -- 2. Insert records:
        -- Full Load: Inserts all historical records (LATEST_FLAG = FALSE) + latest record (LATEST_FLAG = TRUE)
        -- Incremental: Inserts brand new contracts OR newly updated active version
        SELECT
            NULL AS MERGE_KEY,
            s.CONTRACT_ID,
            s.CONTRACT_NUMBER,
            s.CONTRACT_NAME,
            s.CUSTOMER_ID,
            s.CONTRACT_TYPE,
            s.START_DATE,
            s.END_DATE,
            s.CONTRACT_VALUE,
            s.CONTRACT_STATUS,
            s.SALES_REGION,
            s.ACCOUNT_MANAGER,
            s.DATE_CREATED,
            s.DATE_UPDATED,
            -- Effective Start Date
            COALESCE(s.START_DATE, s.DATE_CREATED::DATE) AS CALC_START_DATE,
            -- Effective End Date
            CASE
                WHEN s.version_rank = 1 THEN '9999-12-31'::DATE
                ELSE COALESCE(s.END_DATE, s.next_version_start_date - 1, s.DATE_UPDATED::DATE)
            END AS CALC_END_DATE,
            -- Latest Flag
            CASE
                WHEN s.version_rank = 1 THEN TRUE
                ELSE FALSE
            END AS CALC_LATEST_FLAG
        FROM raw_stream s
        LEFT JOIN {DATABASE}.{SCHEMA_CANONICAL}.CONTRACT_DIM cur
            ON s.CONTRACT_NUMBER = cur.CONTRACT_NUMBER
           AND cur.LATEST_FLAG = TRUE
        WHERE cur.CONTRACT_NUMBER IS NULL -- Brand new contract or Full Load (target is empty)
           OR s.version_rank > 1          -- Historical records in batch (always insert as history)
           OR s.CONTRACT_NUMBER IN (SELECT CONTRACT_NUMBER FROM changed_records) -- Updated active record
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
        CALC_START_DATE                             AS EFFECTIVE_START_DATE,
        CALC_END_DATE                               AS EFFECTIVE_END_DATE,
        CALC_LATEST_FLAG                            AS LATEST_FLAG,
        CURRENT_TIMESTAMP()                         AS DATE_CREATED,
        CURRENT_TIMESTAMP()                         AS DATE_UPDATED,
        'sales'                                     AS SOURCE_SYSTEM
    FROM staged_records
) src
ON tgt.CONTRACT_NUMBER = src.MERGE_KEY
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
