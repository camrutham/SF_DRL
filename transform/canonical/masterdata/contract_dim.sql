-- =====================================================================
-- transform/canonical/masterdata/contract_dim.sql
-- Contract Dimension SCD Type 2 Transformation (Pure DML)
-- Target Table: {DATABASE}.{SCHEMA_CANONICAL}.CONTRACT_DIM
-- Source Table: {DATABASE}.{SCHEMA_RAW}.CONTRACT_RAW
--
-- Load Behavior:
--   - Full Load: When target is empty (or truncated), COALESCE defaults
--     to '1900-01-01', loading and inserting all source records.
--   - Incremental Load: When target has data, filters records where
--     DATE_UPDATED > MAX(target date), updating matched rows and inserting new.
-- =====================================================================

MERGE INTO {DATABASE}.{SCHEMA_CANONICAL}.CONTRACT_DIM tgt
USING (
    SELECT
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
        CAST(c.DATE_CREATED         AS TIMESTAMP_NTZ)   AS SRC_SYS_DATE_CREATED,
        CAST(c.DATE_UPDATED         AS TIMESTAMP_NTZ)   AS SRC_SYS_DATE_UPDATED,
        CAST(CURRENT_DATE()         AS DATE)            AS EFFECTIVE_START_DATE,
        CAST('9999-12-31'           AS DATE)            AS EFFECTIVE_END_DATE,
        TRUE                                            AS LATEST_FLAG,
        CAST(CURRENT_TIMESTAMP()    AS TIMESTAMP_NTZ)   AS DATE_CREATED,
        CAST(CURRENT_TIMESTAMP()    AS TIMESTAMP_NTZ)   AS DATE_UPDATED,
        CAST('sales'                AS VARCHAR)         AS SOURCE_SYSTEM
    FROM {DATABASE}.{SCHEMA_RAW}.CONTRACT_RAW c
    WHERE c.DATE_UPDATED > COALESCE(
        (SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, DATE_UPDATED)) FROM {DATABASE}.{SCHEMA_CANONICAL}.CONTRACT_DIM),
        '1900-01-01'::TIMESTAMP_NTZ
    )
) src
ON tgt.SRC_SYS_ID = src.SRC_SYS_ID
WHEN MATCHED THEN
    UPDATE SET
        tgt.CONTRACT_ID              = src.CONTRACT_ID,
        tgt.CONTRACT_NUMBER          = src.CONTRACT_NUMBER,
        tgt.CONTRACT_NAME            = src.CONTRACT_NAME,
        tgt.CONTRACT_NAME_NORMALIZED = src.CONTRACT_NAME_NORMALIZED,
        tgt.CUSTOMER_ID              = src.CUSTOMER_ID,
        tgt.CONTRACT_TYPE            = src.CONTRACT_TYPE,
        tgt.START_DATE               = src.START_DATE,
        tgt.END_DATE                 = src.END_DATE,
        tgt.CONTRACT_VALUE           = src.CONTRACT_VALUE,
        tgt.CONTRACT_STATUS          = src.CONTRACT_STATUS,
        tgt.SALES_REGION             = src.SALES_REGION,
        tgt.ACCOUNT_MANAGER          = src.ACCOUNT_MANAGER,
        tgt.SRC_SYS_DATE_UPDATED     = src.SRC_SYS_DATE_UPDATED,
        tgt.DATE_UPDATED             = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
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
