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
        c.CONTRACT_ID                               AS SRC_SYS_ID,
        c.CONTRACT_ID::VARCHAR                      AS CONTRACT_ID,
        c.CONTRACT_NUMBER,
        c.CONTRACT_NAME,
        NULL                                        AS CONTRACT_NAME_NORMALIZED,
        c.CUSTOMER_ID,
        c.CONTRACT_TYPE,
        c.START_DATE,
        c.END_DATE,
        c.CONTRACT_VALUE,
        c.CONTRACT_STATUS,
        c.SALES_REGION,
        c.ACCOUNT_MANAGER,
        c.DATE_CREATED                              AS SRC_SYS_DATE_CREATED,
        c.DATE_UPDATED                              AS SRC_SYS_DATE_UPDATED,
        CURRENT_DATE()                              AS EFFECTIVE_START_DATE,
        '9999-12-31'::DATE                          AS EFFECTIVE_END_DATE,
        TRUE                                        AS LATEST_FLAG,
        CURRENT_TIMESTAMP()                         AS DATE_CREATED,
        CURRENT_TIMESTAMP()                         AS DATE_UPDATED,
        'sales'                                     AS SOURCE_SYSTEM
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
