-- =====================================================================
-- transform/canonical/masterdata/customer_dim.sql
-- Customer Dimension SCD Type 2 Transformation (Pure DML)
-- Target Table: {DATABASE}.{SCHEMA_CANONICAL}.CUSTOMER_DIM
-- Source Table: {DATABASE}.{SCHEMA_RAW}.CUSTOMER_RAW
--
-- Load Behavior:
--   - Full Load: When target is empty (or truncated), COALESCE defaults
--     to '1900-01-01', loading and inserting all source records.
--   - Incremental Load: When target has data, filters records where
--     DATE_UPDATED > MAX(target date), updating matched rows and inserting new.
-- =====================================================================

MERGE INTO {DATABASE}.{SCHEMA_CANONICAL}.CUSTOMER_DIM tgt
USING (
    SELECT
        c.CUSTOMER_ID                               AS SRC_SYS_ID,
        c.CUSTOMER_ID::VARCHAR                      AS CUSTOMER_ID,
        c.CUSTOMER_NUMBER,
        c.CUSTOMER_NAME,
        NULL                                        AS CUSTOMER_NAME_NORMALIZED,
        c.CUSTOMER_TYPE,
        c.INDUSTRY,
        c.ADDRESS,
        c.CITY,
        c.STATE,
        c.COUNTRY,
        c.EMAIL,
        c.PHONE,
        c.DATE_CREATED                              AS SRC_SYS_DATE_CREATED,
        c.DATE_UPDATED                              AS SRC_SYS_DATE_UPDATED,
        CURRENT_DATE()                              AS EFFECTIVE_START_DATE,
        '9999-12-31'::DATE                          AS EFFECTIVE_END_DATE,
        TRUE                                        AS LATEST_FLAG,
        CURRENT_TIMESTAMP()                         AS DATE_CREATED,
        CURRENT_TIMESTAMP()                         AS DATE_UPDATED,
        'sales'                                     AS SOURCE_SYSTEM
    FROM {DATABASE}.{SCHEMA_RAW}.CUSTOMER_RAW c
    WHERE c.DATE_UPDATED > COALESCE(
        (SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, DATE_UPDATED)) FROM {DATABASE}.{SCHEMA_CANONICAL}.CUSTOMER_DIM),
        '1900-01-01'::TIMESTAMP_NTZ
    )
) src
ON tgt.SRC_SYS_ID = src.SRC_SYS_ID
WHEN MATCHED THEN
    UPDATE SET
        tgt.CUSTOMER_ID              = src.CUSTOMER_ID,
        tgt.CUSTOMER_NUMBER          = src.CUSTOMER_NUMBER,
        tgt.CUSTOMER_NAME            = src.CUSTOMER_NAME,
        tgt.CUSTOMER_NAME_NORMALIZED = src.CUSTOMER_NAME_NORMALIZED,
        tgt.CUSTOMER_TYPE            = src.CUSTOMER_TYPE,
        tgt.INDUSTRY                 = src.INDUSTRY,
        tgt.ADDRESS                  = src.ADDRESS,
        tgt.CITY                     = src.CITY,
        tgt.STATE                    = src.STATE,
        tgt.COUNTRY                  = src.COUNTRY,
        tgt.EMAIL                    = src.EMAIL,
        tgt.PHONE                    = src.PHONE,
        tgt.SRC_SYS_DATE_UPDATED     = src.SRC_SYS_DATE_UPDATED,
        tgt.DATE_UPDATED             = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_ID, CUSTOMER_ID, CUSTOMER_NUMBER, CUSTOMER_NAME,
        CUSTOMER_NAME_NORMALIZED, CUSTOMER_TYPE, INDUSTRY, ADDRESS,
        CITY, STATE, COUNTRY, EMAIL, PHONE, SRC_SYS_DATE_CREATED,
        SRC_SYS_DATE_UPDATED, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE,
        LATEST_FLAG, DATE_CREATED, DATE_UPDATED, SOURCE_SYSTEM
    )
    VALUES (
        src.SRC_SYS_ID, src.CUSTOMER_ID, src.CUSTOMER_NUMBER, src.CUSTOMER_NAME,
        src.CUSTOMER_NAME_NORMALIZED, src.CUSTOMER_TYPE, src.INDUSTRY, src.ADDRESS,
        src.CITY, src.STATE, src.COUNTRY, src.EMAIL, src.PHONE, src.SRC_SYS_DATE_CREATED,
        src.SRC_SYS_DATE_UPDATED, src.EFFECTIVE_START_DATE, src.EFFECTIVE_END_DATE,
        src.LATEST_FLAG, src.DATE_CREATED, src.DATE_UPDATED, src.SOURCE_SYSTEM
    );
