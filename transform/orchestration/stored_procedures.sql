-- =====================================================================
-- transform/orchestration/stored_procedures.sql
-- Centralized Snowflake Stored Procedures for Canonical, Curated, and Pipeline
-- Supports both Incremental Load and Full Refresh via (is_full_refresh BOOLEAN)
-- Database: {DATABASE} | Schemas: {SCHEMA_CANONICAL}, {SCHEMA_CURATED}
-- =====================================================================

-- =====================================================================
-- 1. CANONICAL PROCEDURES
-- =====================================================================

-- 1.1 SP_LOAD_CONTRACT_DIM
CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_CONTRACT_DIM(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_affected INTEGER DEFAULT 0;
BEGIN
    IF (is_full_refresh) THEN
        TRUNCATE TABLE {DATABASE}.{SCHEMA_CANONICAL}.CONTRACT_DIM;
    END IF;

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
    rows_affected := SQLROWCOUNT;
    RETURN 'SUCCESS: CONTRACT_DIM loaded (Full refresh: ' || :is_full_refresh || '). Rows affected: ' || rows_affected;
END;
$$;

-- 1.2 SP_LOAD_CUSTOMER_DIM
CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_CUSTOMER_DIM(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_affected INTEGER DEFAULT 0;
BEGIN
    IF (is_full_refresh) THEN
        TRUNCATE TABLE {DATABASE}.{SCHEMA_CANONICAL}.CUSTOMER_DIM;
    END IF;

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
    rows_affected := SQLROWCOUNT;
    RETURN 'SUCCESS: CUSTOMER_DIM loaded (Full refresh: ' || :is_full_refresh || '). Rows affected: ' || rows_affected;
END;
$$;

-- 1.3 SP_LOAD_PRODUCT_DIM
CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_PRODUCT_DIM(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_affected INTEGER DEFAULT 0;
BEGIN
    IF (is_full_refresh) THEN
        TRUNCATE TABLE {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM;
    END IF;

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
    rows_affected := SQLROWCOUNT;
    RETURN 'SUCCESS: PRODUCT_DIM loaded (Full refresh: ' || :is_full_refresh || '). Rows affected: ' || rows_affected;
END;
$$;

-- 1.4 SP_LOAD_SALES_FACT
CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_SALES_FACT(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_affected INTEGER DEFAULT 0;
BEGIN
    IF (is_full_refresh) THEN
        TRUNCATE TABLE {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT;
    END IF;

    MERGE INTO {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT tgt
    USING (
        WITH combined_sales AS (
            SELECT
                s.SALE_LINE_ID AS SRC_SYS_ID,
                s.SALE_LINE_ID::VARCHAR AS SALE_LINE_ID,
                s.CUSTOMER_ID,
                s.PRODUCT_ID,
                s.CONTRACT_ID,
                s.SUBMISSION_ID,
                s.SALE_TYPE,
                s.INV_DATE,
                s.INV_QTY,
                s.INV_AMT,
                s.DISC_REQUESTED,
                s.DISC_PAID,
                s.LINE_SEVERITY,
                s.INV_AMT AS GROSS_SALES,
                (s.INV_AMT - COALESCE(s.DISC_PAID, 0)) AS NET_AMOUNT,
                COALESCE(s.DISC_PAID, 0) AS DISCOUNT_AMOUNT,
                CASE WHEN s.INV_AMT = 0 THEN NULL ELSE (COALESCE(s.DISC_PAID, 0) / s.INV_AMT) * 100 END AS DISCOUNT_PERCENT,
                CASE WHEN COALESCE(s.LINE_SEVERITY, '') = 'HIGH' THEN TRUE ELSE FALSE END AS LINE_SEVERITY_FLAG,
                s.SRC_SYS_DATE_CREATED,
                s.SRC_SYS_DATE_UPDATED,
                s.ETL_DATE_CREATED,
                s.ETL_DATE_UPDATED,
                s.SOURCE_SYSTEM,
                s.DATE_UPDATED
            FROM {DATABASE}.{SCHEMA_RAW}.STG_DIRECT_SALES s

            UNION ALL

            SELECT
                s.SALE_LINE_ID AS SRC_SYS_ID,
                s.SALE_LINE_ID::VARCHAR AS SALE_LINE_ID,
                s.CUSTOMER_ID,
                s.PRODUCT_ID,
                s.CONTRACT_ID,
                s.SUBMISSION_ID,
                s.SALE_TYPE,
                s.INV_DATE,
                s.INV_QTY,
                s.INV_AMT,
                s.DISC_REQUESTED,
                s.DISC_PAID,
                s.LINE_SEVERITY,
                s.INV_AMT AS GROSS_SALES,
                (s.INV_AMT - COALESCE(s.DISC_PAID, 0)) AS NET_AMOUNT,
                COALESCE(s.DISC_PAID, 0) AS DISCOUNT_AMOUNT,
                CASE WHEN s.INV_AMT = 0 THEN NULL ELSE (COALESCE(s.DISC_PAID, 0) / s.INV_AMT) * 100 END AS DISCOUNT_PERCENT,
                CASE WHEN COALESCE(s.LINE_SEVERITY, '') = 'HIGH' THEN TRUE ELSE FALSE END AS LINE_SEVERITY_FLAG,
                s.SRC_SYS_DATE_CREATED,
                s.SRC_SYS_DATE_UPDATED,
                s.ETL_DATE_CREATED,
                s.ETL_DATE_UPDATED,
                s.SOURCE_SYSTEM,
                s.DATE_UPDATED
            FROM {DATABASE}.{SCHEMA_RAW}.STG_INDIRECT_SALES s
        )
        SELECT * FROM combined_sales
        WHERE DATE_UPDATED > COALESCE((SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, ETL_DATE_UPDATED)) FROM {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT), '1900-01-01'::TIMESTAMP_NTZ)
           OR SRC_SYS_DATE_UPDATED > COALESCE((SELECT MAX(COALESCE(SRC_SYS_DATE_UPDATED, ETL_DATE_UPDATED)) FROM {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT), '1900-01-01'::TIMESTAMP_NTZ)
    ) src
    ON tgt.SRC_SYS_ID = src.SRC_SYS_ID AND tgt.SALE_TYPE = src.SALE_TYPE
    WHEN MATCHED THEN
        UPDATE SET
            tgt.SALE_LINE_ID         = src.SALE_LINE_ID,
            tgt.CUSTOMER_ID          = src.CUSTOMER_ID,
            tgt.PRODUCT_ID           = src.PRODUCT_ID,
            tgt.CONTRACT_ID          = src.CONTRACT_ID,
            tgt.SUBMISSION_ID        = src.SUBMISSION_ID,
            tgt.INV_DATE             = src.INV_DATE,
            tgt.INV_QTY              = src.INV_QTY,
            tgt.INV_AMT              = src.INV_AMT,
            tgt.DISC_REQUESTED       = src.DISC_REQUESTED,
            tgt.DISC_PAID            = src.DISC_PAID,
            tgt.LINE_SEVERITY        = src.LINE_SEVERITY,
            tgt.GROSS_SALES          = src.GROSS_SALES,
            tgt.NET_AMOUNT           = src.NET_AMOUNT,
            tgt.DISCOUNT_AMOUNT      = src.DISCOUNT_AMOUNT,
            tgt.DISCOUNT_PERCENT     = src.DISCOUNT_PERCENT,
            tgt.LINE_SEVERITY_FLAG   = src.LINE_SEVERITY_FLAG,
            tgt.SRC_SYS_DATE_UPDATED = src.SRC_SYS_DATE_UPDATED,
            tgt.ETL_DATE_UPDATED     = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            SRC_SYS_ID, SALE_LINE_ID, CUSTOMER_ID, PRODUCT_ID, CONTRACT_ID,
            SUBMISSION_ID, SALE_TYPE, INV_DATE, INV_QTY, INV_AMT,
            DISC_REQUESTED, DISC_PAID, LINE_SEVERITY, GROSS_SALES,
            NET_AMOUNT, DISCOUNT_AMOUNT, DISCOUNT_PERCENT, LINE_SEVERITY_FLAG,
            SRC_SYS_DATE_CREATED, SRC_SYS_DATE_UPDATED, ETL_DATE_CREATED,
            ETL_DATE_UPDATED, SOURCE_SYSTEM
        )
        VALUES (
            src.SRC_SYS_ID, src.SALE_LINE_ID, src.CUSTOMER_ID, src.PRODUCT_ID, src.CONTRACT_ID,
            src.SUBMISSION_ID, src.SALE_TYPE, src.INV_DATE, src.INV_QTY, src.INV_AMT,
            src.DISC_REQUESTED, src.DISC_PAID, src.LINE_SEVERITY, src.GROSS_SALES,
            src.NET_AMOUNT, src.DISCOUNT_AMOUNT, src.DISCOUNT_PERCENT, src.LINE_SEVERITY_FLAG,
            src.SRC_SYS_DATE_CREATED, src.SRC_SYS_DATE_UPDATED, CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(), src.SOURCE_SYSTEM
        );
    rows_affected := SQLROWCOUNT;
    RETURN 'SUCCESS: SALES_FACT loaded (Full refresh: ' || :is_full_refresh || '). Rows affected: ' || rows_affected;
END;
$$;

-- 1.5 Master Canonical Procedure: SP_LOAD_CANONICAL
CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_CANONICAL(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    res_contract VARCHAR;
    res_customer VARCHAR;
    res_product VARCHAR;
    res_fact VARCHAR;
BEGIN
    CALL {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_CONTRACT_DIM(:is_full_refresh) INTO :res_contract;
    CALL {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_CUSTOMER_DIM(:is_full_refresh) INTO :res_customer;
    CALL {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_PRODUCT_DIM(:is_full_refresh) INTO :res_product;
    CALL {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_SALES_FACT(:is_full_refresh) INTO :res_fact;
    RETURN 'CANONICAL LOAD COMPLETE (Full refresh: ' || :is_full_refresh || '):\n' || res_contract || '\n' || res_customer || '\n' || res_product || '\n' || res_fact;
END;
$$;


-- =====================================================================
-- 2. CURATED PROCEDURES
-- =====================================================================

-- 2.1 SP_LOAD_CUSTOMER_ANALYTICS
CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_CUSTOMER_ANALYTICS(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_affected INTEGER DEFAULT 0;
BEGIN
    IF (is_full_refresh) THEN
        TRUNCATE TABLE {DATABASE}.{SCHEMA_CURATED}.CUSTOMER_ANALYTICS;
    END IF;

    MERGE INTO {DATABASE}.{SCHEMA_CURATED}.CUSTOMER_ANALYTICS tgt
    USING (
        SELECT
            c.CUSTOMER_ID,
            c.CUSTOMER_NAME,
            c.CUSTOMER_TYPE,
            COUNT(DISTINCT f.SALE_LINE_ID) AS TOTAL_ORDERS,
            SUM(f.INV_QTY) AS TOTAL_QTY,
            SUM(f.GROSS_SALES) AS TOTAL_GROSS_SALES,
            SUM(f.NET_AMOUNT) AS TOTAL_NET_AMOUNT,
            SUM(f.DISCOUNT_AMOUNT) AS TOTAL_DISCOUNT,
            AVG(f.DISCOUNT_PERCENT) AS AVG_DISCOUNT_PERCENT,
            MIN(f.INV_DATE) AS FIRST_PURCHASE_DATE,
            MAX(f.INV_DATE) AS LAST_PURCHASE_DATE,
            SUM(CASE WHEN f.LINE_SEVERITY_FLAG = TRUE THEN 1 ELSE 0 END) AS HIGH_SEVERITY_COUNT,
            CURRENT_TIMESTAMP() AS ETL_DATE_CREATED
        FROM {DATABASE}.{SCHEMA_CANONICAL}.CUSTOMER_DIM c
        LEFT JOIN {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT f
            ON c.CUSTOMER_ID = f.CUSTOMER_ID
        GROUP BY
            c.CUSTOMER_ID,
            c.CUSTOMER_NAME,
            c.CUSTOMER_TYPE
    ) src
    ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
    WHEN MATCHED THEN
        UPDATE SET
            tgt.CUSTOMER_NAME        = src.CUSTOMER_NAME,
            tgt.CUSTOMER_TYPE        = src.CUSTOMER_TYPE,
            tgt.TOTAL_ORDERS         = src.TOTAL_ORDERS,
            tgt.TOTAL_QTY            = src.TOTAL_QTY,
            tgt.TOTAL_GROSS_SALES    = src.TOTAL_GROSS_SALES,
            tgt.TOTAL_NET_AMOUNT     = src.TOTAL_NET_AMOUNT,
            tgt.TOTAL_DISCOUNT       = src.TOTAL_DISCOUNT,
            tgt.AVG_DISCOUNT_PERCENT = src.AVG_DISCOUNT_PERCENT,
            tgt.FIRST_PURCHASE_DATE  = src.FIRST_PURCHASE_DATE,
            tgt.LAST_PURCHASE_DATE   = src.LAST_PURCHASE_DATE,
            tgt.HIGH_SEVERITY_COUNT  = src.HIGH_SEVERITY_COUNT,
            tgt.ETL_DATE_CREATED     = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_TYPE, TOTAL_ORDERS,
            TOTAL_QTY, TOTAL_GROSS_SALES, TOTAL_NET_AMOUNT, TOTAL_DISCOUNT,
            AVG_DISCOUNT_PERCENT, FIRST_PURCHASE_DATE, LAST_PURCHASE_DATE,
            HIGH_SEVERITY_COUNT, ETL_DATE_CREATED
        )
        VALUES (
            src.CUSTOMER_ID, src.CUSTOMER_NAME, src.CUSTOMER_TYPE, src.TOTAL_ORDERS,
            src.TOTAL_QTY, src.TOTAL_GROSS_SALES, src.TOTAL_NET_AMOUNT, src.TOTAL_DISCOUNT,
            src.AVG_DISCOUNT_PERCENT, src.FIRST_PURCHASE_DATE, src.LAST_PURCHASE_DATE,
            src.HIGH_SEVERITY_COUNT, src.ETL_DATE_CREATED
        );
    rows_affected := SQLROWCOUNT;
    RETURN 'SUCCESS: CUSTOMER_ANALYTICS loaded (Full refresh: ' || :is_full_refresh || '). Rows affected: ' || rows_affected;
END;
$$;

-- 2.2 SP_LOAD_PRODUCT_PERFORMANCE
CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_PRODUCT_PERFORMANCE(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_affected INTEGER DEFAULT 0;
BEGIN
    IF (is_full_refresh) THEN
        TRUNCATE TABLE {DATABASE}.{SCHEMA_CURATED}.PRODUCT_PERFORMANCE;
    END IF;

    MERGE INTO {DATABASE}.{SCHEMA_CURATED}.PRODUCT_PERFORMANCE tgt
    USING (
        SELECT
            p.PRODUCT_ID,
            p.PRODUCT_NAME,
            p.PRODUCT_CATEGORY,
            p.BRAND,
            SUM(f.INV_QTY) AS TOTAL_QTY,
            SUM(f.GROSS_SALES) AS TOTAL_GROSS_SALES,
            SUM(f.NET_AMOUNT) AS TOTAL_NET_AMOUNT,
            SUM(f.DISCOUNT_AMOUNT) AS TOTAL_DISCOUNT,
            AVG(f.DISCOUNT_PERCENT) AS AVG_DISCOUNT_PERCENT,
            COUNT(DISTINCT f.SALE_LINE_ID) AS TOTAL_TRANSACTIONS,
            SUM(CASE WHEN f.LINE_SEVERITY_FLAG = TRUE THEN 1 ELSE 0 END) AS HIGH_SEVERITY_COUNT,
            CURRENT_TIMESTAMP() AS ETL_DATE_CREATED
        FROM {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM p
        LEFT JOIN {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT f
            ON p.PRODUCT_ID = f.PRODUCT_ID
        GROUP BY
            p.PRODUCT_ID,
            p.PRODUCT_NAME,
            p.PRODUCT_CATEGORY,
            p.BRAND
    ) src
    ON tgt.PRODUCT_ID = src.PRODUCT_ID
    WHEN MATCHED THEN
        UPDATE SET
            tgt.PRODUCT_NAME         = src.PRODUCT_NAME,
            tgt.PRODUCT_CATEGORY     = src.PRODUCT_CATEGORY,
            tgt.BRAND                = src.BRAND,
            tgt.TOTAL_QTY            = src.TOTAL_QTY,
            tgt.TOTAL_GROSS_SALES    = src.TOTAL_GROSS_SALES,
            tgt.TOTAL_NET_AMOUNT     = src.TOTAL_NET_AMOUNT,
            tgt.TOTAL_DISCOUNT       = src.TOTAL_DISCOUNT,
            tgt.AVG_DISCOUNT_PERCENT = src.AVG_DISCOUNT_PERCENT,
            tgt.TOTAL_TRANSACTIONS   = src.TOTAL_TRANSACTIONS,
            tgt.HIGH_SEVERITY_COUNT  = src.HIGH_SEVERITY_COUNT,
            tgt.ETL_DATE_CREATED     = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            PRODUCT_ID, PRODUCT_NAME, PRODUCT_CATEGORY, BRAND,
            TOTAL_QTY, TOTAL_GROSS_SALES, TOTAL_NET_AMOUNT, TOTAL_DISCOUNT,
            AVG_DISCOUNT_PERCENT, TOTAL_TRANSACTIONS, HIGH_SEVERITY_COUNT,
            ETL_DATE_CREATED
        )
        VALUES (
            src.PRODUCT_ID, src.PRODUCT_NAME, src.PRODUCT_CATEGORY, src.BRAND,
            src.TOTAL_QTY, src.TOTAL_GROSS_SALES, src.TOTAL_NET_AMOUNT, src.TOTAL_DISCOUNT,
            src.AVG_DISCOUNT_PERCENT, src.TOTAL_TRANSACTIONS, src.HIGH_SEVERITY_COUNT,
            src.ETL_DATE_CREATED
        );
    rows_affected := SQLROWCOUNT;
    RETURN 'SUCCESS: PRODUCT_PERFORMANCE loaded (Full refresh: ' || :is_full_refresh || '). Rows affected: ' || rows_affected;
END;
$$;

-- 2.3 SP_LOAD_SALES_SUMMARY
CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_SALES_SUMMARY(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_affected INTEGER DEFAULT 0;
BEGIN
    IF (is_full_refresh) THEN
        TRUNCATE TABLE {DATABASE}.{SCHEMA_CURATED}.SALES_SUMMARY;
    END IF;

    MERGE INTO {DATABASE}.{SCHEMA_CURATED}.SALES_SUMMARY tgt
    USING (
        SELECT
            f.INV_DATE AS SALES_DATE,
            f.CUSTOMER_ID,
            f.PRODUCT_ID,
            f.CONTRACT_ID,
            f.SALE_TYPE,
            SUM(f.INV_QTY) AS TOTAL_QTY,
            SUM(f.GROSS_SALES) AS TOTAL_GROSS_SALES,
            SUM(f.NET_AMOUNT) AS TOTAL_NET_AMOUNT,
            SUM(f.DISCOUNT_AMOUNT) AS TOTAL_DISCOUNT,
            AVG(f.DISCOUNT_PERCENT) AS AVG_DISCOUNT_PERCENT,
            COUNT(*) AS SALES_COUNT,
            SUM(CASE WHEN f.LINE_SEVERITY_FLAG = TRUE THEN 1 ELSE 0 END) AS HIGH_SEVERITY_LINES,
            CURRENT_TIMESTAMP() AS ETL_DATE_CREATED
        FROM {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT f
        GROUP BY
            f.INV_DATE,
            f.CUSTOMER_ID,
            f.PRODUCT_ID,
            f.CONTRACT_ID,
            f.SALE_TYPE
    ) src
    ON tgt.SALES_DATE = src.SALES_DATE
       AND tgt.CUSTOMER_ID = src.CUSTOMER_ID
       AND tgt.PRODUCT_ID = src.PRODUCT_ID
       AND tgt.CONTRACT_ID = src.CONTRACT_ID
       AND tgt.SALE_TYPE = src.SALE_TYPE
    WHEN MATCHED THEN
        UPDATE SET
            tgt.TOTAL_QTY            = src.TOTAL_QTY,
            tgt.TOTAL_GROSS_SALES    = src.TOTAL_GROSS_SALES,
            tgt.TOTAL_NET_AMOUNT     = src.TOTAL_NET_AMOUNT,
            tgt.TOTAL_DISCOUNT       = src.TOTAL_DISCOUNT,
            tgt.AVG_DISCOUNT_PERCENT = src.AVG_DISCOUNT_PERCENT,
            tgt.SALES_COUNT          = src.SALES_COUNT,
            tgt.HIGH_SEVERITY_LINES  = src.HIGH_SEVERITY_LINES,
            tgt.ETL_DATE_CREATED     = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            SALES_DATE, CUSTOMER_ID, PRODUCT_ID, CONTRACT_ID, SALE_TYPE,
            TOTAL_QTY, TOTAL_GROSS_SALES, TOTAL_NET_AMOUNT, TOTAL_DISCOUNT,
            AVG_DISCOUNT_PERCENT, SALES_COUNT, HIGH_SEVERITY_LINES, ETL_DATE_CREATED
        )
        VALUES (
            src.SALES_DATE, src.CUSTOMER_ID, src.PRODUCT_ID, src.CONTRACT_ID, src.SALE_TYPE,
            src.TOTAL_QTY, src.TOTAL_GROSS_SALES, src.TOTAL_NET_AMOUNT, src.TOTAL_DISCOUNT,
            src.AVG_DISCOUNT_PERCENT, src.SALES_COUNT, src.HIGH_SEVERITY_LINES, src.ETL_DATE_CREATED
        );
    rows_affected := SQLROWCOUNT;
    RETURN 'SUCCESS: SALES_SUMMARY loaded (Full refresh: ' || :is_full_refresh || '). Rows affected: ' || rows_affected;
END;
$$;

-- 2.4 Master Curated Procedure: SP_LOAD_CURATED
CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_CURATED(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    res_customer VARCHAR;
    res_product VARCHAR;
    res_summary VARCHAR;
BEGIN
    CALL {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_CUSTOMER_ANALYTICS(:is_full_refresh) INTO :res_customer;
    CALL {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_PRODUCT_PERFORMANCE(:is_full_refresh) INTO :res_product;
    CALL {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_SALES_SUMMARY(:is_full_refresh) INTO :res_summary;
    RETURN 'CURATED LOAD COMPLETE (Full refresh: ' || :is_full_refresh || '):\n' || res_customer || '\n' || res_product || '\n' || res_summary;
END;
$$;


-- =====================================================================
-- 3. MASTER PIPELINE ORCHESTRATION PROCEDURE
-- =====================================================================

CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CANONICAL}.SP_RUN_FULL_TRANSFORMATION(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    res_canonical VARCHAR;
    res_curated VARCHAR;
BEGIN
    CALL {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_CANONICAL(:is_full_refresh) INTO :res_canonical;
    CALL {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_CURATED(:is_full_refresh) INTO :res_curated;
    RETURN '=== FULL TRANSFORMATION PIPELINE COMPLETE (Full refresh: ' || :is_full_refresh || ') ===\n\n[CANONICAL STAGE]\n' || res_canonical || '\n\n[CURATED STAGE]\n' || res_curated;
END;
$$;
