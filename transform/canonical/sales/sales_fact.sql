-- =====================================================================
-- transform/canonical/sales/sales_fact.sql
-- Consolidated Sales Fact Transformation (Pure DML)
-- Target Table: {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT
-- Source Tables: {DATABASE}.{SCHEMA_RAW}.STG_DIRECT_SALES, STG_INDIRECT_SALES
--
-- Load Behavior:
--   - Full Load: When target is empty (or truncated), COALESCE defaults
--     to '1900-01-01', loading and inserting all direct & indirect sales.
--   - Incremental Load: When target has data, filters records where
--     DATE_UPDATED > MAX(target date), updating matched lines and inserting new.
-- =====================================================================

MERGE INTO {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT tgt
USING (
    WITH combined_sales AS (
        SELECT
            s.SALE_LINE_ID AS SRC_SYS_ID,
            CAST(s.SALE_LINE_ID AS VARCHAR) AS SALE_LINE_ID,
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
            CAST(s.SALE_LINE_ID AS VARCHAR) AS SALE_LINE_ID,
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
