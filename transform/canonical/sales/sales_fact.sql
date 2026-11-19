-- =====================================================================
-- transform/canonical/sales/sales_fact.sql
-- Consolidated Sales Fact Transformation (Stream CDC DML)
-- Target Table: {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT
-- Source Streams:
--   - {DATABASE}.{SCHEMA_RAW}.DIRECT_SALE_LINES_RAW_STREAM
--   - {DATABASE}.{SCHEMA_RAW}.INDIRECT_SALE_LINES_RAW_STREAM
--
-- Load Behavior:
--   - Incremental Load (Stream CDC): Atomically consumes inserted/updated
--     delta records from both direct and indirect sales streams.
--   - Full Refresh: Truncates target table, queries base tables, and resets
--     the stream offsets.
-- =====================================================================

MERGE INTO {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT tgt
USING (
    WITH direct_changes AS (
        SELECT
            d.SALE_LINE_ID AS SRC_SYS_ID,
            d.SALE_LINE_ID::VARCHAR AS SALE_LINE_ID,
            d.CUSTOMER_ID,
            d.PRODUCT_ID,
            d.CONTRACT_ID,
            d.SUBMISSION_ID,
            'DIRECT' AS SALE_TYPE,
            d.INV_DATE,
            d.INV_QTY,
            d.INV_AMT,
            d.DISC_REQUESTED,
            d.DISC_PAID,
            d.LINE_SEVERITY,
            d.INV_AMT AS GROSS_SALES,
            (d.INV_AMT - COALESCE(d.DISC_PAID, 0)) AS NET_AMOUNT,
            COALESCE(d.DISC_PAID, 0) AS DISCOUNT_AMOUNT,
            CASE WHEN d.INV_AMT = 0 THEN NULL ELSE (COALESCE(d.DISC_PAID, 0) / d.INV_AMT) * 100 END AS DISCOUNT_PERCENT,
            CASE WHEN COALESCE(d.LINE_SEVERITY, '') = 'HIGH' THEN TRUE ELSE FALSE END AS LINE_SEVERITY_FLAG,
            d.DATE_CREATED AS SRC_SYS_DATE_CREATED,
            d.DATE_UPDATED AS SRC_SYS_DATE_UPDATED,
            'sales' AS SOURCE_SYSTEM,
            d.DATE_UPDATED
        FROM {DATABASE}.{SCHEMA_RAW}.DIRECT_SALE_LINES_RAW_STREAM d
        WHERE d.METADATA$ACTION = 'INSERT'
    ),
    indirect_changes AS (
        SELECT
            i.SALE_LINE_ID AS SRC_SYS_ID,
            i.SALE_LINE_ID::VARCHAR AS SALE_LINE_ID,
            i.CUSTOMER_ID,
            i.PRODUCT_ID,
            i.CONTRACT_ID,
            i.SUBMISSION_ID,
            'INDIRECT' AS SALE_TYPE,
            i.INV_DATE,
            i.INV_QTY,
            i.INV_AMT,
            i.DISC_REQUESTED,
            i.DISC_PAID,
            i.LINE_SEVERITY,
            i.INV_AMT AS GROSS_SALES,
            (i.INV_AMT - COALESCE(i.DISC_PAID, 0)) AS NET_AMOUNT,
            COALESCE(i.DISC_PAID, 0) AS DISCOUNT_AMOUNT,
            CASE WHEN i.INV_AMT = 0 THEN NULL ELSE (COALESCE(i.DISC_PAID, 0) / i.INV_AMT) * 100 END AS DISCOUNT_PERCENT,
            CASE WHEN COALESCE(i.LINE_SEVERITY, '') = 'HIGH' THEN TRUE ELSE FALSE END AS LINE_SEVERITY_FLAG,
            i.DATE_CREATED AS SRC_SYS_DATE_CREATED,
            i.DATE_UPDATED AS SRC_SYS_DATE_UPDATED,
            'sales' AS SOURCE_SYSTEM,
            i.DATE_UPDATED
        FROM {DATABASE}.{SCHEMA_RAW}.INDIRECT_SALE_LINES_RAW_STREAM i
        WHERE i.METADATA$ACTION = 'INSERT'
    ),
    combined_sales AS (
        SELECT * FROM direct_changes
        UNION ALL
        SELECT * FROM indirect_changes
    )
    SELECT * FROM combined_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC_SYS_ID, SALE_TYPE ORDER BY DATE_UPDATED DESC) = 1
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
