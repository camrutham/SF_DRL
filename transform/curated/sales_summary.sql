-- =====================================================================
-- transform/curated/sales_summary.sql
-- Daily Sales Summary Rollup Transformation (Stream CDC DML)
-- Target Table: {DATABASE}.{SCHEMA_CURATED}.SALES_SUMMARY
-- Source Table: {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT
-- Source Stream: {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_STREAM_SUMMARY
--
-- Load Behavior:
--   - Incremental Load (Stream CDC): Re-aggregates daily metrics ONLY for
--     the specific date/customer/product/contract/sale_type slices that
--     changed in SALES_FACT_STREAM_SUMMARY.
--   - Full Refresh: Re-aggregates entire history across all slices.
-- =====================================================================

MERGE INTO {DATABASE}.{SCHEMA_CURATED}.SALES_SUMMARY tgt
USING (
    WITH changed_keys AS (
        SELECT DISTINCT INV_DATE, CUSTOMER_ID, PRODUCT_ID, CONTRACT_ID, SALE_TYPE
        FROM {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_STREAM_SUMMARY
        WHERE METADATA$ACTION = 'INSERT'
    )
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
    INNER JOIN changed_keys k
        ON f.INV_DATE = k.INV_DATE
       AND f.CUSTOMER_ID = k.CUSTOMER_ID
       AND f.PRODUCT_ID = k.PRODUCT_ID
       AND f.CONTRACT_ID = k.CONTRACT_ID
       AND f.SALE_TYPE = k.SALE_TYPE
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
