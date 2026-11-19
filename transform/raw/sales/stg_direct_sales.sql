-- =====================================================================
-- transform/raw/sales/stg_direct_sales.sql
-- Direct Sales Staging View
-- Database: {DATABASE} | Schema: {SCHEMA_RAW}
-- =====================================================================

CREATE OR REPLACE VIEW {DATABASE}.{SCHEMA_RAW}.STG_DIRECT_SALES AS
SELECT
    SALE_LINE_ID,
    CUSTOMER_ID,
    PRODUCT_ID,
    CONTRACT_ID,
    SUBMISSION_ID,
    'DIRECT'                            AS SALE_TYPE,
    INV_DATE,
    INV_QTY,
    INV_AMT,
    DISC_REQUESTED,
    DISC_PAID,
    LINE_SEVERITY,
    DATE_CREATED                        AS SRC_SYS_DATE_CREATED,
    DATE_UPDATED                        AS SRC_SYS_DATE_UPDATED,
    DATE_UPDATED,
    CURRENT_TIMESTAMP()                 AS ETL_DATE_CREATED,
    CURRENT_TIMESTAMP()                 AS ETL_DATE_UPDATED,
    'sales'                             AS SOURCE_SYSTEM
FROM {DATABASE}.{SCHEMA_RAW}.DIRECT_SALE_LINES_RAW;
