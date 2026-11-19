-- =====================================================================
-- transform/canonical/sales/sales_fact_indirect.sql
-- Indirect Sales Fact View Projection
-- Database: {DATABASE} | Schema: {SCHEMA_CANONICAL}
-- =====================================================================

CREATE OR REPLACE VIEW {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_INDIRECT AS
SELECT *
FROM {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT
WHERE SALE_TYPE = 'INDIRECT';

