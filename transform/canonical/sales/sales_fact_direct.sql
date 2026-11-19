-- =====================================================================
-- transform/canonical/sales/sales_fact_direct.sql
-- Direct Sales Fact View Projection
-- Database: {DATABASE} | Schema: {SCHEMA_CANONICAL}
-- =====================================================================

CREATE OR REPLACE VIEW {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_DIRECT AS
SELECT *
FROM {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT
WHERE SALE_TYPE = 'DIRECT';

