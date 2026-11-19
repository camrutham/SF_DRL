-- =====================================================================
-- transform/canonical/sales/streams.sql
-- Change Data Capture (CDC) Streams for Canonical Sales Fact
-- Database: {DATABASE} | Schema: {SCHEMA_CANONICAL}
-- =====================================================================

CREATE STREAM IF NOT EXISTS {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_STREAM_SUMMARY
  ON TABLE {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream tracking changes on SALES_FACT for SALES_SUMMARY rollup';

CREATE STREAM IF NOT EXISTS {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_STREAM_CUSTOMER
  ON TABLE {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream tracking changes on SALES_FACT for CUSTOMER_ANALYTICS';

CREATE STREAM IF NOT EXISTS {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_STREAM_PRODUCT
  ON TABLE {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream tracking changes on SALES_FACT for PRODUCT_PERFORMANCE';
