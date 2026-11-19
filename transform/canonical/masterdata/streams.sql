-- =====================================================================
-- transform/canonical/masterdata/streams.sql
-- Change Data Capture (CDC) Streams for Canonical Masterdata Tables
-- Database: {DATABASE} | Schema: {SCHEMA_CANONICAL}
-- =====================================================================

CREATE STREAM IF NOT EXISTS {DATABASE}.{SCHEMA_CANONICAL}.CUSTOMER_DIM_STREAM
  ON TABLE {DATABASE}.{SCHEMA_CANONICAL}.CUSTOMER_DIM
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream tracking changes on CUSTOMER_DIM for curated downstream aggregations';

CREATE STREAM IF NOT EXISTS {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM_STREAM
  ON TABLE {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream tracking changes on PRODUCT_DIM for curated downstream aggregations';
