-- =====================================================================
-- transform/raw/sales/streams.sql
-- Change Data Capture (CDC) Streams for Sales Raw Tables
-- Database: {DATABASE} | Schema: {SCHEMA_RAW}
-- =====================================================================

CREATE STREAM IF NOT EXISTS {DATABASE}.{SCHEMA_RAW}.DIRECT_SALE_LINES_RAW_STREAM
  ON TABLE {DATABASE}.{SCHEMA_RAW}.DIRECT_SALE_LINES_RAW
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream tracking inserts and updates on DIRECT_SALE_LINES_RAW';

CREATE STREAM IF NOT EXISTS {DATABASE}.{SCHEMA_RAW}.INDIRECT_SALE_LINES_RAW_STREAM
  ON TABLE {DATABASE}.{SCHEMA_RAW}.INDIRECT_SALE_LINES_RAW
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream tracking inserts and updates on INDIRECT_SALE_LINES_RAW';

