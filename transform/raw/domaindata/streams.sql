-- =====================================================================
-- transform/raw/domaindata/streams.sql
-- Change Data Capture (CDC) Streams for Domaindata Raw Tables
-- Database: {DATABASE} | Schema: {SCHEMA_RAW}
-- =====================================================================

CREATE STREAM IF NOT EXISTS {DATABASE}.{SCHEMA_RAW}.CONTRACT_RAW_STREAM
  ON TABLE {DATABASE}.{SCHEMA_RAW}.CONTRACT_RAW
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream tracking inserts and updates on CONTRACT_RAW';
