-- =====================================================================
-- transform/orchestration/tasks.sql
-- Snowflake Native Task DAG for Automated Transformation Execution
-- Database: {DATABASE} | Warehouse: {WAREHOUSE}
-- =====================================================================

-- Root Task: Runs Canonical Transformations when raw streams have data
-- (or can be triggered on schedule / on-demand via EXECUTE TASK)
CREATE OR REPLACE TASK {DATABASE}.{SCHEMA_CANONICAL}.TSK_LOAD_CANONICAL
  WAREHOUSE = {WAREHOUSE}
  SCHEDULE = '60 MINUTE'
  WHEN
    SYSTEM$STREAM_HAS_DATA('{DATABASE}.{SCHEMA_RAW}.CONTRACT_RAW_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('{DATABASE}.{SCHEMA_RAW}.CUSTOMER_RAW_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('{DATABASE}.{SCHEMA_RAW}.PRODUCT_RAW_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('{DATABASE}.{SCHEMA_RAW}.DIRECT_SALE_LINES_RAW_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('{DATABASE}.{SCHEMA_RAW}.INDIRECT_SALE_LINES_RAW_STREAM')
  AS
    CALL {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_CANONICAL();

-- Downstream Task: Runs Curated Transformations automatically AFTER canonical load
CREATE OR REPLACE TASK {DATABASE}.{SCHEMA_CANONICAL}.TSK_LOAD_CURATED
  WAREHOUSE = {WAREHOUSE}
  AFTER {DATABASE}.{SCHEMA_CANONICAL}.TSK_LOAD_CANONICAL
  AS
    CALL {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_CURATED();

-- Standalone On-Demand Pipeline Task (executes entire pipeline in one call)
CREATE OR REPLACE TASK {DATABASE}.{SCHEMA_CANONICAL}.TSK_RUN_FULL_PIPELINE
  WAREHOUSE = {WAREHOUSE}
  AS
    CALL {DATABASE}.{SCHEMA_CANONICAL}.SP_RUN_FULL_TRANSFORMATION();

