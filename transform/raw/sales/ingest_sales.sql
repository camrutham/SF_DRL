-- ============================================================================
-- Sales Raw Ingestion Script
-- Source: s3://sales-cam-dev/external_stage/sales/
-- Target: sales_raw.sales_raw
-- Author: Data Engineering Team
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Step 1: Schema Setup
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS sales.file_formats;
CREATE SCHEMA IF NOT EXISTS sales.external_stage;

-- ----------------------------------------------------------------------------
-- Step 2: File Format Definition
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT sales.file_formats.parquet_format
  TYPE = 'PARQUET';

-- ----------------------------------------------------------------------------
-- Step 3: Storage Integration with S3
-- ----------------------------------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::849107555298:role/aws_s3_sf_int'
  STORAGE_ALLOWED_LOCATIONS = ('s3://sales-cam-dev/external_stage/')
  COMMENT = 'Integration between Snowflake and S3 for sales data ingestion';

-- ----------------------------------------------------------------------------
-- Step 4: External Stage for Sales Data
-- ----------------------------------------------------------------------------
CREATE OR REPLACE STAGE sales.external_stage.sales_stage
  URL = 's3://sales-cam-dev/external_stage/sales/'
  FILE_FORMAT = sales.file_formats.parquet_format
  STORAGE_INTEGRATION = s3_int;

-- ============================================================================
-- FULL LOAD SECTION
-- ============================================================================

-- Create a session-scoped temporary staging table
CREATE OR REPLACE TEMP TABLE sales_stage_temp AS
SELECT 
    $1:sale_line_ref_num::INT AS sale_line_ref_num,
    $1:customer_id::INT AS customer_id,
    $1:product_id::INT AS product_id,
    $1:sales_amount::DECIMAL(12,2) AS sales_amount,
    $1:sale_qty::INT AS sale_qty,
    $1:region::STRING AS region,
    $1:order_date::DATE AS order_date,
    TO_TIMESTAMP(($1:date_updated::NUMBER) / 1e9) AS date_updated
FROM @sales.external_stage.sales_stage/sales.parquet;

-- Load into target raw table
INSERT INTO sales.sales_raw.sales_raw
SELECT * FROM sales_stage_temp;

-- Optional: drop temp table for cleanup
DROP TABLE IF EXISTS sales_stage_temp;

-- ============================================================================
-- INCREMENTAL LOAD SECTION
-- ============================================================================

-- Create temporary table for incremental data
CREATE OR REPLACE TEMP TABLE sales_stage_incr_temp AS
SELECT 
    $1:sale_line_ref_num::INT AS sale_line_ref_num,
    $1:customer_id::INT AS customer_id,
    $1:product_id::INT AS product_id,
    $1:sales_amount::DECIMAL(12,2) AS sales_amount,
    $1:sale_qty::INT AS sale_qty,
    $1:region::STRING AS region,
    $1:order_date::DATE AS order_date,
    TO_TIMESTAMP(($1:date_updated::NUMBER) / 1e9) AS date_updated
FROM @sales.external_stage.sales_stage/sales.parquet;

-- Merge incremental data based on sale_line_ref_num and date_updated
MERGE INTO sales_raw.sales_raw AS target
USING (
    SELECT 
        sale_line_ref_num,
        customer_id,
        product_id,
        sales_amount,
        sale_qty,
        region,
        order_date,
        date_updated
    FROM sales_stage_incr_temp
) AS src
ON target.sale_line_ref_num = src.sale_line_ref_num
WHEN MATCHED AND src.date_updated > target.date_updated THEN
    UPDATE SET
        customer_id = src.customer_id,
        product_id = src.product_id,
        sales_amount = src.sales_amount,
        sale_qty = src.sale_qty,
        region = src.region,
        order_date = src.order_date,
        date_updated = src.date_updated
WHEN NOT MATCHED THEN
    INSERT (sale_line_ref_num, customer_id, product_id, sales_amount, sale_qty, region, order_date, date_updated)
    VALUES (src.sale_line_ref_num, src.customer_id, src.product_id, src.sales_amount, src.sale_qty, src.region, src.order_date, src.date_updated);

-- Optional: drop temp table after merge
DROP TABLE IF EXISTS sales_stage_incr_temp;
