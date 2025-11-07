-- ============================================================================
-- Customer Raw Ingestion Script
-- Source: s3://sales-cam-dev/external_stage/customer/customer.parquet
-- Target: sales_raw.customer_raw
-- Author: Data Engineering Team
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Step 1: File Format Definition
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = 'PARQUET';

-- ----------------------------------------------------------------------------
-- Step 2: Storage Integration with S3
-- ----------------------------------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::849107555298:role/aws_s3_sf_int'
  STORAGE_ALLOWED_LOCATIONS = ('s3://sales-cam-dev/external_stage/')
  COMMENT = 'Integration between Snowflake and S3 for customer data ingestion';

-- ----------------------------------------------------------------------------
-- Step 3: External Stage for Customer Data
-- ----------------------------------------------------------------------------
CREATE OR REPLACE STAGE customer_stage
  URL = 's3://sales-cam-dev/external_stage/customer/'
  FILE_FORMAT = parquet_format
  STORAGE_INTEGRATION = s3_int;

-- ============================================================================
-- FULL LOAD SECTION
-- ============================================================================
CREATE OR REPLACE TEMP TABLE customer_stage_temp AS
SELECT 
    $1:customer_id::INT AS customer_id,
    $1:customer_name::STRING AS customer_name,
    $1:customer_state::STRING AS customer_state,
    $1:customer_type::STRING AS customer_type,
    $1:customer_region::STRING AS customer_region,
    TO_TIMESTAMP(($1:date_updated::NUMBER) / 1e9) AS date_updated
FROM @customer_stage/customer.parquet;

-- Optional: Inspect data
SELECT * FROM customer_stage_temp;

-- Load data into raw customer table
INSERT INTO customer_raw
SELECT * FROM customer_stage_temp;

-- ============================================================================
-- INCREMENTAL LOAD SECTION
-- ============================================================================
CREATE OR REPLACE TEMP TABLE customer_stage_incr_temp AS
SELECT 
    $1:customer_id::INT AS customer_id,
    $1:customer_name::STRING AS customer_name,
    $1:customer_state::STRING AS customer_state,
    $1:customer_type::STRING AS customer_type,
    $1:customer_region::STRING AS customer_region,
    TO_TIMESTAMP(($1:date_updated::NUMBER) / 1e9) AS date_updated
FROM @customer_stage/customer.parquet;

MERGE INTO customer_raw AS target
USING (
    SELECT 
        customer_id,
        customer_name,
        customer_state,
        customer_type,
        customer_region,
        date_updated
    FROM customer_stage_incr_temp
) AS src
ON target.customer_id = src.customer_id
WHEN MATCHED AND src.date_updated > target.date_updated THEN
    UPDATE SET
        customer_name = src.customer_name,
        customer_state = src.customer_state,
        customer_type = src.customer_type,
        customer_region = src.customer_region,
        date_updated = src.date_updated
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, customer_state, customer_type, customer_region, date_updated)
    VALUES (src.customer_id, src.customer_name, src.customer_state, src.customer_type, src.customer_region, src.date_updated);
