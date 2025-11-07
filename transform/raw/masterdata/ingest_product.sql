-- ============================================================================
-- Customer Raw Ingestion Script
-- Source: s3://sales-cam-dev/external_stage/product/product.parquet
-- Target: sales_raw.product_raw
-- Author: Data Engineering Team
-- ============================================================================

-- ============================================================================
-- FULL LOAD SECTION
-- ============================================================================
CREATE OR REPLACE TEMP TABLE product_stage_temp AS
SELECT 
    $1:product_id::INT AS product_id,
    $1:product_name::STRING AS product_name,
    $1:product_cat_id::INT AS product_cat_id,
    TO_TIMESTAMP(($1:date_updated::NUMBER) / 1e9) AS date_updated
FROM @product_stage/product.parquet;

-- Load data into raw customer table
INSERT INTO product_raw
SELECT * FROM product_stage_temp;

-- ============================================================================
-- INCREMENTAL LOAD SECTION
-- ============================================================================
CREATE OR REPLACE TEMP TABLE product_stage_incr_temp AS
SELECT 
    $1:product_id::INT AS product_id,
    $1:product_name::STRING AS product_name,
    $1:product_cat_id::INT AS product_cat_id,
    TO_TIMESTAMP(($1:date_updated::NUMBER) / 1e9) AS date_updated
FROM @product_stage/product.parquet;

MERGE INTO product_raw AS target
USING (
    SELECT 
        product_id,
        product_name,
        product_cat_id,
        date_updated
    FROM product_stage_incr_temp
) AS src
ON target.product_id = src.product_id
WHEN MATCHED AND src.date_updated > target.date_updated THEN
    UPDATE SET
        product_name = src.product_name,
        product_cat_id = src.product_cat_id,
        date_updated = src.date_updated
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, product_cat_id, date_updated)
    VALUES (src.product_id, src.product_name, src.product_cat_id, src.date_updated);
