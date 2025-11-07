-- ============================================================================
-- Customer Raw Ingestion Script
-- Source: s3://sales-cam-dev/external_stage/product/product_category.parquet
-- Target: sales_raw.product_category_raw
-- Author: Data Engineering Team
-- ============================================================================

-- ============================================================================
-- FULL LOAD SECTION
-- ============================================================================
CREATE OR REPLACE TEMP TABLE product_category_stage_temp AS
SELECT 
    $1:product_cat_id::INT AS product_cat_id,
    $1:product_category_name::STRING AS product_category_name,
    TO_TIMESTAMP(($1:date_updated::NUMBER) / 1e9) AS date_updated
FROM @product_category_stage/product_category.parquet;

-- Load data into raw customer table
INSERT INTO product_cat_raw
SELECT * FROM product_category_stage_temp;

-- ============================================================================
-- INCREMENTAL LOAD SECTION
-- ============================================================================
CREATE OR REPLACE TEMP TABLE product_category_stage_incr_temp AS
SELECT 
    $1:product_cat_id::INT AS product_cat_id,
    $1:product_category_name::STRING AS product_category_name,
    TO_TIMESTAMP(($1:date_updated::NUMBER) / 1e9) AS date_updated
FROM @product_category_stage/product_category.parquet;

MERGE INTO product_cat_raw AS target
USING (
    SELECT 
        product_cat_id,
        product_category_name,
        date_updated
    FROM product_category_stage_incr_temp
) AS src
ON target.product_cat_id = src.product_cat_id
WHEN MATCHED AND src.date_updated > target.date_updated THEN
    UPDATE SET
        product_category_name = src.product_category_name,
        date_updated = src.date_updated
WHEN NOT MATCHED THEN
    INSERT (product_cat_id, product_category_name, date_updated)
    VALUES (src.product_cat_id, src.product_category_name, src.date_updated);
