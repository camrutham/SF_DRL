-- ================================================================
-- Incremental Load for sales_canonical.product_dim
-- Source: sales_raw.product_raw, sales_raw.product_cat_raw
-- Target: sales_canonical.product_dim
-- ================================================================

-- ============================================================================
-- FULL LOAD SECTION
-- ============================================================================

INSERT INTO sales_canonical.product_dim (
    product_id,
    product_name,
    product_category_name,
    date_updated
)
SELECT 
    p.product_id,
    p.product_name,
    c.product_category_name,
    GREATEST(p.date_updated, c.date_updated) AS date_updated
FROM sales_raw.product_raw p
LEFT JOIN sales_raw.product_cat_raw c
    ON p.product_cat_id = c.product_cat_id;


-- ============================================================================
-- INCREMENTAL LOAD SECTION
-- ============================================================================

MERGE INTO sales_canonical.product_dim AS target
USING (
    SELECT 
        p.product_id,
        p.product_name,
        c.product_category_name,
        GREATEST(p.date_updated, c.date_updated) AS date_updated
    FROM sales_raw.product_raw p
    LEFT JOIN sales_raw.product_cat_raw c
        ON p.product_cat_id = c.product_cat_id
) AS src
ON target.product_id = src.product_id

WHEN MATCHED AND src.date_updated > target.date_updated THEN
    UPDATE SET
        target.product_name = src.product_name,
        target.product_category_name = src.product_category_name,
        target.date_updated = src.date_updated

WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, product_category_name, date_updated)
    VALUES (src.product_id, src.product_name, src.product_category_name, src.date_updated);