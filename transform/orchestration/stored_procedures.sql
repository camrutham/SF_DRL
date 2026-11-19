-- =====================================================================
-- transform/orchestration/stored_procedures.sql
-- Master Pipeline Orchestrator Procedures
-- Coordinates individual model procedures (dynamically generated and
-- deployed from transform/canonical and transform/curated model files).
-- Database: {DATABASE} | Schemas: {SCHEMA_CANONICAL}, {SCHEMA_CURATED}
-- =====================================================================

-- =====================================================================
-- 1. MASTER CANONICAL STAGE PROCEDURE
-- =====================================================================
CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_CANONICAL(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    res_contract VARCHAR;
    res_customer VARCHAR;
    res_product  VARCHAR;
    res_fact     VARCHAR;
BEGIN
    CALL {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_CONTRACT_DIM(:is_full_refresh) INTO :res_contract;
    CALL {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_CUSTOMER_DIM(:is_full_refresh) INTO :res_customer;
    CALL {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_PRODUCT_DIM(:is_full_refresh) INTO :res_product;
    CALL {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_SALES_FACT(:is_full_refresh) INTO :res_fact;
    RETURN 'CANONICAL LOAD COMPLETE (Full refresh: ' || :is_full_refresh || '):\n' || res_contract || '\n' || res_customer || '\n' || res_product || '\n' || res_fact;
END;
$$;


-- =====================================================================
-- 2. MASTER CURATED STAGE PROCEDURE
-- =====================================================================
CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_CURATED(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    res_customer VARCHAR;
    res_product  VARCHAR;
    res_summary  VARCHAR;
BEGIN
    CALL {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_CUSTOMER_ANALYTICS(:is_full_refresh) INTO :res_customer;
    CALL {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_PRODUCT_PERFORMANCE(:is_full_refresh) INTO :res_product;
    CALL {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_SALES_SUMMARY(:is_full_refresh) INTO :res_summary;
    RETURN 'CURATED LOAD COMPLETE (Full refresh: ' || :is_full_refresh || '):\n' || res_customer || '\n' || res_product || '\n' || res_summary;
END;
$$;


-- =====================================================================
-- 3. MASTER FULL PIPELINE ORCHESTRATOR
-- =====================================================================
CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA_CANONICAL}.SP_RUN_FULL_TRANSFORMATION(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    res_canonical VARCHAR;
    res_curated   VARCHAR;
BEGIN
    CALL {DATABASE}.{SCHEMA_CANONICAL}.SP_LOAD_CANONICAL(:is_full_refresh) INTO :res_canonical;
    CALL {DATABASE}.{SCHEMA_CURATED}.SP_LOAD_CURATED(:is_full_refresh) INTO :res_curated;
    RETURN '=== FULL TRANSFORMATION PIPELINE COMPLETE (Full refresh: ' || :is_full_refresh || ') ===\n\n[CANONICAL STAGE]\n' || res_canonical || '\n\n[CURATED STAGE]\n' || res_curated;
END;
$$;
