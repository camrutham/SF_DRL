# SF_DRL: Snowflake Native Data Pipeline

A 100% **Snowflake Native** end-to-end data ingestion, transformation, and orchestration pipeline. This project replaces external transformation frameworks (such as dbt) entirely with Snowflake-native architectural primitives: **Snowflake Views**, **Native SQL MERGE statements**, and **Change Data Capture (CDC) Streams**.

Transformations are organized under the [`transform/`](file:///c:/repos/SF_DRL/transform) directory, exactly mirroring the structure and submodules of the [`schema/`](file:///c:/repos/SF_DRL/schema) directory.

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph Landing["S3 Landing Stages"]
        S3_Contract["s3://.../contract/"]
        S3_Customer["s3://.../customer/"]
        S3_Product["s3://.../product/"]
        S3_Direct["s3://.../direct_sale_lines/"]
        S3_Indirect["s3://.../indirect_sale_lines/"]
    end

    subgraph Raw["Raw Layer (sales_raw)"]
        CONTRACT_RAW --> CONTRACT_RAW_STREAM
        CUSTOMER_RAW --> CUSTOMER_RAW_STREAM
        PRODUCT_RAW --> PRODUCT_RAW_STREAM
        DIRECT_SALE_LINES_RAW --> DIRECT_SALE_STREAM[DIRECT_SALE_LINES_RAW_STREAM]
        INDIRECT_SALE_LINES_RAW --> INDIRECT_SALE_STREAM[INDIRECT_SALE_LINES_RAW_STREAM]
    end

    subgraph Canonical["Canonical Layer (sales_canonical)"]
        CONTRACT_RAW_STREAM -->|Stream CDC MERGE| CONTRACT_DIM["CONTRACT_DIM (SCD Type 2)"]
        CUSTOMER_RAW_STREAM -->|Stream CDC MERGE| CUSTOMER_DIM["CUSTOMER_DIM (SCD Type 2)"]
        PRODUCT_RAW_STREAM -->|Stream CDC MERGE| PRODUCT_DIM["PRODUCT_DIM (SCD Type 2)"]
        DIRECT_SALE_STREAM & INDIRECT_SALE_STREAM -->|Stream CDC MERGE| SALES_FACT["SALES_FACT (Consolidated Facts)"]
        
        CUSTOMER_DIM --> CUSTOMER_DIM_STREAM
        PRODUCT_DIM --> PRODUCT_DIM_STREAM
        SALES_FACT --> SALES_FACT_STREAM_SUMMARY
        SALES_FACT --> SALES_FACT_STREAM_CUSTOMER
        SALES_FACT --> SALES_FACT_STREAM_PRODUCT
    end

    subgraph Curated["Curated Layer (sales_curated)"]
        CUSTOMER_DIM_STREAM & SALES_FACT_STREAM_CUSTOMER -->|Incremental Aggregation| CUSTOMER_ANALYTICS["CUSTOMER_ANALYTICS"]
        PRODUCT_DIM_STREAM & SALES_FACT_STREAM_PRODUCT -->|Incremental Aggregation| PRODUCT_PERFORMANCE["PRODUCT_PERFORMANCE"]
        SALES_FACT_STREAM_SUMMARY -->|Incremental Aggregation| SALES_SUMMARY["SALES_SUMMARY"]
    end

    Landing -->|COPY INTO| Raw
```

---

## Structural Alignment: `schema/` and `transform/`

The project maintains an exact 1:1 alignment between table DDL schemas and transformation SQL files across stages and submodules:

| Layer / Stage | Submodule | Schema Definition (DDL) | Transformation SQL (Single Source of Truth) | Target Snowflake Object |
| :--- | :--- | :--- | :--- | :--- |
| **Raw** | `masterdata` | `schema/raw/masterdata/contract_raw.yaml` | `transform/raw/masterdata/streams.sql` | `sales_raw.CONTRACT_RAW` & CDC Stream |
| | `masterdata` | `schema/raw/masterdata/customer_raw.yaml` | `transform/raw/masterdata/streams.sql` | `sales_raw.CUSTOMER_RAW` & CDC Stream |
| | `masterdata` | `schema/raw/masterdata/product_raw.yaml` | `transform/raw/masterdata/streams.sql` | `sales_raw.PRODUCT_RAW` & CDC Stream |
| | `sales` | `schema/raw/sales/direct_sale_lines_raw.yaml` | `transform/raw/sales/stg_direct_sales.sql`<br>`transform/raw/sales/streams.sql` | `sales_raw.DIRECT_SALE_LINES_RAW`<br>`sales_raw.STG_DIRECT_SALES` (View) |
| | `sales` | `schema/raw/sales/indirect_sale_lines_raw.yaml` | `transform/raw/sales/stg_indirect_sales.sql`<br>`transform/raw/sales/streams.sql` | `sales_raw.INDIRECT_SALE_LINES_RAW`<br>`sales_raw.STG_INDIRECT_SALES` (View) |
| **Canonical** | `masterdata` | `schema/canonical/masterdata/contract.yaml` | `transform/canonical/masterdata/contract_dim.sql` | `sales_canonical.CONTRACT_DIM` |
| | `masterdata` | `schema/canonical/masterdata/customer_dim.yaml` | `transform/canonical/masterdata/customer_dim.sql`<br>`transform/canonical/masterdata/streams.sql` | `sales_canonical.CUSTOMER_DIM` & CDC Stream |
| | `masterdata` | `schema/canonical/masterdata/product_dim.yaml` | `transform/canonical/masterdata/product_dim.sql`<br>`transform/canonical/masterdata/streams.sql` | `sales_canonical.PRODUCT_DIM` & CDC Stream |
| | `sales` | `schema/canonical/sales/sales_fact.yaml` | `transform/canonical/sales/sales_fact.sql`<br>`transform/canonical/sales/streams.sql`<br>`transform/canonical/sales/sales_fact_direct.sql`<br>`transform/canonical/sales/sales_fact_indirect.sql` | `sales_canonical.SALES_FACT`<br>CDC Streams & Projections: `SALES_FACT_DIRECT`, `SALES_FACT_INDIRECT` |
| **Curated** | *(root)* | `schema/curated/customer_analytics.yaml` | `transform/curated/customer_analytics.sql` | `sales_curated.CUSTOMER_ANALYTICS` |
| | *(root)* | `schema/curated/product_performance.yaml` | `transform/curated/product_performance.sql` | `sales_curated.PRODUCT_PERFORMANCE` |
| | *(root)* | `schema/curated/sales_summary.yaml` | `transform/curated/sales_summary.sql` | `sales_curated.SALES_SUMMARY` |

---

## Directory Tree

```
SF_DRL/
├── README.md                           # Complete architecture guide & command reference
├── schema/                             # YAML Table Schemas (DDL source of truth)
│   ├── raw/                            # Raw layer schemas
│   │   ├── masterdata/                 # contract_raw.yaml, customer_raw.yaml, product_raw.yaml
│   │   └── sales/                      # direct_sale_lines_raw.yaml, indirect_sale_lines_raw.yaml
│   ├── canonical/                      # Canonical layer schemas
│   │   ├── masterdata/                 # contract.yaml, customer_dim.yaml, product_dim.yaml
│   │   └── sales/                      # sales_fact.yaml
│   └── curated/                        # Curated analytical schemas
│       ├── customer_analytics.yaml
│       ├── product_performance.yaml
│       └── sales_summary.yaml
├── transform/                          # Snowflake Native Transformation SQL Scripts
│   ├── raw/                            # Raw layer views and CDC streams
│   │   ├── masterdata/                 # streams.sql
│   │   └── sales/                      # stg_direct_sales.sql, stg_indirect_sales.sql, streams.sql
│   ├── canonical/                      # Canonical layer transformations & CDC streams
│   │   ├── masterdata/                 # contract_dim.sql, customer_dim.sql, product_dim.sql, streams.sql
│   │   └── sales/                      # sales_fact.sql, streams.sql, sales_fact_direct.sql, sales_fact_indirect.sql
│   └── curated/                        # Curated layer aggregations
│       ├── customer_analytics.sql
│       ├── product_performance.sql
│       └── sales_summary.sql
├── scripts/
│   ├── schema_orchestrator.py          # Complete environment orchestrator
│   ├── snowflake_transforms.py         # Direct Native Transformation Engine & View Deployer
│   ├── copy_and_load.py                # Unified Copy Load + Direct Native Transformation Pipeline
│   ├── ingest_data.py                  # Pipeline runner (raw -> canonical -> curated)
│   ├── copy_data.py                    # Stage & Raw copy load logic
│   ├── ddl_generator.py                # YAML to DDL SQL generator
│   ├── snowflake_utils.py              # Snowflake connector & query helpers
│   ├── config.json                     # Unified configuration
│   └── copy_data_manifest.json         # S3 landing to raw table mappings
└── templates/                          # CSV templates for storage integrations, stages, file formats
```

---

## Quickstart & Usage

### 1. Deploy the Complete Infrastructure & Native Objects
Runs database creation, schemas, external stages, tables from YAML, staging views, and CDC streams:
```bash
python scripts/schema_orchestrator.py
```
*(To preview without executing: `python scripts/schema_orchestrator.py --dry-run`)*

### 2. Run the Full Ingestion & Transformation Pipeline
Executes `COPY INTO` from S3 into raw tables, then executes canonical and curated transformations directly from the SQL files:
```bash
# Run full pipeline (Raw COPY load + Canonical + Curated):
python scripts/copy_and_load.py

# With full refresh (truncate targets & reload):
python scripts/copy_and_load.py --full-refresh

# Or via ingest_data.py:
python scripts/ingest_data.py

# Run transformations only (Canonical + Curated):
python scripts/copy_and_load.py --stage transforms

# Ingest raw data only:
python scripts/copy_and_load.py --stage raw
```

### 3. Standalone Transformation Management
```bash
# Deploy/update views and CDC streams in Snowflake:
python scripts/snowflake_transforms.py --deploy

# Execute all transformations directly (incremental):
python scripts/snowflake_transforms.py --stage all

# Execute all transformations with full refresh (truncate & reload):
python scripts/snowflake_transforms.py --stage all --full-refresh

# Execute a specific stage:
python scripts/snowflake_transforms.py --stage canonical
python scripts/snowflake_transforms.py --stage curated
```
