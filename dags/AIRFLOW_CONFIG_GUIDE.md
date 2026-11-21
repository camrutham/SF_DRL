# Apache Airflow DAG Configuration Guide

This guide details all available configuration JSONs across all three DAGs in the `SF_DRL` data warehouse project. When manually triggering runs in the Airflow UI (**http://localhost:8088/**), click **Trigger DAG w/ config** and paste the desired JSON.

---

## Architecture: 3 Dedicated Purpose-Built DAGs

| DAG ID | Schedule | Default Mode | Purpose |
| :--- | :--- | :--- | :--- |
| **`sf_drl_incremental_pipeline`** | `0 */6 * * *` (Every 6h) | Incremental CDC (`full_refresh: false`) | Automated periodic micro-batch CDC loads using Snowflake Streams. |
| **`sf_drl_full_load_pipeline`** | `None` (On-Demand) | Full Refresh (`full_refresh: true`) | Manual bulk reloads, layer-specific runs, and single-table backfills. |
| **`sf_drl_schema_provisioning`** | `None` (On-Demand) | DDL Provisioning | Creation of DB, schemas, external stages, tables, and CDC streams. |

---

## 1. Full Load & Selective Backfill DAG (`sf_drl_full_load_pipeline`)

File: `dags/sf_drl_full_load_dag.py`  
Schedule: `None` (Strictly On-Demand)  
Default: Truncates target tables, re-reads base data, and resets stream offsets.

### Ready-to-Use Config JSONs

#### Scenario 1.1: Complete Warehouse Full Load (Default)
Reloads every layer from scratch: S3 to Raw -> Raw to Canonical -> Canonical to Curated.
```json
{}
```

#### Scenario 1.2: S3 to Raw Only
Runs only the S3 ingestion tasks (`copy_contract`, `copy_customer`, `copy_product`, `copy_dir_sale_lines`, `copy_indir_sale_lines`). Canonical and curated tasks are automatically skipped.
```json
{
  "stages": ["raw"]
}
```

#### Scenario 1.3: Raw to Canonical Only
Runs only the canonical dimension and fact models (`CONTRACT_DIM`, `CUSTOMER_DIM`, `PRODUCT_DIM`, `SALES_FACT`). S3 ingestion and curated tasks are skipped.
```json
{
  "stages": ["canonical"]
}
```

#### Scenario 1.4: S3 to Raw AND Raw to Canonical (Skip Curated)
Ingests S3 files and transforms raw data into canonical tables, stopping before the curated layer.
```json
{
  "stages": ["raw", "canonical"]
}
```

#### Scenario 1.5: Curated Tables Only
Runs only the curated analytics models (`CUSTOMER_ANALYTICS`, `PRODUCT_PERFORMANCE`, `SALES_SUMMARY`). Upstream canonical tables are read directly.
```json
{
  "stages": ["curated"]
}
```

#### Scenario 1.6: Single Individual Pipeline / Table (e.g., `SALES_FACT`)
Runs ONLY the `SALES_FACT` transformation model. All other tasks in raw, canonical, and curated are skipped.
```json
{
  "tables": ["SALES_FACT"]
}
```

#### Scenario 1.7: Multiple Specific Tables Across Layers
Runs only selected tables (for example, raw `contract` and canonical `CONTRACT_DIM`).
```json
{
  "tables": ["contract", "CONTRACT_DIM"]
}
```

#### Scenario 1.8: Dry-Run Simulation (Any Scenario)
Add `"dry_run": true` to preview execution plans, check table targets, and print SQL without executing DML in Snowflake:
```json
{
  "stages": ["canonical"],
  "dry_run": true
}
```

---

## 2. Incremental CDC Pipeline (`sf_drl_incremental_pipeline`)

File: `dags/sf_drl_pipeline_dag.py`  
Schedule: `0 */6 * * *` (Every 6 hours)  
Default: Incremental stream CDC processing (`SYSTEM$STREAM_HAS_DATA`).

### Ready-to-Use Config JSONs

#### Scenario 2.1: Automated / Default Incremental Run
Processes new data recorded by CDC streams. If a stream has 0 changes, execution completes with 0 warehouse cost.
```json
{}
```

#### Scenario 2.2: Incremental Run for Specific Stage
Runs incremental CDC processing only for canonical models:
```json
{
  "stages": ["canonical"]
}
```

#### Scenario 2.3: Incremental Run for Single Table
Evaluates the stream for only `SALES_FACT`:
```json
{
  "tables": ["SALES_FACT"]
}
```

#### Scenario 2.4: Dry-Run Incremental Simulation
Verifies stream statuses and logs generated DML without executing:
```json
{
  "dry_run": true
}
```

---

## 3. Schema & Infrastructure Provisioning (`sf_drl_schema_provisioning`)

File: `dags/sf_drl_provision_dag.py`  
Schedule: `None` (Strictly On-Demand)  
Default: Sequentially creates DB, schemas, external stages, YAML tables, and CDC streams.

### Ready-to-Use Config JSONs

#### Scenario 3.1: Complete Live Provisioning (Default)
Provisions database, schemas, file formats, stages, tables, and CDC streams.
```json
{}
```

#### Scenario 3.2: Dry-Run Validation / Audit
Generates and logs all DDL statements in the Airflow task logs **without modifying Snowflake**:
```json
{
  "dry_run": true
}
```

#### Scenario 3.3: Database, Stages & Tables Only (Skip Transforms)
Creates the database, schemas, external stages, and tables from YAML, skipping views and streams.
```json
{
  "skip_transforms": true
}
```

#### Scenario 3.4: Native Views & CDC Streams Only
Skips database, stages, and tables. Deploys or refreshes only staging views and CDC streams.
```json
{
  "transforms_only": true
}
```

---

## Parameter Reference Matrix

| Parameter | Accepted Values | Target DAGs | Description |
| :--- | :--- | :--- | :--- |
| `stages` / `stage` | `["raw"]`, `["canonical"]`, `["curated"]`, `["raw", "canonical"]` | Full Load, Incremental | Selectively filters which stage(s) to execute. |
| `tables` / `table` | e.g. `["SALES_FACT"]`, `["contract", "CONTRACT_DIM"]` | Full Load, Incremental | Selectively runs specific individual table(s) or model(s). |
| `full_refresh` | `true` or `false` | Full Load (defaults to `true`), Incremental (defaults to `false`) | Determines whether to truncate & reload from base tables or use CDC streams. |
| `dry_run` | `true` or `false` | All DAGs | Prints execution plan and SQL in task logs without running DDL/DML. |
| `skip_transforms` | `true` or `false` | Provisioning DAG | Skips deployment of staging views and CDC streams. |
| `transforms_only` | `true` or `false` | Provisioning DAG | Deploys only staging views and CDC streams, skipping tables and stages. |
