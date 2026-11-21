# Apache Airflow DAG Configuration Guide

This guide provides ready-to-use configuration JSONs for both DAGs in the `SF_DRL` data warehouse project. When manually triggering runs in the Airflow UI (**http://localhost:8088/**), click **Trigger DAG w/ config** and paste the desired JSON.

---

## 1. Incremental Pipeline DAG (`sf_drl_incremental_pipeline`)

File: `dags/sf_drl_pipeline_dag.py`  
Schedule: `0 */6 * * *` (Every 6 hours)  
Purpose: Ingest S3 Parquet files via `COPY INTO` and process transformation models using Snowflake Change Data Capture (CDC) Streams.

### Configuration Scenarios

#### Scenario 1.1: Standard Incremental Run (Default)
Executes normal 6-hour incremental load. Reads changes recorded in CDC Streams (`SYSTEM$STREAM_HAS_DATA`). If no new data arrived, streams are skipped with 0 warehouse compute cost.
```json
{}
```

#### Scenario 1.2: Full Refresh / Reprocessing
Truncates canonical and curated target tables, re-reads all records directly from base tables (bypassing streams), and resets CDC stream offsets to align with the base tables.
```json
{
  "full_refresh": true
}
```

#### Scenario 1.3: Dry-Run Incremental Simulation
Validates connectivity, tests `SYSTEM$STREAM_HAS_DATA()`, and prints the generated SQL statements in task logs without running `COPY INTO` or modifying any Snowflake data.
```json
{
  "dry_run": true
}
```

#### Scenario 1.4: Dry-Run Full Refresh Simulation
Simulates full refresh logic and prints table truncation commands and base-table DML without actually altering tables.
```json
{
  "full_refresh": true,
  "dry_run": true
}
```

---

## 2. Schema Provisioning DAG (`sf_drl_schema_provisioning`)

File: `dags/sf_drl_provision_dag.py`  
Schedule: `None` (Strictly On-Demand)  
Purpose: Provision Snowflake infrastructure, stages, storage integrations, table DDL, and CDC streams.

### Configuration Scenarios

#### Scenario 2.1: Full Live Provisioning (Default)
Runs all 4 stages sequentially:
1. Creates Database & Schemas (`sales`, `sales_raw`, `sales_canonical`, `sales_curated`).
2. Creates File Formats, Storage Integrations, and External Stages from `templates/*.csv`.
3. Creates Target Tables from `schema/**/*.yaml` (`CREATE TABLE IF NOT EXISTS`).
4. Creates Snowflake staging views and CDC streams from `transform/` hierarchy.
```json
{}
```

#### Scenario 2.2: Dry-Run Validation / Audit
Generates and prints all SQL (database creation, external stages, YAML table schemas, and CDC stream DDL) in the Airflow task execution logs **without executing any DDL in Snowflake**. Ideal for code reviews and syntax validation.
```json
{
  "dry_run": true
}
```

#### Scenario 2.3: Database, Stages & Tables Only (Skip Transforms)
Provisions the database, external stages, and all target tables from YAML, but skips deploying staging views and CDC streams.
```json
{
  "skip_transforms": true
}
```

#### Scenario 2.4: Views & CDC Streams Only
Skips database, stage, and table DDL. Only deploys or refreshes Snowflake native staging views and CDC streams. Useful when modifying model SQL or stream definitions.
```json
{
  "transforms_only": true
}
```

#### Scenario 2.5: Views & CDC Streams Dry-Run Preview
Simulates deploying views and streams only, printing their DDL in logs without executing.
```json
{
  "transforms_only": true,
  "dry_run": true
}
```

---

## Summary Matrix

| DAG ID | Config Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `sf_drl_incremental_pipeline` | `full_refresh` | `boolean` | `false` | When `true`, reloads all data from base tables and resets CDC stream offsets. |
| `sf_drl_incremental_pipeline` | `dry_run` | `boolean` | `false` | When `true`, prints execution plans and SQL without running DML. |
| `sf_drl_schema_provisioning` | `dry_run` | `boolean` | `false` | When `true`, prints all generated DDL in Airflow logs without altering Snowflake. |
| `sf_drl_schema_provisioning` | `skip_transforms` | `boolean` | `false` | When `true`, skips deploying views and CDC streams. |
| `sf_drl_schema_provisioning` | `transforms_only` | `boolean` | `false` | When `true`, only deploys views and CDC streams (skips db/stages/tables). |

---

## How to Apply in Airflow UI

1. Go to **http://localhost:8088/**.
2. Locate the DAG (`sf_drl_incremental_pipeline` or `sf_drl_schema_provisioning`).
3. Click the dropdown arrow on the **Trigger DAG** button (or click the play icon) and select **Trigger DAG w/ config**.
4. In the configuration text box, paste the JSON snippet.
5. Click **Trigger**.
