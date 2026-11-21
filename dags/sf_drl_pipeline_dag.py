"""
sf_drl_pipeline_dag.py
======================
Apache Airflow DAG for the SF_DRL Data Warehouse:
- Automatically schedules incremental CDC loads every 6 hours (00:00, 06:00, 12:00, 18:00 UTC).
- Dynamically discovers all models and dependencies from transform/ and templates/.
- Provides full monitoring, task-level logs, retries, and run history in the Airflow UI.
- Supports on-demand full refresh via DAG Configuration: {"full_refresh": true}.

Architecture:
  Stage 1: Raw Ingestion (S3 -> Snowflake Raw Tables via COPY INTO)
  Stage 2: Canonical Transformations (SCD Type 1 & 2 Dimensions, Sales Fact)
  Stage 3: Curated Analytics (Customer Metrics, Product Performance, Daily KPI Summary)
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root and scripts to sys.path so modules are discoverable
DAGS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(os.environ.get("SF_DRL_ROOT", DAGS_DIR.parent))
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Import project utilities and engines
from snowflake_utils import get_config, connect_snowflake
from snowflake_transforms import discover_models, execute_model
from copy_and_load import run_raw_copy_load, load_json, COPY_MANIFEST_PATH


# ==========================================================
# Task Execution Callables
# ==========================================================
def execute_raw_task(table_key: str, **context):
    """Executes COPY INTO for a single raw table from S3."""
    print(f"🚀 Ingesting raw table key: {table_key} from S3 landing...")
    run_raw_copy_load(selected_table=table_key, dry_run=False)
    print(f"✅ Ingested raw table key: {table_key}")


def execute_model_task(model_table: str, stage_name: str, **context):
    """
    Executes a single transformation model (canonical or curated).
    Evaluates CDC streams incrementally; if {"full_refresh": true} is passed,
    truncates target, reloads from base table, and resets stream offsets.
    """
    dag_run = context.get("dag_run")
    full_refresh = False
    if dag_run and dag_run.conf:
        full_refresh = bool(dag_run.conf.get("full_refresh", False))

    print(f"🚀 Executing model: {model_table} [Stage: {stage_name}] (full_refresh={full_refresh})...")
    cfg = get_config()
    models = discover_models(stage=stage_name, cfg=cfg)
    target_model = next((m for m in models if m["table"] == model_table), None)

    if not target_model:
        raise ValueError(f"Model {model_table} not found in stage {stage_name}.")

    conn = connect_snowflake()
    cur = conn.cursor()
    try:
        cur.execute(f"USE ROLE {cfg['snowflake']['role']}")
        cur.execute(f"USE DATABASE {cfg['snowflake']['database']}")
        cur.execute(f"USE WAREHOUSE {cfg['snowflake']['warehouse']}")

        result = execute_model(cur, target_model, cfg, full_refresh=full_refresh, dry_run=False)
        conn.commit()
        print(f"✅ Finished: {result}")
    finally:
        cur.close()
        conn.close()


# ==========================================================
# DAG Definition
# ==========================================================
default_args = {
    "owner": "antigravity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sf_drl_incremental_pipeline",
    default_args=default_args,
    description="6-Hour Incremental Stream CDC Pipeline (Raw -> Canonical -> Curated)",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "cdc", "drl", "stream"],
    doc_md=__doc__,
) as dag:

    # --------------------------------------------------------
    # Stage 1: Raw Ingestion Task Group
    # --------------------------------------------------------
    raw_tasks = {}
    with TaskGroup("stage_1_raw_ingestion", tooltip="Ingest S3 parquet files into Snowflake raw tables") as tg_raw:
        manifest = load_json(COPY_MANIFEST_PATH) if COPY_MANIFEST_PATH.exists() else {"tables": {}}
        for tbl_key in manifest.get("tables", {}).keys():
            raw_tasks[tbl_key] = PythonOperator(
                task_id=f"copy_{tbl_key}",
                python_callable=execute_raw_task,
                op_kwargs={"table_key": tbl_key},
            )

    # --------------------------------------------------------
    # Stage 2: Canonical Transformations Task Group
    # --------------------------------------------------------
    canonical_tasks = {}
    cfg = get_config()
    canonical_models = discover_models(stage="canonical", cfg=cfg)

    with TaskGroup("stage_2_canonical_transforms", tooltip="SCD Type 1 & 2 Canonical Dimensions & Facts") as tg_canonical:
        for model in canonical_models:
            tbl = model["table"]
            canonical_tasks[tbl] = PythonOperator(
                task_id=f"model_{tbl.lower()}",
                python_callable=execute_model_task,
                op_kwargs={"model_table": tbl, "stage_name": "canonical"},
            )

        # Wire dependencies within canonical layer from discover_models()
        for model in canonical_models:
            tbl = model["table"]
            for up in model.get("depends_on", []):
                if up in canonical_tasks:
                    canonical_tasks[up] >> canonical_tasks[tbl]

    # Map raw ingestion sources to root canonical models
    # contract -> CONTRACT_DIM, customer -> CUSTOMER_DIM, product -> PRODUCT_DIM
    if "contract" in raw_tasks and "CONTRACT_DIM" in canonical_tasks:
        raw_tasks["contract"] >> canonical_tasks["CONTRACT_DIM"]
    if "customer" in raw_tasks and "CUSTOMER_DIM" in canonical_tasks:
        raw_tasks["customer"] >> canonical_tasks["CUSTOMER_DIM"]
    if "product" in raw_tasks and "PRODUCT_DIM" in canonical_tasks:
        raw_tasks["product"] >> canonical_tasks["PRODUCT_DIM"]
    if "dir_sale_lines" in raw_tasks and "SALES_FACT" in canonical_tasks:
        raw_tasks["dir_sale_lines"] >> canonical_tasks["SALES_FACT"]
    if "indir_sale_lines" in raw_tasks and "SALES_FACT" in canonical_tasks:
        raw_tasks["indir_sale_lines"] >> canonical_tasks["SALES_FACT"]

    # --------------------------------------------------------
    # Stage 3: Curated Analytics Task Group
    # --------------------------------------------------------
    curated_tasks = {}
    curated_models = discover_models(stage="curated", cfg=cfg)

    with TaskGroup("stage_3_curated_analytics", tooltip="Curated Aggregations & Daily Summaries") as tg_curated:
        for model in curated_models:
            tbl = model["table"]
            curated_tasks[tbl] = PythonOperator(
                task_id=f"model_{tbl.lower()}",
                python_callable=execute_model_task,
                op_kwargs={"model_table": tbl, "stage_name": "curated"},
            )

        # Wire dependencies within curated layer
        for model in curated_models:
            tbl = model["table"]
            for up in model.get("depends_on", []):
                if up in curated_tasks:
                    curated_tasks[up] >> curated_tasks[tbl]

    # Wire cross-stage dependencies (Curated models depend on their canonical upstreams)
    for model in curated_models:
        tbl = model["table"]
        for up in model.get("depends_on", []):
            if up in canonical_tasks:
                canonical_tasks[up] >> curated_tasks[tbl]
