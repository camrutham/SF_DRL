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

# Resolve project root dynamically across both Host OS and Docker container mounts
DAGS_DIR = Path(__file__).resolve().parent

candidate_roots = [
    Path(os.environ.get("SF_DRL_ROOT", "")),
    Path("/repos/SF_DRL"),
    DAGS_DIR.parent,
    Path("C:/repos/SF_DRL"),
]

PROJECT_ROOT = DAGS_DIR.parent
for cand in candidate_roots:
    if cand and (cand / "scripts" / "snowflake_utils.py").exists():
        PROJECT_ROOT = cand
        break

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import Airflow modules (supports Airflow 2.x and Airflow 3.x)
from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

try:
    from airflow.sdk import TaskGroup
except ImportError:
    try:
        from airflow.utils.task_group import TaskGroup
    except ImportError:
        TaskGroup = None

try:
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.exceptions import AirflowSkipException

# Import project utilities and engines
from snowflake_utils import get_config, connect_snowflake
from snowflake_transforms import discover_models, execute_model
from copy_and_load import run_raw_copy_load, load_json, COPY_MANIFEST_PATH


# ==========================================================
# Filtering Logic for Selective Execution
# ==========================================================
def should_run_stage_and_table(stage_name: str, table_name: str, conf: dict) -> bool:
    """
    Evaluates whether a task should run based on configuration filters.
    Supports:
      - 'stages': list of stages (e.g. ['raw', 'canonical'], ['curated'])
      - 'stage': single stage string shorthand (e.g. 'canonical')
      - 'tables': list of tables/models (e.g. ['SALES_FACT'], ['contract', 'CONTRACT_DIM'])
      - 'table': single table string shorthand (e.g. 'SALES_FACT')
    """
    if not conf:
        return True

    # 1. Evaluate stage filter
    selected_stages = conf.get("stages")
    if selected_stages is None and "stage" in conf:
        selected_stages = [conf["stage"]]

    if selected_stages:
        stages_lower = [str(s).strip().lower() for s in selected_stages]
        if stage_name.lower() not in stages_lower and not (stage_name.lower() == "raw" and "s3" in stages_lower):
            return False

    # 2. Evaluate table filter
    selected_tables = conf.get("tables")
    if selected_tables is None and "table" in conf:
        selected_tables = [conf["table"]]

    if selected_tables:
        tables_upper = {str(t).strip().upper() for t in selected_tables}
        candidates = {
            table_name.upper(),
            table_name.lower(),
            table_name.replace("_", "").upper(),
        }
        if stage_name.lower() == "raw":
            candidates.add(f"{table_name.upper()}_RAW")
            candidates.add(table_name.upper().replace("_RAW", ""))

        if not any(c in tables_upper for c in candidates):
            return False

    return True


# ==========================================================
# Task Execution Callables
# ==========================================================
def execute_raw_task(table_key: str, **context):
    """Executes COPY INTO for a single raw table from S3."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    if not should_run_stage_and_table("raw", table_key, conf):
        print(f"⏭️ Skipping raw table '{table_key}': excluded by configuration filters.")
        raise AirflowSkipException(f"Table '{table_key}' excluded by run config.")

    dry_run = bool(conf.get("dry_run", False))
    print(f"🚀 Ingesting raw table key: {table_key} from S3 landing (dry_run={dry_run})...")
    run_raw_copy_load(selected_table=table_key, dry_run=dry_run)
    print(f"✅ Ingested raw table key: {table_key}")


def execute_model_task(model_table: str, stage_name: str, **context):
    """
    Executes a single transformation model (canonical or curated).
    Evaluates CDC streams incrementally; if {"full_refresh": true} is passed,
    truncates target, reloads from base table, and resets stream offsets.
    Supports {"dry_run": true} for safe validation.
    """
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    if not should_run_stage_and_table(stage_name, model_table, conf):
        print(f"⏭️ Skipping {stage_name}.{model_table}: excluded by configuration filters.")
        raise AirflowSkipException(f"Model '{model_table}' excluded by run config.")

    full_refresh = bool(conf.get("full_refresh", False))
    dry_run = bool(conf.get("dry_run", False))

    print(f"🚀 Executing model: {model_table} [Stage: {stage_name}] (full_refresh={full_refresh}, dry_run={dry_run})...")
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

        result = execute_model(cur, target_model, cfg, full_refresh=full_refresh, dry_run=dry_run)
        if not dry_run:
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
    "trigger_rule": "none_failed",  # Allows downstream tasks to run when upstreams are selectively skipped
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

    # Dynamically wire raw ingestion sources to canonical models based on SQL lineage
    raw_table_to_task = {}
    for tbl_key, info in manifest.get("tables", {}).items():
        if tbl_key in raw_tasks:
            raw_table_to_task[info.get("table", "").upper()] = raw_tasks[tbl_key]
            raw_table_to_task[tbl_key.upper()] = raw_tasks[tbl_key]

    for model in canonical_models:
        tbl = model["table"]
        if tbl in canonical_tasks:
            for raw_ref in model.get("reads", set()):
                if raw_ref.upper() in raw_table_to_task:
                    raw_table_to_task[raw_ref.upper()] >> canonical_tasks[tbl]

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
