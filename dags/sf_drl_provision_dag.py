"""
sf_drl_provision_dag.py
========================
Apache Airflow DAG for Snowflake Infrastructure Provisioning:
- Orchestrates complete database, schema, external stages, tables, and CDC stream setup.
- Unscheduled (schedule=None): strictly triggered on-demand for provisioning or infrastructure updates.
- Executes in 4 structured sequential stages:
    1. Database & Schemas (sales, sales_raw, sales_canonical, sales_curated)
    2. External Objects (File formats, Storage integrations, Stages)
    3. Target Tables (Generated dynamically from schema/*.yaml DDL definitions)
    4. Native Transformation Objects (Staging views and CDC streams)

Trigger Options (via Airflow UI "Trigger DAG w/ config"):
  - Standard live provisioning:
      {}
  - Dry run preview (prints generated SQL in task logs without running DDL):
      {"dry_run": true}
  - Skip native transformation deployment:
      {"skip_transforms": true}
"""

import os
import sys
from datetime import datetime
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
TEMPLATES_DIR = PROJECT_ROOT / "templates"
SCHEMA_DIR = PROJECT_ROOT / "schema"

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

# Import project utilities and schema orchestrator
from snowflake_utils import get_config, connect_snowflake
from schema_orchestrator import (
    create_schemas,
    setup_external_objects,
    create_tables,
    setup_transforms,
)


# ==========================================================
# Task Execution Callables
# ==========================================================
def task_create_schemas(**context):
    """Step 1: Create Database and Schemas (raw, canonical, curated)."""
    dag_run = context.get("dag_run")
    dry_run = bool(dag_run.conf.get("dry_run", False)) if dag_run and dag_run.conf else False

    cfg = get_config()
    snow_cfg = cfg["snowflake"]
    print(f"🚀 [Step 1/4] Provisioning Database & Schemas (dry_run={dry_run})...")

    if dry_run:
        create_schemas(None, snow_cfg, dry_run=True)
        print("✅ [Dry-Run] Database & Schemas validated.")
        return

    conn = connect_snowflake()
    cur = conn.cursor()
    try:
        create_schemas(cur, snow_cfg, dry_run=False)
        conn.commit()
        print("✅ Database & Schemas created successfully.")
    finally:
        cur.close()
        conn.close()


def task_setup_external_objects(**context):
    """Step 2: Create File Formats, Storage Integrations, and External Stages."""
    dag_run = context.get("dag_run")
    dry_run = bool(dag_run.conf.get("dry_run", False)) if dag_run and dag_run.conf else False

    cfg = get_config()
    snow_cfg = cfg["snowflake"]
    print(f"🚀 [Step 2/4] Setting up External Stage Objects from {TEMPLATES_DIR} (dry_run={dry_run})...")

    if dry_run:
        setup_external_objects(None, snow_cfg, TEMPLATES_DIR, dry_run=True)
        print("✅ [Dry-Run] External stage objects validated.")
        return

    conn = connect_snowflake()
    cur = conn.cursor()
    try:
        cur.execute(f"USE ROLE {snow_cfg['role']}")
        cur.execute(f"USE DATABASE {snow_cfg['database']}")
        cur.execute(f"USE SCHEMA {snow_cfg['schema_raw']}")
        setup_external_objects(cur, snow_cfg, TEMPLATES_DIR, dry_run=False)
        conn.commit()
        print("✅ External stage objects created successfully.")
    finally:
        cur.close()
        conn.close()


def task_create_tables(**context):
    """Step 3: Parse YAML table definitions and execute CREATE TABLE IF NOT EXISTS."""
    dag_run = context.get("dag_run")
    dry_run = bool(dag_run.conf.get("dry_run", False)) if dag_run and dag_run.conf else False

    cfg = get_config()
    snow_cfg = cfg["snowflake"]
    print(f"🚀 [Step 3/4] Creating Target Tables from {SCHEMA_DIR} (dry_run={dry_run})...")

    if dry_run:
        create_tables(None, snow_cfg, SCHEMA_DIR, dry_run=True)
        print("✅ [Dry-Run] Target tables DDL validated.")
        return

    conn = connect_snowflake()
    cur = conn.cursor()
    try:
        cur.execute(f"USE ROLE {snow_cfg['role']}")
        cur.execute(f"USE DATABASE {snow_cfg['database']}")
        create_tables(cur, snow_cfg, SCHEMA_DIR, dry_run=False)
        conn.commit()
        print("✅ Target tables created successfully.")
    finally:
        cur.close()
        conn.close()


def task_setup_transforms(**context):
    """Step 4: Deploy Snowflake Native Transformation Objects (Staging Views & CDC Streams)."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    dry_run = bool(conf.get("dry_run", False))
    skip_transforms = bool(conf.get("skip_transforms", False))

    if skip_transforms:
        print("⏭️ Skipping native transformation objects deployment (skip_transforms=True).")
        return

    cfg = get_config()
    snow_cfg = cfg["snowflake"]
    print(f"🚀 [Step 4/4] Setting up Native Transformation Views & CDC Streams (dry_run={dry_run})...")

    if dry_run:
        setup_transforms(None, cfg, dry_run=True)
        print("✅ [Dry-Run] Native transformation objects validated.")
        return

    conn = connect_snowflake()
    cur = conn.cursor()
    try:
        cur.execute(f"USE ROLE {snow_cfg['role']}")
        cur.execute(f"USE DATABASE {snow_cfg['database']}")
        setup_transforms(cur, cfg, dry_run=False)
        conn.commit()
        print("✅ Native transformation views and CDC streams deployed successfully.")
    finally:
        cur.close()
        conn.close()


# ==========================================================
# DAG Definition (Unscheduled Provisioning)
# ==========================================================
default_args = {
    "owner": "antigravity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="sf_drl_schema_provisioning",
    default_args=default_args,
    description="On-Demand Provisioning: Snowflake Database, Schemas, Stages, Tables & CDC Streams",
    schedule=None,  # No schedule, run manually on demand
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "provisioning", "schema", "ddl"],
    doc_md=__doc__,
) as dag:

    create_database_schemas = PythonOperator(
        task_id="1_create_database_and_schemas",
        python_callable=task_create_schemas,
    )

    setup_external_stage = PythonOperator(
        task_id="2_setup_external_stage_objects",
        python_callable=task_setup_external_objects,
    )

    create_target_tables = PythonOperator(
        task_id="3_create_target_tables",
        python_callable=task_create_tables,
    )

    setup_transformation_views_streams = PythonOperator(
        task_id="4_setup_transformation_objects",
        python_callable=task_setup_transforms,
    )

    # Sequential execution order for provisioning
    (
        create_database_schemas
        >> setup_external_stage
        >> create_target_tables
        >> setup_transformation_views_streams
    )
