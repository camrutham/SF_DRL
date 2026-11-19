#!/usr/bin/env python3
"""
snowflake_transforms.py
=======================
Snowflake Native Transformation Engine & Orchestrator:
Executes transformations organized under the `transform/` directory:
  - Models (Pure DML):
      transform/canonical/masterdata/ & transform/canonical/sales/
      transform/curated/
  - Orchestration (Stored Procedures, Tasks, Dynamic Tables):
      transform/orchestration/

Features:
  - Incremental Load (default): Processes delta based on high watermark timestamps.
  - Full Refresh (--full-refresh): Truncates target tables and reloads all records from scratch.

Usage:
  python scripts/snowflake_transforms.py --deploy               # Deploy all views, procs, streams, tasks
  python scripts/snowflake_transforms.py --stage all            # Execute all transformations (incremental)
  python scripts/snowflake_transforms.py --stage all --full-refresh # Full refresh
  python scripts/snowflake_transforms.py --stage canonical      # Execute canonical stage only
  python scripts/snowflake_transforms.py --stage curated        # Execute curated stage only
  python scripts/snowflake_transforms.py --dry-run              # Preview SQL / calls without executing
"""

import argparse
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TRANSFORM_DIR = PROJECT_ROOT / "transform"
LOG_DIR = PROJECT_ROOT / "logs"

from snowflake_utils import connect_snowflake, get_config


def setup_logging(label: str = "transforms") -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{label}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logger = logging.getLogger()
    for h in list(logger.handlers):
        logger.removeHandler(h)
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return log_file


def format_sql(raw_sql: str, cfg: dict) -> str:
    snow_cfg = cfg["snowflake"]
    return (
        raw_sql.replace("{DATABASE}", snow_cfg["database"])
        .replace("{SCHEMA_RAW}", snow_cfg["schema_raw"])
        .replace("{SCHEMA_CANONICAL}", snow_cfg["schema_canonical"])
        .replace("{SCHEMA_CURATED}", snow_cfg["schema_curated"])
        .replace("{WAREHOUSE}", snow_cfg.get("warehouse", "COMPUTE_WH"))
    )


def extract_statements(sql_text: str) -> List[str]:
    """Split SQL file into executable statements handling stored procedure $$ blocks and comments."""
    statements = []
    proc_pattern = re.compile(r'(CREATE OR REPLACE PROCEDURE\s+[\s\S]*?\$\$;)', re.IGNORECASE)
    procs = []

    def repl(m):
        procs.append(m.group(1))
        return f"__PROC_PLACEHOLDER_{len(procs)-1}__;"

    masked = proc_pattern.sub(repl, sql_text)

    raw_stmts = [s.strip() for s in masked.split(";") if s.strip()]
    for s in raw_stmts:
        m = re.search(r"__PROC_PLACEHOLDER_(\d+)__", s)
        if m:
            statements.append(procs[int(m.group(1))])
        else:
            lines = [l for l in s.split("\n") if not l.strip().startswith("--") and l.strip()]
            if lines:
                statements.append("\n".join(lines))
    return statements


def execute_sql_file(cur, file_path: Path, cfg: dict, dry_run: bool = False) -> None:
    """Read, format, and execute statements from a SQL file."""
    if not file_path.exists():
        logging.warning(f"File not found: {file_path}")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        raw_sql = f.read()

    formatted_sql = format_sql(raw_sql, cfg)
    stmts = extract_statements(formatted_sql)

    rel_path = file_path.relative_to(PROJECT_ROOT)
    logging.info(f"📄 Processing {rel_path} ({len(stmts)} statements)...")
    for stmt in stmts:
        summary_line = stmt.strip().split("\n")[0][:80]
        if dry_run:
            logging.info(f"[DRY-RUN]\n{stmt}\n")
        else:
            logging.info(f"⏳ Executing: {summary_line}...")
            cur.execute(stmt)
            logging.info(f"✅ Executed: {summary_line}")


# ==========================================================
# Model Specifications for Dynamic Stored Procedure Builder
# (Pure DML in model files is the Single Source of Truth)
# ==========================================================
MODEL_SPECS = [
    # Canonical Layer Models
    {
        "file": TRANSFORM_DIR / "canonical" / "masterdata" / "contract_dim.sql",
        "schema_key": "schema_canonical",
        "table": "CONTRACT_DIM",
        "proc_name": "SP_LOAD_CONTRACT_DIM",
    },
    {
        "file": TRANSFORM_DIR / "canonical" / "masterdata" / "customer_dim.sql",
        "schema_key": "schema_canonical",
        "table": "CUSTOMER_DIM",
        "proc_name": "SP_LOAD_CUSTOMER_DIM",
    },
    {
        "file": TRANSFORM_DIR / "canonical" / "masterdata" / "product_dim.sql",
        "schema_key": "schema_canonical",
        "table": "PRODUCT_DIM",
        "proc_name": "SP_LOAD_PRODUCT_DIM",
    },
    {
        "file": TRANSFORM_DIR / "canonical" / "sales" / "sales_fact.sql",
        "schema_key": "schema_canonical",
        "table": "SALES_FACT",
        "proc_name": "SP_LOAD_SALES_FACT",
    },
    # Curated Layer Models
    {
        "file": TRANSFORM_DIR / "curated" / "customer_analytics.sql",
        "schema_key": "schema_curated",
        "table": "CUSTOMER_ANALYTICS",
        "proc_name": "SP_LOAD_CUSTOMER_ANALYTICS",
    },
    {
        "file": TRANSFORM_DIR / "curated" / "product_performance.sql",
        "schema_key": "schema_curated",
        "table": "PRODUCT_PERFORMANCE",
        "proc_name": "SP_LOAD_PRODUCT_PERFORMANCE",
    },
    {
        "file": TRANSFORM_DIR / "curated" / "sales_summary.sql",
        "schema_key": "schema_curated",
        "table": "SALES_SUMMARY",
        "proc_name": "SP_LOAD_SALES_SUMMARY",
    },
]


def build_model_procedure_sql(spec: dict, cfg: dict) -> str:
    """Dynamically generate a Snowflake Stored Procedure from a pure DML model file."""
    snow_cfg = cfg["snowflake"]
    db = snow_cfg["database"]
    schema = snow_cfg[spec["schema_key"]]
    table = spec["table"]
    proc_name = spec["proc_name"]
    file_path = spec["file"]

    if not file_path.exists():
        raise FileNotFoundError(f"Model file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        raw_dml = f.read()

    # Strip header comment block
    lines = []
    in_header = True
    for line in raw_dml.split("\n"):
        if in_header and (line.strip().startswith("--") or not line.strip()):
            continue
        in_header = False
        lines.append(line)
    pure_dml = "\n".join(lines).strip()
    if pure_dml.endswith(";"):
        pure_dml = pure_dml[:-1].strip()

    formatted_dml = format_sql(pure_dml, cfg)
    indented_dml = "\n".join(f"    {l}" if l.strip() else "" for l in formatted_dml.split("\n"))

    proc_sql = f"""CREATE OR REPLACE PROCEDURE {db}.{schema}.{proc_name}(is_full_refresh BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_affected INTEGER DEFAULT 0;
BEGIN
    IF (is_full_refresh) THEN
        TRUNCATE TABLE {db}.{schema}.{table};
    END IF;

{indented_dml};
    rows_affected := SQLROWCOUNT;
    RETURN 'SUCCESS: {table} loaded (Full refresh: ' || :is_full_refresh || '). Rows affected: ' || rows_affected;
END;
$$;"""
    return proc_sql


def deploy_all(cur, cfg: dict, dry_run: bool = False) -> None:
    """Deploy all transformation views, dynamically built procedures, streams, and tasks."""
    logging.info("🚀 Deploying Snowflake Native Transformation Objects from transform/...")

    # Step 1: Deploy Views & Streams
    pre_deploy_files = [
        # Stage 1: Raw / Staging views
        TRANSFORM_DIR / "raw" / "sales" / "stg_direct_sales.sql",
        TRANSFORM_DIR / "raw" / "sales" / "stg_indirect_sales.sql",
        # CDC Streams
        TRANSFORM_DIR / "raw" / "masterdata" / "streams.sql",
        TRANSFORM_DIR / "raw" / "sales" / "streams.sql",
        # Stage 2: Canonical helper views
        TRANSFORM_DIR / "canonical" / "sales" / "sales_fact_direct.sql",
        TRANSFORM_DIR / "canonical" / "sales" / "sales_fact_indirect.sql",
    ]
    for sql_file in pre_deploy_files:
        execute_sql_file(cur, sql_file, cfg, dry_run=dry_run)

    # Step 2: Dynamically build and deploy stored procedures directly from pure SQL model files
    logging.info("⚙️  Dynamically building & deploying model stored procedures from pure SQL models...")
    for spec in MODEL_SPECS:
        proc_sql = build_model_procedure_sql(spec, cfg)
        rel_path = spec["file"].relative_to(PROJECT_ROOT)
        summary_line = f"CREATE OR REPLACE PROCEDURE {spec['proc_name']} (from {rel_path})"
        if dry_run:
            logging.info(f"[DRY-RUN]\n{proc_sql}\n")
        else:
            logging.info(f"⏳ Executing: {summary_line}...")
            cur.execute(proc_sql)
            logging.info(f"✅ Executed: {summary_line}")

    # Step 3: Deploy master orchestrators and tasks
    post_deploy_files = [
        TRANSFORM_DIR / "orchestration" / "stored_procedures.sql",
        TRANSFORM_DIR / "orchestration" / "tasks.sql",
    ]
    for sql_file in post_deploy_files:
        execute_sql_file(cur, sql_file, cfg, dry_run=dry_run)

    logging.info("✅ All Snowflake Native transformation objects successfully deployed (Zero duplicate SQL!).")


def run_canonical(cur, cfg: dict, full_refresh: bool = False, dry_run: bool = False) -> str:
    """Execute canonical layer transformations (SCD2 Dimensions & Sales Fact)."""
    logging.info(f"🔄 Running Stage 2: Canonical Transformations (full_refresh={full_refresh})...")
    snow_cfg = cfg["snowflake"]
    db = snow_cfg["database"]
    canonical_schema = snow_cfg["schema_canonical"]

    fr_param = "TRUE" if full_refresh else "FALSE"
    call_sql = f"CALL {db}.{canonical_schema}.SP_LOAD_CANONICAL({fr_param});"
    if dry_run:
        logging.info(f"[DRY-RUN] {call_sql}")
        return "[DRY-RUN] Canonical load previewed."

    cur.execute(call_sql)
    res = cur.fetchone()[0]
    logging.info(f"✅ Canonical Load Result:\n{res}")
    return res


def run_curated(cur, cfg: dict, full_refresh: bool = False, dry_run: bool = False) -> str:
    """Execute curated layer transformations (Customer Analytics, Product Perf, Sales Summary)."""
    logging.info(f"🔄 Running Stage 3: Curated Transformations (full_refresh={full_refresh})...")
    snow_cfg = cfg["snowflake"]
    db = snow_cfg["database"]
    curated_schema = snow_cfg["schema_curated"]

    fr_param = "TRUE" if full_refresh else "FALSE"
    call_sql = f"CALL {db}.{curated_schema}.SP_LOAD_CURATED({fr_param});"
    if dry_run:
        logging.info(f"[DRY-RUN] {call_sql}")
        return "[DRY-RUN] Curated load previewed."

    cur.execute(call_sql)
    res = cur.fetchone()[0]
    logging.info(f"✅ Curated Load Result:\n{res}")
    return res


def run_full_pipeline(cur, cfg: dict, full_refresh: bool = False, dry_run: bool = False) -> str:
    """Execute full transformation pipeline (Canonical + Curated)."""
    logging.info(f"🚀 Running Complete Transformation Pipeline (full_refresh={full_refresh})...")
    snow_cfg = cfg["snowflake"]
    db = snow_cfg["database"]
    canonical_schema = snow_cfg["schema_canonical"]

    fr_param = "TRUE" if full_refresh else "FALSE"
    call_sql = f"CALL {db}.{canonical_schema}.SP_RUN_FULL_TRANSFORMATION({fr_param});"
    if dry_run:
        logging.info(f"[DRY-RUN] {call_sql}")
        return "[DRY-RUN] Full transformation previewed."

    cur.execute(call_sql)
    res = cur.fetchone()[0]
    logging.info(f"✅ Full Transformation Pipeline Result:\n{res}")
    return res


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Snowflake Native Data Transformation Engine (organized under transform/)."
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Deploy Snowflake transformation objects from transform/ (Views, Procedures, Streams, Tasks).",
    )
    parser.add_argument(
        "--stage",
        choices=["all", "canonical", "curated", "transforms"],
        default="all",
        help="Which transformation stage to run: 'canonical', 'curated', 'transforms' (both), or 'all' (default).",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Perform a full refresh (truncate targets and reload all data from raw).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview SQL statements without executing.",
    )
    args = parser.parse_args()

    setup_logging("transforms")
    cfg = get_config()

    stage = args.stage
    if stage in ("transforms", "dbt"):
        stage = "all"

    if args.dry_run:
        logging.info("ℹ️ Running in DRY-RUN mode.")

    if not args.dry_run:
        conn = connect_snowflake()
        cur = conn.cursor()
    else:
        conn = None
        cur = None

    try:
        if args.deploy:
            deploy_all(cur, cfg, dry_run=args.dry_run)

        if not args.deploy:
            if stage == "canonical":
                run_canonical(cur, cfg, full_refresh=args.full_refresh, dry_run=args.dry_run)
            elif stage == "curated":
                run_curated(cur, cfg, full_refresh=args.full_refresh, dry_run=args.dry_run)
            elif stage == "all":
                run_full_pipeline(cur, cfg, full_refresh=args.full_refresh, dry_run=args.dry_run)

        if conn and not args.dry_run:
            conn.commit()
    except Exception as exc:
        logging.error(f"❌ Transformation execution failed: {exc}", exc_info=True)
        return 1
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    logging.info("🏁 Snowflake Native Transformations completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
