#!/usr/bin/env python3
"""
snowflake_transforms.py
=======================
Direct Snowflake Native Transformation Engine:
Executes transformation SQL files directly from the `transform/` directory:
  - Canonical Models (Pure DML):
      transform/canonical/masterdata/contract_dim.sql
      transform/canonical/masterdata/customer_dim.sql
      transform/canonical/masterdata/product_dim.sql
      transform/canonical/sales/sales_fact.sql
  - Curated Models (Pure DML):
      transform/curated/customer_analytics.sql
      transform/curated/product_performance.sql
      transform/curated/sales_summary.sql
  - Staging Views & CDC Streams:
      transform/raw/sales/
      transform/raw/masterdata/

Features:
  - 100% Single Source of Truth: No stored_procedures.sql file. Logic lives ONLY in transform/ files.
  - Incremental Load (default): Evaluates watermark filter (> MAX target date) directly in pure DML.
  - Full Refresh (--full-refresh): Truncates target table before executing the DML.

Usage:
  python scripts/snowflake_transforms.py --deploy               # Deploy staging views & CDC streams
  python scripts/snowflake_transforms.py --stage all            # Execute all transformations (incremental)
  python scripts/snowflake_transforms.py --stage all --full-refresh # Full refresh (truncate & reload)
  python scripts/snowflake_transforms.py --stage canonical      # Execute canonical stage only
  python scripts/snowflake_transforms.py --stage curated        # Execute curated stage only
  python scripts/snowflake_transforms.py --dry-run              # Preview SQL statements without executing
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
    """Split SQL file into executable statements handling comments and semicolons."""
    raw_stmts = [s.strip() for s in sql_text.split(";") if s.strip()]
    clean_stmts = []
    for s in raw_stmts:
        lines = [l for l in s.split("\n") if not l.strip().startswith("--") and l.strip()]
        if lines:
            clean_stmts.append("\n".join(lines))
    return clean_stmts


def execute_sql_file(cur, file_path: Path, cfg: dict, dry_run: bool = False) -> None:
    """Read, format, and execute DDL statements (views, streams) from a SQL file."""
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
# Model Specifications: Direct Single Source of Truth
# ==========================================================
CANONICAL_MODELS = [
    {
        "table": "CONTRACT_DIM",
        "file": TRANSFORM_DIR / "canonical" / "masterdata" / "contract_dim.sql",
        "schema_key": "schema_canonical",
    },
    {
        "table": "CUSTOMER_DIM",
        "file": TRANSFORM_DIR / "canonical" / "masterdata" / "customer_dim.sql",
        "schema_key": "schema_canonical",
    },
    {
        "table": "PRODUCT_DIM",
        "file": TRANSFORM_DIR / "canonical" / "masterdata" / "product_dim.sql",
        "schema_key": "schema_canonical",
    },
    {
        "table": "SALES_FACT",
        "file": TRANSFORM_DIR / "canonical" / "sales" / "sales_fact.sql",
        "schema_key": "schema_canonical",
    },
]

CURATED_MODELS = [
    {
        "table": "CUSTOMER_ANALYTICS",
        "file": TRANSFORM_DIR / "curated" / "customer_analytics.sql",
        "schema_key": "schema_curated",
    },
    {
        "table": "PRODUCT_PERFORMANCE",
        "file": TRANSFORM_DIR / "curated" / "product_performance.sql",
        "schema_key": "schema_curated",
    },
    {
        "table": "SALES_SUMMARY",
        "file": TRANSFORM_DIR / "curated" / "sales_summary.sql",
        "schema_key": "schema_curated",
    },
]


def execute_model(cur, model_spec: dict, cfg: dict, full_refresh: bool = False, dry_run: bool = False) -> str:
    """Read and execute a transformation model SQL file directly against Snowflake."""
    snow_cfg = cfg["snowflake"]
    db = snow_cfg["database"]
    schema = snow_cfg[model_spec["schema_key"]]
    table = model_spec["table"]
    file_path = model_spec["file"]

    if not file_path.exists():
        raise FileNotFoundError(f"Model file not found: {file_path}")

    rel_path = file_path.relative_to(PROJECT_ROOT)
    logging.info(f"📄 Running model: {table} ({rel_path})...")

    # If full refresh requested, truncate target table first
    if full_refresh:
        truncate_sql = f"TRUNCATE TABLE {db}.{schema}.{table};"
        if dry_run:
            logging.info(f"[DRY-RUN] {truncate_sql}")
        else:
            logging.info(f"🧹 Truncating target table: {db}.{schema}.{table}...")
            cur.execute(truncate_sql)
            logging.info(f"✅ Truncated: {db}.{schema}.{table}")

    with open(file_path, "r", encoding="utf-8") as f:
        raw_sql = f.read()

    formatted_sql = format_sql(raw_sql, cfg)
    stmts = extract_statements(formatted_sql)

    rows_affected = 0
    for stmt in stmts:
        summary_line = stmt.strip().split("\n")[0][:80]
        if dry_run:
            logging.info(f"[DRY-RUN]\n{stmt}\n")
        else:
            logging.info(f"⏳ Executing DML: {summary_line}...")
            cur.execute(stmt)
            if cur.rowcount and cur.rowcount > 0:
                rows_affected += cur.rowcount
            logging.info(f"✅ Executed DML for {table}")

    return f"SUCCESS: {table} loaded (Full refresh: {full_refresh}). Rows affected: {rows_affected}"


def deploy_all(cur, cfg: dict, dry_run: bool = False) -> None:
    """Deploy transformation views and CDC streams directly from transform/."""
    logging.info("🚀 Deploying Snowflake Native Transformation Objects from transform/...")

    deploy_order = [
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

    for sql_file in deploy_order:
        execute_sql_file(cur, sql_file, cfg, dry_run=dry_run)

    logging.info("✅ All Snowflake Native transformation objects successfully deployed (Direct file execution mode).")


def run_canonical(cur, cfg: dict, full_refresh: bool = False, dry_run: bool = False) -> str:
    """Execute canonical layer transformations directly from transform/canonical/ files."""
    logging.info(f"🔄 Running Stage 2: Canonical Transformations (full_refresh={full_refresh})...")
    results = []
    for model in CANONICAL_MODELS:
        res = execute_model(cur, model, cfg, full_refresh=full_refresh, dry_run=dry_run)
        results.append(res)
    summary = "\n".join(results)
    logging.info(f"✅ Canonical Stage Complete:\n{summary}")
    return summary


def run_curated(cur, cfg: dict, full_refresh: bool = False, dry_run: bool = False) -> str:
    """Execute curated layer transformations directly from transform/curated/ files."""
    logging.info(f"🔄 Running Stage 3: Curated Transformations (full_refresh={full_refresh})...")
    results = []
    for model in CURATED_MODELS:
        res = execute_model(cur, model, cfg, full_refresh=full_refresh, dry_run=dry_run)
        results.append(res)
    summary = "\n".join(results)
    logging.info(f"✅ Curated Stage Complete:\n{summary}")
    return summary


def run_full_pipeline(cur, cfg: dict, full_refresh: bool = False, dry_run: bool = False) -> str:
    """Execute full transformation pipeline directly from transform/ files."""
    logging.info(f"🚀 Running Complete Transformation Pipeline (full_refresh={full_refresh})...")
    res_canonical = run_canonical(cur, cfg, full_refresh=full_refresh, dry_run=dry_run)
    res_curated = run_curated(cur, cfg, full_refresh=full_refresh, dry_run=dry_run)
    summary = (
        f"=== FULL TRANSFORMATION PIPELINE COMPLETE (Full refresh: {full_refresh}) ===\n\n"
        f"[CANONICAL STAGE]\n{res_canonical}\n\n"
        f"[CURATED STAGE]\n{res_curated}"
    )
    logging.info(f"🏁 Pipeline Finished:\n{summary}")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Snowflake Native Data Transformation Engine (Direct Transform File Execution)."
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Deploy Snowflake transformation views and streams directly from transform/.",
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
