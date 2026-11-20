#!/usr/bin/env python3
"""
snowflake_transforms.py
=======================
Direct Snowflake Native Transformation Engine (Stream-Based CDC Architecture):
Executes pure SQL transformation models directly from the `transform/` directory
using Snowflake Change Data Capture (CDC) Streams for zero full-table-scan incremental processing:

  - Canonical Models (Stream CDC DML):
      transform/canonical/masterdata/contract_dim.sql (CONTRACT_RAW_STREAM)
      transform/canonical/masterdata/customer_dim.sql (CUSTOMER_RAW_STREAM)
      transform/canonical/masterdata/product_dim.sql (PRODUCT_RAW_STREAM)
      transform/canonical/sales/sales_fact.sql (DIRECT & INDIRECT SALE LINES RAW STREAMS)

  - Curated Models (Stream CDC Rollup DML):
      transform/curated/customer_analytics.sql (CUSTOMER_DIM_STREAM, SALES_FACT_STREAM_CUSTOMER)
      transform/curated/product_performance.sql (PRODUCT_DIM_STREAM, SALES_FACT_STREAM_PRODUCT)
      transform/curated/sales_summary.sql (SALES_FACT_STREAM_SUMMARY)

  - Staging Views & CDC Streams:
      transform/raw/masterdata/streams.sql
      transform/raw/sales/streams.sql
      transform/canonical/masterdata/streams.sql
      transform/canonical/sales/streams.sql

Features:
  - High-Performance CDC: Queries only changed rows recorded in Snowflake Streams.
  - Skip on Empty Streams: Evaluates SYSTEM$STREAM_HAS_DATA() before executing DML; skips empty streams with 0 warehouse cost.
  - Full Refresh (--full-refresh): Truncates target table, queries base table, and resets stream offsets.

Usage:
  python scripts/snowflake_transforms.py --deploy               # Deploy staging views & CDC streams
  python scripts/snowflake_transforms.py --stage all            # Execute all transformations (Stream CDC incremental)
  python scripts/snowflake_transforms.py --stage all --full-refresh # Full refresh (truncate & reload from base)
  python scripts/snowflake_transforms.py --stage canonical      # Execute canonical stage only
  python scripts/snowflake_transforms.py --stage curated        # Execute curated stage only
  python scripts/snowflake_transforms.py --dry-run              # Preview SQL statements without executing
"""

import argparse
import csv
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TRANSFORM_DIR = PROJECT_ROOT / "transform"
TEMPLATES_DIR = PROJECT_ROOT / "templates"
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


def load_stream_mappings(cfg: dict = None) -> List[dict]:
    """Load stream to base table mappings from templates/stream_mappings.csv."""
    mapping_file = TEMPLATES_DIR / "stream_mappings.csv"
    if not mapping_file.exists():
        logging.warning(f"⚠️ Stream mapping template not found: {mapping_file}")
        return []

    mappings = []
    with open(mapping_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("stream_name") and row.get("base_table_name"):
                mappings.append(row)
    return mappings


def transform_sql_for_full_refresh(sql_text: str, stream_mappings: List[dict] = None) -> str:
    """
    Transform stream-based CDC SQL into full-refresh SQL against base tables.
    Dynamically swaps stream names to base table names using templates/stream_mappings.csv.
    """
    if stream_mappings is None:
        stream_mappings = load_stream_mappings()

    sql = sql_text

    # 1. Dynamically swap stream names to base table names from template CSV
    for mapping in stream_mappings:
        stream_name = mapping["stream_name"].strip()
        base_table = mapping["base_table_name"].strip()
        sql = sql.replace(stream_name, base_table)

    # 2. Swap the stream filter (METADATA$ACTION = 'INSERT') to a no-op (WHERE 1=1)
    sql = re.sub(
        r"WHERE\s+([a-zA-Z0-9_]+\.)?METADATA\$ACTION\s*=\s*'INSERT'",
        "WHERE 1=1",
        sql,
        flags=re.IGNORECASE,
    )

    return sql


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
# Model Specifications: Stream CDC Single Source of Truth
# ==========================================================
CANONICAL_MODELS = [
    {
        "table": "CONTRACT_DIM",
        "file": TRANSFORM_DIR / "canonical" / "masterdata" / "contract_dim.sql",
        "schema_key": "schema_canonical",
        "streams": ["{DATABASE}.{SCHEMA_RAW}.CONTRACT_RAW_STREAM"],
        "reset_streams_sql": [
            "CREATE OR REPLACE STREAM {DATABASE}.{SCHEMA_RAW}.CONTRACT_RAW_STREAM ON TABLE {DATABASE}.{SCHEMA_RAW}.CONTRACT_RAW APPEND_ONLY = FALSE COMMENT = 'CDC stream tracking inserts and updates on CONTRACT_RAW';"
        ],
    },
    {
        "table": "CUSTOMER_DIM",
        "file": TRANSFORM_DIR / "canonical" / "masterdata" / "customer_dim.sql",
        "schema_key": "schema_canonical",
        "streams": ["{DATABASE}.{SCHEMA_RAW}.CUSTOMER_RAW_STREAM"],
        "reset_streams_sql": [
            "CREATE OR REPLACE STREAM {DATABASE}.{SCHEMA_RAW}.CUSTOMER_RAW_STREAM ON TABLE {DATABASE}.{SCHEMA_RAW}.CUSTOMER_RAW APPEND_ONLY = FALSE COMMENT = 'CDC stream tracking inserts and updates on CUSTOMER_RAW';"
        ],
    },
    {
        "table": "PRODUCT_DIM",
        "file": TRANSFORM_DIR / "canonical" / "masterdata" / "product_dim.sql",
        "schema_key": "schema_canonical",
        "streams": ["{DATABASE}.{SCHEMA_RAW}.PRODUCT_RAW_STREAM"],
        "reset_streams_sql": [
            "CREATE OR REPLACE STREAM {DATABASE}.{SCHEMA_RAW}.PRODUCT_RAW_STREAM ON TABLE {DATABASE}.{SCHEMA_RAW}.PRODUCT_RAW APPEND_ONLY = FALSE COMMENT = 'CDC stream tracking inserts and updates on PRODUCT_RAW';"
        ],
    },
    {
        "table": "SALES_FACT",
        "file": TRANSFORM_DIR / "canonical" / "sales" / "sales_fact.sql",
        "schema_key": "schema_canonical",
        "streams": [
            "{DATABASE}.{SCHEMA_RAW}.DIRECT_SALE_LINES_RAW_STREAM",
            "{DATABASE}.{SCHEMA_RAW}.INDIRECT_SALE_LINES_RAW_STREAM",
        ],
        "reset_streams_sql": [
            "CREATE OR REPLACE STREAM {DATABASE}.{SCHEMA_RAW}.DIRECT_SALE_LINES_RAW_STREAM ON TABLE {DATABASE}.{SCHEMA_RAW}.DIRECT_SALE_LINES_RAW APPEND_ONLY = FALSE COMMENT = 'CDC stream tracking inserts and updates on DIRECT_SALE_LINES_RAW';",
            "CREATE OR REPLACE STREAM {DATABASE}.{SCHEMA_RAW}.INDIRECT_SALE_LINES_RAW_STREAM ON TABLE {DATABASE}.{SCHEMA_RAW}.INDIRECT_SALE_LINES_RAW APPEND_ONLY = FALSE COMMENT = 'CDC stream tracking inserts and updates on INDIRECT_SALE_LINES_RAW';",
        ],
    },
]

CURATED_MODELS = [
    {
        "table": "CUSTOMER_ANALYTICS",
        "file": TRANSFORM_DIR / "curated" / "customer_analytics.sql",
        "schema_key": "schema_curated",
        "streams": [
            "{DATABASE}.{SCHEMA_CANONICAL}.CUSTOMER_DIM_STREAM",
            "{DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_STREAM_CUSTOMER",
        ],
        "reset_streams_sql": [
            "CREATE OR REPLACE STREAM {DATABASE}.{SCHEMA_CANONICAL}.CUSTOMER_DIM_STREAM ON TABLE {DATABASE}.{SCHEMA_CANONICAL}.CUSTOMER_DIM APPEND_ONLY = FALSE COMMENT = 'CDC stream tracking changes on CUSTOMER_DIM for curated downstream aggregations';",
            "CREATE OR REPLACE STREAM {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_STREAM_CUSTOMER ON TABLE {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT APPEND_ONLY = FALSE COMMENT = 'CDC stream tracking changes on SALES_FACT for CUSTOMER_ANALYTICS';",
        ],
    },
    {
        "table": "PRODUCT_PERFORMANCE",
        "file": TRANSFORM_DIR / "curated" / "product_performance.sql",
        "schema_key": "schema_curated",
        "streams": [
            "{DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM_STREAM",
            "{DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_STREAM_PRODUCT",
        ],
        "reset_streams_sql": [
            "CREATE OR REPLACE STREAM {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM_STREAM ON TABLE {DATABASE}.{SCHEMA_CANONICAL}.PRODUCT_DIM APPEND_ONLY = FALSE COMMENT = 'CDC stream tracking changes on PRODUCT_DIM for curated downstream aggregations';",
            "CREATE OR REPLACE STREAM {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_STREAM_PRODUCT ON TABLE {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT APPEND_ONLY = FALSE COMMENT = 'CDC stream tracking changes on SALES_FACT for PRODUCT_PERFORMANCE';",
        ],
    },
    {
        "table": "SALES_SUMMARY",
        "file": TRANSFORM_DIR / "curated" / "sales_summary.sql",
        "schema_key": "schema_curated",
        "streams": [
            "{DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_STREAM_SUMMARY",
        ],
        "reset_streams_sql": [
            "CREATE OR REPLACE STREAM {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT_STREAM_SUMMARY ON TABLE {DATABASE}.{SCHEMA_CANONICAL}.SALES_FACT APPEND_ONLY = FALSE COMMENT = 'CDC stream tracking changes on SALES_FACT for SALES_SUMMARY rollup';",
        ],
    },
]


def execute_model(cur, model_spec: dict, cfg: dict, full_refresh: bool = False, dry_run: bool = False) -> str:
    """Read and execute a transformation model SQL file directly against Snowflake using CDC Streams."""
    snow_cfg = cfg["snowflake"]
    db = snow_cfg["database"]
    schema = snow_cfg[model_spec["schema_key"]]
    table = model_spec["table"]
    file_path = model_spec["file"]

    if not file_path.exists():
        raise FileNotFoundError(f"Model file not found: {file_path}")

    rel_path = file_path.relative_to(PROJECT_ROOT)
    logging.info(f"📄 Running model: {table} ({rel_path})...")

    # 1. CDC Stream Check (Incremental only)
    if not full_refresh and model_spec.get("streams"):
        has_any_data = False
        stream_names = [format_sql(s, cfg) for s in model_spec["streams"]]
        if dry_run:
            has_any_data = True
            logging.info(f"[DRY-RUN] Checked SYSTEM$STREAM_HAS_DATA on: {', '.join(stream_names)}")
        else:
            for s_name in stream_names:
                try:
                    cur.execute(f"SELECT SYSTEM$STREAM_HAS_DATA('{s_name}')")
                    row = cur.fetchone()
                    if row and row[0]:
                        has_any_data = True
                        logging.info(f"⚡ CDC Stream {s_name} has new data.")
                        break
                except Exception as stream_err:
                    logging.warning(f"⚠️ Could not check stream {s_name}: {stream_err}. Proceeding with execution.")
                    has_any_data = True
                    break

            if not has_any_data:
                logging.info(f"⏭️ Skipping {table}: No new CDC changes in streams ({', '.join(stream_names)}).")
                return f"SKIPPED: {table} (No CDC stream data)"

    # 2. If full refresh requested, truncate target table first
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
    if full_refresh:
        formatted_sql = transform_sql_for_full_refresh(formatted_sql)

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

    # 3. If full refresh, reset stream offsets so they start fresh from now
    if full_refresh and model_spec.get("reset_streams_sql"):
        for reset_stmt_tmpl in model_spec["reset_streams_sql"]:
            reset_stmt = format_sql(reset_stmt_tmpl, cfg)
            if dry_run:
                logging.info(f"[DRY-RUN] Reset stream: {reset_stmt}")
            else:
                logging.info(f"🔄 Resetting stream offset: {reset_stmt[:80]}...")
                cur.execute(reset_stmt)
                logging.info("✅ Stream offset reset.")

    return f"SUCCESS: {table} loaded (Full refresh: {full_refresh}). Rows affected: {rows_affected}"


def deploy_all(cur, cfg: dict, dry_run: bool = False) -> None:
    """Deploy transformation views and CDC streams directly from transform/."""
    logging.info("🚀 Deploying Snowflake Native Transformation Objects & CDC Streams...")

    deploy_order = [
        # Stage 1: Raw CDC Streams
        TRANSFORM_DIR / "raw" / "masterdata" / "streams.sql",
        TRANSFORM_DIR / "raw" / "sales" / "streams.sql",
        # Stage 2: Canonical CDC Streams
        TRANSFORM_DIR / "canonical" / "masterdata" / "streams.sql",
        TRANSFORM_DIR / "canonical" / "sales" / "streams.sql",
        # Stage 2: Canonical Projection Views
        TRANSFORM_DIR / "canonical" / "sales" / "sales_fact_direct.sql",
        TRANSFORM_DIR / "canonical" / "sales" / "sales_fact_indirect.sql",
    ]

    for sql_file in deploy_order:
        execute_sql_file(cur, sql_file, cfg, dry_run=dry_run)

    logging.info("✅ All Snowflake Native transformation objects & CDC streams successfully deployed.")


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
        description="Snowflake Native Data Transformation Engine (Stream-Based CDC Architecture)."
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
        help="Perform a full refresh (truncate targets and reload all data from base tables).",
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
