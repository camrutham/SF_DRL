#!/usr/bin/env python3
"""
copy_and_load.py
================
Unified data ingestion and transformation orchestration pipeline:

  Stage 1 – Raw Copy Load : Executes COPY INTO raw tables from S3 landing stages
                            (combining copy_data.py logic via copy_data_manifest.json).
  Stage 2 – Canonical     : Runs dbt models for staging + canonical layers (raw → canonical).
  Stage 3 – Curated       : Runs dbt models for curated layer (canonical → curated).

Usage
-----
  python scripts/copy_and_load.py                  # Run full pipeline (all 3 stages)
  python scripts/copy_and_load.py --stage raw      # Run raw COPY load only
  python scripts/copy_and_load.py --stage dbt      # Run dbt transformations only
  python scripts/copy_and_load.py --table contract # Copy load a single table
  python scripts/copy_and_load.py --dry-run        # Print COPY SQL / dbt commands without executing
"""

import argparse
import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path
import snowflake.connector

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')


# ==========================================================
# Paths & Config
# ==========================================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_CONFIG = SCRIPT_DIR / "config.json"
COPY_MANIFEST_PATH = SCRIPT_DIR / "copy_data_manifest.json"


# ==========================================================
# Logging
# ==========================================================
def setup_logging(label: str) -> Path:
    """Configure file and console logging."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"copy_load_{label}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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


def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_config() -> dict:
    if DEFAULT_CONFIG.exists():
        return load_json(DEFAULT_CONFIG)
    raise FileNotFoundError(f"Config file not found: {DEFAULT_CONFIG}")


# ==========================================================
# Stage 1 – Raw Copy Load (copy_data.py functionality)
# ==========================================================
def build_copy_sql(database: str, schema: str, stage_name: str, table_name: str, file_format: str, pattern: str) -> str:
    return f"""
    COPY INTO {database}.{schema}.{table_name}
    FROM @{database}.{schema}.{stage_name}
    PATTERN = '{pattern}'
    FILE_FORMAT = (FORMAT_NAME = {file_format})
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE';
    """.strip()


def run_raw_copy_load(selected_table: str | None = None, dry_run: bool = False) -> None:
    """Copy parquet files from S3 stages into Snowflake raw tables."""
    logging.info("📦 Stage 1 – Raw Copy Load starting...")

    if not COPY_MANIFEST_PATH.exists():
        raise FileNotFoundError(f"Copy manifest file not found: {COPY_MANIFEST_PATH}")

    manifest = load_json(COPY_MANIFEST_PATH)
    cfg = get_config()
    snow_cfg = cfg["snowflake"]
    database = snow_cfg["database"]
    raw_schema = snow_cfg["schema_raw"]

    table_items = manifest.get("tables", {})
    if selected_table:
        if selected_table not in table_items:
            raise ValueError(f"Unknown table '{selected_table}'. Available manifest keys: {list(table_items.keys())}")
        target_keys = [selected_table]
    else:
        target_keys = list(table_items.keys())

    if dry_run:
        for key in target_keys:
            t = table_items[key]
            sql = build_copy_sql(
                database=database,
                schema=t.get("schema", raw_schema),
                stage_name=t["stage_name"],
                table_name=t["table"],
                file_format=t.get("file_format", manifest.get("file_format", "parquet_format")),
                pattern=t.get("pattern", ".*\\.parquet$"),
            )
            logging.info(f"[DRY-RUN] Table {t['table']}:\n{sql}")
        logging.info("✅ Stage 1 – Raw Copy Load (Dry-run) complete.")
        return

    conn = snowflake.connector.connect(
        user=snow_cfg["user"],
        password=snow_cfg["password"],
        account=snow_cfg["account"],
        warehouse=snow_cfg["warehouse"],
        role=snow_cfg.get("role"),
    )
    cur = conn.cursor()

    try:
        cur.execute(f"USE ROLE {snow_cfg['role']}")
        cur.execute(f"USE DATABASE {database}")
        cur.execute(f"USE SCHEMA {raw_schema}")
        cur.execute(f"USE WAREHOUSE {snow_cfg['warehouse']}")

        for key in target_keys:
            t = table_items[key]
            tbl_name = t["table"]
            stg_name = t["stage_name"]
            fmt_name = t.get("file_format", manifest.get("file_format", "parquet_format"))
            pattern = t.get("pattern", ".*\\.parquet$")
            sch_name = t.get("schema", raw_schema)

            sql = build_copy_sql(database, sch_name, stg_name, tbl_name, fmt_name, pattern)
            logging.info(f"⏳ Executing COPY INTO for {database}.{sch_name}.{tbl_name} from @{stg_name}...")
            cur.execute(sql)
            results = cur.fetchall() if cur.description else None
            logging.info(f"✅ COPY result for {tbl_name}: {results}")

        conn.commit()
        logging.info("✅ Stage 1 – Raw Copy Load complete.")
    finally:
        cur.close()
        conn.close()


# ==========================================================
# Stages 2 & 3 – dbt transformations (ingest_data.py functionality)
# ==========================================================
DBT_CANONICAL_SELECTORS = [
    "staging.stg_direct_sales",
    "staging.stg_indirect_sales",
    "canonical.dim_customer",
    "canonical.dim_product",
    "canonical.dim_contract",
    "canonical.sales_fact_direct",
    "canonical.sales_fact_indirect",
    "canonical.sales_fact",
]

DBT_CURATED_SELECTORS = [
    "curated.customer_analytics",
    "curated.product_performance",
    "curated.sales_summary",
]


def run_dbt_models(selectors: list[str], label: str, dry_run: bool = False) -> None:
    """Execute `dbt run --select <selectors>`."""
    cmd = [
        "dbt", "run",
        "--profiles-dir", str(PROJECT_ROOT),
        "--project-dir", str(PROJECT_ROOT),
        "--select",
    ] + selectors

    logging.info(f"🔄 {label} – dbt run starting...")
    logging.info(f"   Command: {' '.join(cmd)}")

    if dry_run:
        logging.info(f"[DRY-RUN] Would execute: {' '.join(cmd)}")
        return

    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed with exit code {result.returncode} for {label}.")
    logging.info(f"✅ {label} – dbt run complete.")


# ==========================================================
# Main Execution
# ==========================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Unified Copy Data Ingestion and dbt Data Transformation Pipeline."
    )
    parser.add_argument(
        "--stage",
        choices=["all", "raw", "dbt"],
        default="all",
        help="Which stage(s) to run: 'raw' (COPY INTO), 'dbt' (transforms), 'all' (both, default)",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Specific table key to load (e.g., contract, customer, product, dir_sale_lines, indir_sale_lines)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL and dbt commands without executing",
    )
    args = parser.parse_args()

    log_file = setup_logging("pipeline")
    start = datetime.now()
    logging.info("🚀 Starting copy_and_load pipeline...")
    logging.info(f"   Stage   : {args.stage}")
    if args.table:
        logging.info(f"   Table   : {args.table}")
    if args.dry_run:
        logging.info("   Mode    : dry-run")

    success = True
    try:
        # Stage 1: Raw COPY INTO
        if args.stage in ("all", "raw"):
            run_raw_copy_load(selected_table=args.table, dry_run=args.dry_run)

        # Stage 2 & 3: dbt Transforms
        if args.stage in ("all", "dbt"):
            run_dbt_models(DBT_CANONICAL_SELECTORS, "Stage 2 – raw → canonical", dry_run=args.dry_run)
            run_dbt_models(DBT_CURATED_SELECTORS, "Stage 3 – canonical → curated", dry_run=args.dry_run)

    except Exception as exc:
        logging.error(f"❌ Pipeline execution failed: {exc}", exc_info=True)
        success = False
    finally:
        duration = (datetime.now() - start).total_seconds()
        logging.info(f"⏱️  Total duration: {duration:.2f}s")
        logging.info(f"📄 Log file saved: {log_file}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
