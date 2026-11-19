#!/usr/bin/env python3
"""
copy_and_load.py
================
Unified data ingestion and Snowflake Native transformation orchestration pipeline:

  Stage 1 – Raw Copy Load : Executes COPY INTO raw tables from S3 landing stages
                            (via copy_data_manifest.json).
  Stage 2 – Canonical     : Runs Snowflake Native transformations (SCD2 Dimensions & Sales Fact).
  Stage 3 – Curated       : Runs Snowflake Native transformations (Curated Analytical Models).

Features:
  - Incremental Load (default): Loads delta changes.
  - Full Refresh (--full-refresh): Truncates and rebuilds canonical and curated tables.

Usage
-----
  python scripts/copy_and_load.py                  # Run full pipeline (all 3 stages, incremental)
  python scripts/copy_and_load.py --full-refresh   # Run full pipeline with full refresh
  python scripts/copy_and_load.py --stage raw      # Run raw COPY load only
  python scripts/copy_and_load.py --stage transforms # Run Snowflake Native transforms only
  python scripts/copy_and_load.py --stage canonical# Run canonical transformations only
  python scripts/copy_and_load.py --stage curated  # Run curated transformations only
  python scripts/copy_and_load.py --table contract # Copy load a single raw table
  python scripts/copy_and_load.py --dry-run        # Preview COPY SQL and stored procedure calls
"""

import argparse
import json
import logging
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

from snowflake_transforms import run_canonical, run_curated, run_full_pipeline


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
# Stage 1 – Raw Copy Load
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
# Stages 2 & 3 – Snowflake Native Transformations
# ==========================================================
def run_transformations(stage: str = "all", full_refresh: bool = False, dry_run: bool = False) -> None:
    """Execute Snowflake Native Transformations via Stored Procedures."""
    cfg = get_config()
    snow_cfg = cfg["snowflake"]

    if dry_run:
        logging.info(f"[DRY-RUN] Executing transformations for stage: {stage} (full_refresh={full_refresh})")
        if stage in ("all", "transforms", "canonical"):
            run_canonical(None, cfg, full_refresh=full_refresh, dry_run=True)
        if stage in ("all", "transforms", "curated"):
            run_curated(None, cfg, full_refresh=full_refresh, dry_run=True)
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
        cur.execute(f"USE DATABASE {snow_cfg['database']}")
        cur.execute(f"USE WAREHOUSE {snow_cfg['warehouse']}")

        if stage in ("all", "transforms"):
            run_full_pipeline(cur, cfg, full_refresh=full_refresh, dry_run=False)
        elif stage == "canonical":
            run_canonical(cur, cfg, full_refresh=full_refresh, dry_run=False)
        elif stage == "curated":
            run_curated(cur, cfg, full_refresh=full_refresh, dry_run=False)

        conn.commit()
    finally:
        cur.close()
        conn.close()


# ==========================================================
# Main Execution
# ==========================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Unified Ingestion and Snowflake Native Data Transformation Pipeline."
    )
    parser.add_argument(
        "--stage",
        choices=["all", "raw", "canonical", "curated", "transforms"],
        default="all",
        help="Which stage(s) to run: 'raw' (COPY INTO), 'canonical', 'curated', 'transforms' (both canonical and curated), 'all' (default)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Perform full refresh (truncate target tables and reload from scratch)",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Specific table key to load (e.g., contract, customer, product, dir_sale_lines, indir_sale_lines)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL and procedure calls without executing",
    )
    args = parser.parse_args()

    selected_stage = args.stage
    if selected_stage == "dbt":
        print("ℹ️ Note: dbt has been retired in favor of Snowflake Native Transformations. Executing native transforms...")
        selected_stage = "transforms"

    log_file = setup_logging("pipeline")
    start = datetime.now()
    logging.info("🚀 Starting copy_and_load pipeline (Snowflake Native)...")
    logging.info(f"   Stage        : {selected_stage}")
    logging.info(f"   Full Refresh : {args.full_refresh}")
    if args.table:
        logging.info(f"   Table        : {args.table}")
    if args.dry_run:
        logging.info("   Mode         : dry-run")

    success = True
    try:
        # Stage 1: Raw COPY INTO
        if selected_stage in ("all", "raw"):
            run_raw_copy_load(selected_table=args.table, dry_run=args.dry_run)

        # Stage 2 & 3: Snowflake Native Transforms
        if selected_stage in ("all", "transforms", "canonical", "curated"):
            transform_scope = "all" if selected_stage in ("all", "transforms") else selected_stage
            run_transformations(stage=transform_scope, full_refresh=args.full_refresh, dry_run=args.dry_run)

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
