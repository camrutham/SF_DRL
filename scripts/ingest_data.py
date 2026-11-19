#!/usr/bin/env python3
"""
ingest_data.py
==============
End-to-end data ingestion and Snowflake Native transformation pipeline:

  Stage 1 – Raw Load    : COPY INTO raw tables from S3 stages (via copy_data.py)
  Stage 2 – Canonical   : Snowflake Native SCD2 & Fact transformations (raw → canonical)
  Stage 3 – Curated     : Snowflake Native Analytical Aggregations (canonical → curated)

Features:
  - Incremental Load (default): Loads delta changes based on watermark timestamps.
  - Full Refresh (--full-refresh): Truncates and rebuilds canonical and curated tables.

Usage
-----
  python scripts/ingest_data.py                  # Run all three stages (incremental)
  python scripts/ingest_data.py --full-refresh   # Run all three stages (full refresh)
  python scripts/ingest_data.py --stage raw      # Raw load only
  python scripts/ingest_data.py --stage transforms # Snowflake native transforms only
  python scripts/ingest_data.py --stage canonical# Canonical stage only
  python scripts/ingest_data.py --stage curated  # Curated stage only
  python scripts/ingest_data.py --table contract # Raw-load a single table
  python scripts/ingest_data.py --dry-run        # Print commands/SQL without executing
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# ==========================================================
# Paths
# ==========================================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
LOG_DIR = PROJECT_ROOT / "logs"
CONFIG_PATH = SCRIPT_DIR / "config.json"

from snowflake_utils import connect_snowflake, get_config
from snowflake_transforms import run_canonical, run_curated, run_full_pipeline


# ==========================================================
# Logging
# ==========================================================
def setup_logging(label: str) -> Path:
    """Configure file + console logging; return the log file path."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"ingest_{label}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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


# ==========================================================
# Stage 1 – Raw load  (delegate to copy_data.py)
# ==========================================================
def run_raw_load(table: str | None = None, dry_run: bool = False) -> None:
    """
    Invoke copy_data.py to COPY parquet files from S3 stages into raw tables.
    Pass --table to restrict to a single table, --dry-run to print SQL only.
    """
    cmd = [sys.executable, str(SCRIPT_DIR / "copy_data.py")]
    if table:
        cmd += ["--table", table]
    if dry_run:
        cmd.append("--dry-run")

    logging.info("📦 Stage 1 – Raw load starting...")
    logging.info(f"   Command: {' '.join(cmd)}")

    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        raise RuntimeError(
            f"copy_data.py exited with code {result.returncode}. "
            "Check the output above for details."
        )
    logging.info("✅ Stage 1 – Raw load complete.")


# ==========================================================
# Stage 2 & 3 – Snowflake Native Transformations
# ==========================================================
def run_native_transforms(stage: str = "all", full_refresh: bool = False, dry_run: bool = False) -> None:
    """Execute Snowflake Native transformations via Stored Procedures."""
    cfg = get_config()
    snow_cfg = cfg["snowflake"]

    if dry_run:
        logging.info(f"[DRY-RUN] Would execute Snowflake Native transforms for stage: {stage} (full_refresh={full_refresh})")
        if stage in ("all", "canonical"):
            run_canonical(None, cfg, full_refresh=full_refresh, dry_run=True)
        if stage in ("all", "curated"):
            run_curated(None, cfg, full_refresh=full_refresh, dry_run=True)
        return

    conn = connect_snowflake()
    cur = conn.cursor()
    try:
        cur.execute(f"USE ROLE {snow_cfg['role']}")
        cur.execute(f"USE DATABASE {snow_cfg['database']}")
        cur.execute(f"USE WAREHOUSE {snow_cfg['warehouse']}")

        if stage == "all":
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
# Main
# ==========================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "End-to-end ingestion: raw load (copy_data.py) + Snowflake Native transforms.\n\n"
            "Stages:\n"
            "  raw         – COPY parquet from S3 stages into Snowflake raw tables\n"
            "  canonical   – Transform raw tables into Canonical SCD2 Dims & Sales Fact\n"
            "  curated     – Aggregate Canonical models into Curated business tables\n"
            "  transforms  – Run both canonical and curated stages\n"
            "  all         – Run full pipeline (raw + canonical + curated, default)\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--stage",
        choices=["all", "raw", "canonical", "curated", "transforms"],
        default="all",
        help="Which stage(s) to run: 'raw', 'canonical', 'curated', 'transforms' (both), or 'all' (default).",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Perform full refresh (truncate target tables and reload from scratch)",
    )
    parser.add_argument(
        "--table",
        default=None,
        help=(
            "Raw-load a specific table only (matches copy_data_manifest.json key, "
            "e.g. contract, customer, product, dir_sale_lines, indir_sale_lines). "
            "Only applies when --stage is 'raw' or 'all'."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL and procedure calls without executing them.",
    )
    args = parser.parse_args()

    selected_stage = args.stage
    log_file = setup_logging("pipeline")
    start = datetime.now()
    logging.info("🚀 Ingestion pipeline starting (Snowflake Native)...")
    logging.info(f"   Stage        : {selected_stage}")
    logging.info(f"   Full Refresh : {args.full_refresh}")
    if args.table:
        logging.info(f"   Table        : {args.table}")
    if args.dry_run:
        logging.info("   Mode         : dry-run")

    success = True
    try:
        # Stage 1: Raw Load
        if selected_stage in ("all", "raw"):
            run_raw_load(table=args.table, dry_run=args.dry_run)

        # Stage 2 & 3: Snowflake Native Transforms
        if selected_stage in ("all", "transforms", "canonical", "curated"):
            transform_scope = "all" if selected_stage in ("all", "transforms") else selected_stage
            run_native_transforms(stage=transform_scope, full_refresh=args.full_refresh, dry_run=args.dry_run)

    except Exception as exc:
        logging.error(f"❌ Pipeline failed: {exc}", exc_info=True)
        success = False
    finally:
        duration = (datetime.now() - start).total_seconds()
        logging.info(f"⏱️  Total duration: {duration:.2f}s")
        logging.info(f"📄 Log file: {log_file}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
