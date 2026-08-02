#!/usr/bin/env python3
"""
ingest_data.py
==============
End-to-end data ingestion pipeline:

  Stage 1 – Raw Load    : COPY INTO raw tables from S3 stages  (via copy_data.py)
  Stage 2 – Canonical   : dbt run  staging + canonical models  (raw  → canonical)
  Stage 3 – Curated     : dbt run  curated models              (canonical → curated)

Usage
-----
  python scripts/ingest_data.py                  # run all three stages
  python scripts/ingest_data.py --stage raw      # raw load only
  python scripts/ingest_data.py --stage dbt      # dbt transforms only
  python scripts/ingest_data.py --table contract # raw-load a single table
  python scripts/ingest_data.py --dry-run        # print SQL without executing
"""

import argparse
import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# ==========================================================
# Paths
# ==========================================================
SCRIPT_DIR   = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
LOG_DIR      = PROJECT_ROOT / "logs"
CONFIG_PATH  = SCRIPT_DIR / "config.json"

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
# Stage 2 & 3 – dbt transforms
# ==========================================================

# dbt model selectors for each layer
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


def run_dbt(selectors: list[str], label: str) -> None:
    """
    Execute `dbt run --select <selectors>` from the project root.
    profiles.yml and dbt_project.yml are both in PROJECT_ROOT.
    """
    cmd = [
        "dbt", "run",
        "--profiles-dir", str(PROJECT_ROOT),
        "--project-dir",  str(PROJECT_ROOT),
        "--select",
    ] + selectors

    logging.info(f"🔄 {label} – dbt run starting...")
    logging.info(f"   Models : {', '.join(selectors)}")
    logging.info(f"   Command: {' '.join(cmd)}")

    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        raise RuntimeError(
            f"dbt run exited with code {result.returncode} for {label}. "
            "Check the dbt output above for details."
        )
    logging.info(f"✅ {label} – dbt run complete.")


def run_canonical_transform() -> None:
    """Stage 2 – raw → canonical using staging + canonical dbt models."""
    run_dbt(DBT_CANONICAL_SELECTORS, "Stage 2 – raw → canonical")


def run_curated_transform() -> None:
    """Stage 3 – canonical → curated using curated dbt models."""
    run_dbt(DBT_CURATED_SELECTORS, "Stage 3 – canonical → curated")


# ==========================================================
# Main
# ==========================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "End-to-end ingestion: raw load (copy_data.py) + dbt transforms.\n\n"
            "Stages:\n"
            "  raw  – COPY parquet from S3 stages into Snowflake raw tables\n"
            "  dbt  – Run dbt models: staging → canonical → curated\n"
            "  all  – Run both stages (default)\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--stage",
        choices=["all", "raw", "dbt"],
        default="all",
        help="Which stage(s) to run (default: all)",
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
        help="For raw stage: print COPY SQL without executing it.",
    )
    args = parser.parse_args()

    log_file = setup_logging("pipeline")
    start = datetime.now()
    logging.info("🚀 Ingestion pipeline starting...")
    logging.info(f"   Stage   : {args.stage}")
    if args.table:
        logging.info(f"   Table   : {args.table}")
    if args.dry_run:
        logging.info("   Mode    : dry-run")

    success = True
    try:
        if args.stage in ("all", "raw"):
            run_raw_load(table=args.table, dry_run=args.dry_run)

        # Skip dbt when dry-run (nothing to transform yet) or table-specific run
        if args.stage in ("all", "dbt") and not args.dry_run:
            run_canonical_transform()
            run_curated_transform()

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