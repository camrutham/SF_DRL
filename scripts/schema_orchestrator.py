#!/usr/bin/env python3
"""
schema_orchestrator.py
======================
Orchestrates complete Snowflake database and schema infrastructure setup:

  1. Create Database & Schemas (from create_schema.py)
     - Creates database (e.g., sales) and schemas (sales_raw, sales_canonical, sales_curated).

  2. Setup External Stage Objects (from setup_external_objects.py)
     - Creates File Formats, Storage Integrations, and Stages driven by CSV templates
       (templates/file_formats.csv, templates/storage_integrations.csv, templates/stages.csv).

  3. Create Target Tables (from create_tables.py)
     - Parses table definition YAML files under the schema/ directory tree (raw, canonical, curated)
       and executes CREATE TABLE IF NOT EXISTS.

Usage
-----
  python scripts/schema_orchestrator.py
  python scripts/schema_orchestrator.py --dry-run
"""

import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import Dict

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')


from ddl_generator import load_yaml_tables, render_create_table_sql
from snowflake_utils import connect_snowflake, get_config, execute_sql

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TEMPLATES_DIR = PROJECT_ROOT / "templates"
SCHEMA_DIR = PROJECT_ROOT / "schema"


class SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def interp(template: str, cfg: Dict) -> str:
    """Safely format string placeholders in template."""
    if not template:
        return template
    try:
        return template.format_map(SafeDict(cfg))
    except Exception:
        return template


def load_csv_rows(path: Path) -> list:
    """Load cleaned rows from a CSV template file."""
    if not path.exists():
        print(f"ℹ️ CSV not found: {path} (skipping)")
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            cleaned = {}
            for k, v in r.items():
                key = (k or "").strip()
                val = (v or "").strip()
                if key:
                    cleaned[key] = val
            rows.append(cleaned)
    return rows


# ==========================================================
# 1. Create Database & Schemas
# ==========================================================
def create_schemas(cur, snowflake_cfg: dict, dry_run: bool = False):
    """Create Database and Schemas (raw, canonical, curated)."""
    db_name = snowflake_cfg["database"]
    raw = snowflake_cfg["schema_raw"]
    canonical = snowflake_cfg["schema_canonical"]
    curated = snowflake_cfg["schema_curated"]

    sqls = [
        (f"CREATE DATABASE IF NOT EXISTS {db_name}", f"Create Database '{db_name}'"),
        (f"CREATE SCHEMA IF NOT EXISTS {db_name}.{raw}", f"Create Schema '{db_name}.{raw}'"),
        (f"CREATE SCHEMA IF NOT EXISTS {db_name}.{canonical}", f"Create Schema '{db_name}.{canonical}'"),
        (f"CREATE SCHEMA IF NOT EXISTS {db_name}.{curated}", f"Create Schema '{db_name}.{curated}'"),
    ]

    print("\n--- 1. Creating Database and Schemas ---")
    for sql, label in sqls:
        if dry_run:
            print(f"[DRY-RUN] {sql}")
        else:
            execute_sql(cur, sql, label)


# ==========================================================
# 2. Setup External Objects (File Formats, Integrations, Stages)
# ==========================================================
def create_file_formats(cur, snowflake_cfg: dict, csv_path: Path, dry_run: bool = False):
    """Create file formats from CSV template."""
    for row in load_csv_rows(csv_path):
        name = row.get("name")
        ftype = row.get("type") or row.get("format_type")
        options = row.get("options", "")
        options = interp(options, snowflake_cfg)
        if not (name and ftype):
            print(f"⚠️ Skipping file format row (missing name/type): {row}")
            continue
        sql = f"CREATE OR REPLACE FILE FORMAT {name} TYPE = '{ftype}'"
        if options:
            sql += f" {options}"
        if dry_run:
            print(f"[DRY-RUN] {sql}")
        else:
            execute_sql(cur, sql, f"Create File Format {name}")


def create_storage_integrations(cur, snowflake_cfg: dict, csv_path: Path, dry_run: bool = False):
    """Create storage integrations from CSV template."""
    for row in load_csv_rows(csv_path):
        name = row.get("name")
        storage_provider = row.get("storage_provider", "S3")
        enabled = row.get("enabled", "TRUE")
        aws_arn = interp(row.get("storage_aws_role_arn", ""), snowflake_cfg)
        allowed = row.get("storage_allowed_locations", "")
        comment = interp(row.get("comment", ""), snowflake_cfg)

        if not name:
            print(f"⚠️ Skipping storage integration row (missing name): {row}")
            continue

        sql = (
            f"CREATE OR REPLACE STORAGE INTEGRATION {name}\n"
            f"  TYPE = EXTERNAL_STAGE\n"
            f"  STORAGE_PROVIDER = {storage_provider}\n"
            f"  ENABLED = {enabled}"
        )
        if aws_arn:
            sql += f"\n  STORAGE_AWS_ROLE_ARN = '{aws_arn}'"
        if allowed:
            locs = [l.strip() for l in allowed.replace(";", ",").split(",") if l.strip()]
            locs_sql = ", ".join([f"'{l}'" for l in locs])
            sql += f"\n  STORAGE_ALLOWED_LOCATIONS = ({locs_sql})"
        if comment:
            sql += f"\n  COMMENT = '{comment}'"

        if dry_run:
            print(f"[DRY-RUN] {sql}")
        else:
            execute_sql(cur, sql, f"Create Storage Integration {name}")


def create_stages(cur, snowflake_cfg: dict, csv_path: Path, dry_run: bool = False):
    """Create stages from CSV template."""
    for row in load_csv_rows(csv_path):
        name = row.get("name")
        url = interp(row.get("url", ""), snowflake_cfg)
        file_format = row.get("file_format", "")
        storage_integration = row.get("storage_integration", "")
        comment = interp(row.get("comment", ""), snowflake_cfg)

        if not name:
            print(f"⚠️ Skipping stage row (missing name): {row}")
            continue

        sql = f"CREATE OR REPLACE STAGE {name}"
        if url:
            sql += f"\n  URL = '{url}'"
        if file_format:
            sql += f"\n  FILE_FORMAT = {file_format}"
        if storage_integration:
            sql += f"\n  STORAGE_INTEGRATION = {storage_integration}"
        if comment:
            sql += f"\n  COMMENT = '{comment}'"

        if dry_run:
            print(f"[DRY-RUN] {sql}")
        else:
            execute_sql(cur, sql, f"Create Stage {name}")


def setup_external_objects(cur, snowflake_cfg: dict, templates_dir: Path, dry_run: bool = False):
    """Orchestrate setup of file formats, storage integrations, and stages."""
    print("\n--- 2. Setting up External Stage Objects ---")
    create_file_formats(cur, snowflake_cfg, templates_dir / "file_formats.csv", dry_run)
    create_storage_integrations(cur, snowflake_cfg, templates_dir / "storage_integrations.csv", dry_run)
    create_stages(cur, snowflake_cfg, templates_dir / "stages.csv", dry_run)


# ==========================================================
# 3. Create Target Tables (from schema/*.yaml definitions)
# ==========================================================
def create_tables(cur, snowflake_cfg: dict, schema_dir: Path, dry_run: bool = False):
    """Parse YAML table definitions and execute CREATE TABLE IF NOT EXISTS."""
    print("\n--- 3. Creating Target Tables from YAML definitions ---")
    db_name = snowflake_cfg["database"]
    schema_map = {
        "raw": snowflake_cfg["schema_raw"],
        "canonical": snowflake_cfg["schema_canonical"],
        "curated": snowflake_cfg["schema_curated"],
    }

    for table_def, yaml_path in load_yaml_tables(schema_dir):
        table_name = table_def.get("table_name")
        columns = table_def.get("columns", [])

        if not (table_name and columns):
            print(f"⚠️ Skipping {yaml_path.name}: missing 'table_name' or 'columns'")
            continue

        # Identify layer from folder path
        parts = yaml_path.parts
        if "schema" not in parts:
            print(f"⚠️ Unexpected folder structure for {yaml_path}")
            continue

        schema_index = parts.index("schema")
        layer = parts[schema_index + 1].lower()
        schema_name = schema_map.get(layer)

        if not schema_name:
            print(f"⚠️ Unknown layer '{layer}' for {yaml_path}")
            continue

        sql = render_create_table_sql(table_def, db_name, schema_name)
        print(f"🧱 Table target: {db_name}.{schema_name}.{table_name}")

        if dry_run:
            print(f"[DRY-RUN]\n{sql}")
        else:
            execute_sql(cur, f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema_name}", f"Create Schema {schema_name}")
            execute_sql(cur, sql, f"Create Table {table_name}")


# ==========================================================
# Main Orchestrator Execution
# ==========================================================
def main():
    parser = argparse.ArgumentParser(
        description="Orchestrate Snowflake database, schema, external objects, and table creation."
    )
    parser.add_argument("--dry-run", action="store_true", help="Print generated SQL without executing")
    args = parser.parse_args()

    cfg = get_config()
    snowflake_cfg = cfg["snowflake"]
    db_name = snowflake_cfg["database"]

    print("🚀 Starting Schema Orchestration...")

    if args.dry_run:
        create_schemas(None, snowflake_cfg, dry_run=True)
        setup_external_objects(None, snowflake_cfg, TEMPLATES_DIR, dry_run=True)
        create_tables(None, snowflake_cfg, SCHEMA_DIR, dry_run=True)
        print("\n🏁 Dry-run completed successfully.")
        return 0

    conn = connect_snowflake()
    cur = conn.cursor()

    try:
        # Step 1: Create Database & Schemas
        create_schemas(cur, snowflake_cfg)

        # Context switch to role, db, schema
        cur.execute(f"USE ROLE {snowflake_cfg['role']}")
        cur.execute(f"USE DATABASE {db_name}")
        cur.execute(f"USE SCHEMA {snowflake_cfg['schema_raw']}")

        # Step 2: External Stage Objects
        setup_external_objects(cur, snowflake_cfg, TEMPLATES_DIR)

        # Step 3: Tables
        create_tables(cur, snowflake_cfg, SCHEMA_DIR)

        conn.commit()
        print("\n🏁 All schemas, external objects, and tables orchestrated successfully.")
    except Exception as exc:
        print(f"\n❌ Schema orchestration failed: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
