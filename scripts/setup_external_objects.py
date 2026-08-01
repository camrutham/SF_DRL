#!/usr/bin/env python3
import argparse
import csv
import json
import sys
from pathlib import Path

import snowflake.connector

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_TEMPLATE_DIR = PROJECT_ROOT / "templates"
DEFAULT_CONFIG = SCRIPT_DIR / "config.json"


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_config():
    for candidate in (DEFAULT_CONFIG, PROJECT_ROOT / "config.json"):
        if candidate.exists():
            return load_json(candidate)
    raise FileNotFoundError(f"Config file not found: {DEFAULT_CONFIG}")


class SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def interpolate(template: str, values: dict) -> str:
    if not template:
        return template
    try:
        return template.format_map(SafeDict(values))
    except Exception:
        return template


def load_csv_rows(path: Path):
    if not path.exists():
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cleaned = {}
            for key, value in row.items():
                k = (key or "").strip()
                v = (value or "").strip()
                if k:
                    cleaned[k] = v
            rows.append(cleaned)
    return rows


def render_file_format_sql(row: dict, values: dict):
    name = row.get("name")
    file_type = row.get("type") or row.get("format_type")
    options = row.get("options", "")
    if not name or not file_type:
        raise ValueError(f"Invalid file format row: {row}")

    sql = f"CREATE OR REPLACE FILE FORMAT {name} TYPE = '{file_type}'"
    option_sql = interpolate(options, values)
    if option_sql:
        sql += f" {option_sql}"
    return sql


def render_storage_integration_sql(row: dict, values: dict):
    name = row.get("name")
    if not name:
        raise ValueError(f"Missing storage integration name in row: {row}")

    provider = row.get("storage_provider", "S3")
    enabled = row.get("enabled", "TRUE")
    aws_arn = interpolate(row.get("storage_aws_role_arn", ""), values)
    allowed_locations = row.get("storage_allowed_locations", "")
    comment = interpolate(row.get("comment", ""), values)

    sql = (
        f"CREATE OR REPLACE STORAGE INTEGRATION {name}\n"
        f"  TYPE = EXTERNAL_STAGE\n"
        f"  STORAGE_PROVIDER = {provider}\n"
        f"  ENABLED = {enabled}"
    )
    if aws_arn:
        sql += f"\n  STORAGE_AWS_ROLE_ARN = '{aws_arn}'"
    if allowed_locations:
        locations = [loc.strip() for loc in allowed_locations.replace(";", ",").split(",") if loc.strip()]
        loc_sql = ", ".join([f"'{loc}'" for loc in locations])
        sql += f"\n  STORAGE_ALLOWED_LOCATIONS = ({loc_sql})"
    if comment:
        sql += f"\n  COMMENT = '{comment}'"
    return sql


def render_stage_sql(row: dict, values: dict):
    name = row.get("name")
    if not name:
        raise ValueError(f"Missing stage name in row: {row}")

    url = interpolate(row.get("url", ""), values)
    file_format = row.get("file_format", "")
    storage_integration = row.get("storage_integration", "")
    comment = interpolate(row.get("comment", ""), values)
    schema_name = values.get("schema_raw", "PUBLIC")
    qualified_name = f"{values.get('database')}.{schema_name}.{name}"

    sql = f"CREATE OR REPLACE STAGE {qualified_name}"
    if url:
        sql += f"\n  URL = '{url}'"
    if file_format:
        sql += f"\n  FILE_FORMAT = {file_format}"
    if storage_integration:
        sql += f"\n  STORAGE_INTEGRATION = {storage_integration}"
    if comment:
        sql += f"\n  COMMENT = '{comment}'"
    return sql


def connect_snowflake(cfg):
    snow_cfg = cfg["snowflake"]
    return snowflake.connector.connect(
        user=snow_cfg["user"],
        password=snow_cfg["password"],
        account=snow_cfg["account"],
        warehouse=snow_cfg["warehouse"],
        role=snow_cfg.get("role"),
    )


def print_sql_block(label, sql):
    print(f"\n--- {label} ---")
    print(sql)


def ensure_objects(cfg, template_dir, dry_run=False):
    values = {
        "database": cfg["snowflake"]["database"],
        "schema_raw": cfg["snowflake"]["schema_raw"],
        "schema_canonical": cfg["snowflake"]["schema_canonical"],
        "schema_curated": cfg["snowflake"]["schema_curated"],
        "warehouse": cfg["snowflake"]["warehouse"],
        "role": cfg["snowflake"].get("role"),
    }

    file_format_rows = load_csv_rows(template_dir / "file_formats.csv")
    storage_rows = load_csv_rows(template_dir / "storage_integrations.csv")
    stage_rows = load_csv_rows(template_dir / "stages.csv")

    if dry_run:
        for row in file_format_rows:
            print_sql_block(f"FILE FORMAT: {row.get('name')}", render_file_format_sql(row, values))
        for row in storage_rows:
            print_sql_block(f"STORAGE INTEGRATION: {row.get('name')}", render_storage_integration_sql(row, values))
        for row in stage_rows:
            print_sql_block(f"STAGE: {row.get('name')}", render_stage_sql(row, values))
        return

    conn = connect_snowflake(cfg)
    cur = conn.cursor()
    try:
        cur.execute(f"USE ROLE {cfg['snowflake']['role']}")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {cfg['snowflake']['database']}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg['snowflake']['database']}.{cfg['snowflake']['schema_raw']}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg['snowflake']['database']}.{cfg['snowflake']['schema_canonical']}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg['snowflake']['database']}.{cfg['snowflake']['schema_curated']}")
        cur.execute(f"USE DATABASE {cfg['snowflake']['database']}")
        cur.execute(f"USE SCHEMA {cfg['snowflake']['database']}.{cfg['snowflake']['schema_raw']}")
        cur.execute(f"USE WAREHOUSE {cfg['snowflake']['warehouse']}")

        for row in file_format_rows:
            sql = render_file_format_sql(row, values)
            print(f"DEBUG FILE FORMAT: {row.get('name')}\n{sql}")
            cur.execute(sql)
            print(f"✅ Created file format: {row.get('name')}")

        for row in storage_rows:
            sql = render_storage_integration_sql(row, values)
            print(f"DEBUG INTEGRATION: {row.get('name')}\n{sql}")
            cur.execute(sql)
            print(f"✅ Created storage integration: {row.get('name')}")

        for row in stage_rows:
            sql = render_stage_sql(row, values)
            print(f"DEBUG STAGE: {row.get('name')}\n{sql}")
            cur.execute(sql)
            print(f"✅ Created stage: {row.get('name')}")

        conn.commit()
    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Create Snowflake external-stage objects from template CSV files.")
    parser.add_argument("--template-dir", default=str(DEFAULT_TEMPLATE_DIR), help="Directory containing file_formats.csv, storage_integrations.csv, and stages.csv")
    parser.add_argument("--dry-run", action="store_true", help="Print generated SQL without connecting to Snowflake")
    args = parser.parse_args()

    cfg = get_config()
    template_dir = Path(args.template_dir)
    ensure_objects(cfg, template_dir, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"❌ ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
