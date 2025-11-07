from pathlib import Path
import csv
import logging
from typing import Dict

from ddl_generator import load_yaml_tables, render_create_table_sql
from snowflake_utils import connect_snowflake, get_config, execute_sql

# --- Load config ---
cfg = get_config()
db_name = cfg["database"]

schema_map = {
    "raw": cfg["schema_raw"],
    "canonical": cfg["schema_canonical"],
    "curated": cfg["schema_curated"]
}

# --- Connect to Snowflake ---
conn = connect_snowflake()
cur = conn.cursor()

# --- Base schema directory ---
base_dir = Path(__file__).resolve().parent.parent / "schema"

# --- CSV-driven objects (file formats, storage integrations, stages) ---
def load_csv_rows(path: Path):
    if not path.exists():
        print(f"ℹ️ CSV not found: {path} (skipping)")
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            # strip keys and values; guard against None keys/values
            cleaned = {}
            for k, v in r.items():
                key = (k or "").strip()
                val = (v or "").strip()
                if key:
                    cleaned[key] = val
            rows.append(cleaned)
    return rows


class SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def interp(template: str, cfg: Dict):
    if not template:
        return template
    try:
        return template.format_map(SafeDict(cfg))
    except Exception:
        return template


def create_file_formats(cur, cfg, csv_path: Path):
    for row in load_csv_rows(csv_path):
        name = row.get("name")
        ftype = row.get("type") or row.get("format_type")
        options = row.get("options", "")
        options = interp(options, cfg)
        if not (name and ftype):
            print(f"⚠️ Skipping file format row (missing name/type): {row}")
            continue
        sql = f"CREATE OR REPLACE FILE FORMAT {name} TYPE = '{ftype}'"
        if options:
            sql += f" {options}"
        execute_sql(cur, sql, f"Create File Format {name}")


def create_storage_integrations(cur, cfg, csv_path: Path):
    for row in load_csv_rows(csv_path):
        name = row.get("name")
        storage_provider = row.get("storage_provider", "S3")
        enabled = row.get("enabled", "TRUE")
        aws_arn = interp(row.get("storage_aws_role_arn", ""), cfg)
        allowed = row.get("storage_allowed_locations", "")
        comment = interp(row.get("comment", ""), cfg)

        if not name:
            print(f"⚠️ Skipping storage integration row (missing name): {row}")
            continue

        sql = f"CREATE OR REPLACE STORAGE INTEGRATION {name}\n  TYPE = EXTERNAL_STAGE\n  STORAGE_PROVIDER = {storage_provider}\n  ENABLED = {enabled}"
        if aws_arn:
            sql += f"\n  STORAGE_AWS_ROLE_ARN = '{aws_arn}'"
        if allowed:
            # support semicolon or comma separated
            locs = [l.strip() for l in allowed.replace(';', ',').split(',') if l.strip()]
            locs_sql = ', '.join([f"'{l}'" for l in locs])
            sql += f"\n  STORAGE_ALLOWED_LOCATIONS = ({locs_sql})"
        if comment:
            sql += f"\n  COMMENT = '{comment}'"

        execute_sql(cur, sql, f"Create Storage Integration {name}")


def create_stages(cur, cfg, csv_path: Path):
    for row in load_csv_rows(csv_path):
        name = row.get("name")
        url = interp(row.get("url", ""), cfg)
        file_format = row.get("file_format", "")
        storage_integration = row.get("storage_integration", "")
        comment = interp(row.get("comment", ""), cfg)

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

        execute_sql(cur, sql, f"Create Stage {name}")


# CSV templates directory
script_dir = Path(__file__).resolve().parent
templates_dir = script_dir.parent / "templates"
file_formats_csv = templates_dir / "file_formats.csv"
storage_int_csv = templates_dir / "storage_integrations.csv"
stages_csv = templates_dir / "stages.csv"

# Execute CSV-driven DDL first
cfg = get_config()
db_name = cfg["database"]

# Ensure database and schema exist before using
execute_sql(cur, f"CREATE DATABASE IF NOT EXISTS {db_name}", "Create Database")
execute_sql(cur, f"CREATE SCHEMA IF NOT EXISTS {db_name}.{cfg['schema_raw']}", f"Create Schema {cfg['schema_raw']}")

cur.execute(f"USE ROLE {cfg['role']}")
cur.execute(f"USE DATABASE {db_name}")
cur.execute(f"USE SCHEMA {cfg['schema_raw']}")

create_file_formats(cur, cfg, file_formats_csv)
create_storage_integrations(cur, cfg, storage_int_csv)
create_stages(cur, cfg, stages_csv)

# Continue with table creation

for table_def, yaml_path in load_yaml_tables(base_dir):
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

    # --- Generate SQL ---
    sql = render_create_table_sql(table_def, db_name, schema_name)

    print(f"🧱 Creating table: {db_name}.{schema_name}.{table_name}")

    # --- Execute DDL ---
    execute_sql(cur, f"CREATE DATABASE IF NOT EXISTS {db_name}", "Create Database")
    execute_sql(cur, f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema_name}", f"Create Schema {schema_name}")
    execute_sql(cur, sql, f"Create Table {table_name}")

cur.close()
conn.close()
print("🏁 All tables created successfully.")
