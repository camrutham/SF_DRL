#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

import snowflake.connector


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_MANIFEST = SCRIPT_DIR / "copy_data_manifest.json"
DEFAULT_CONFIG = SCRIPT_DIR / "config.json"


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_config():
    if DEFAULT_CONFIG.exists():
        return load_json(DEFAULT_CONFIG)
    raise FileNotFoundError(f"Config file not found: {DEFAULT_CONFIG}")


def get_copy_defaults(cfg=None):
    cfg = cfg or get_config()
    copy_cfg = cfg.get("copy", {}) if isinstance(cfg, dict) else {}
    return {
        "default_schema": copy_cfg.get("default_schema", "sales_raw"),
        "storage_integration": copy_cfg.get("storage_integration", "drl_sf_s3_int"),
        "file_format": copy_cfg.get("file_format", "parquet_format"),
    }


def get_manifest(path=None):
    manifest_path = Path(path) if path else DEFAULT_MANIFEST
    if manifest_path.exists():
        return load_json(manifest_path)

    cfg = get_config()
    defaults = get_copy_defaults(cfg)
    return {
        "default_schema": defaults["default_schema"],
        "storage_integration": defaults["storage_integration"],
        "file_format": defaults["file_format"],
        "tables": {},
    }


def build_stage_sql(database, schema, stage_name, source_prefix, storage_integration, file_format):
    return f"""
    CREATE OR REPLACE STAGE {database}.{schema}.{stage_name}
      URL = '{source_prefix}'
      STORAGE_INTEGRATION = {storage_integration}
      FILE_FORMAT = (FORMAT_NAME = {file_format});
    """.strip()


def build_copy_sql(database, schema, stage_name, table_name, file_format, pattern):
    return f"""
    COPY INTO {database}.{schema}.{table_name}
    FROM @{database}.{schema}.{stage_name}
    PATTERN = '{pattern}'
    FILE_FORMAT = (FORMAT_NAME = {file_format})
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE';
    """.strip()


def build_pipe_sql(database, schema, pipe_name, stage_name, table_name, file_format, pattern):
    return f"""
    CREATE OR REPLACE PIPE {database}.{schema}.{pipe_name}
      AUTO_INGEST = TRUE
      AS
      COPY INTO {database}.{schema}.{table_name}
      FROM @{database}.{schema}.{stage_name}
      PATTERN = '{pattern}'
      FILE_FORMAT = (FORMAT_NAME = {file_format})
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      ON_ERROR = 'CONTINUE';
    """.strip()


def connect_snowflake(cfg):
    snow_cfg = cfg["snowflake"]
    return snowflake.connector.connect(
        user=snow_cfg["user"],
        password=snow_cfg["password"],
        account=snow_cfg["account"],
        warehouse=snow_cfg["warehouse"],
        role=snow_cfg.get("role"),
    )


def ensure_file_format(cur, file_format_name):
    sql = f"CREATE OR REPLACE FILE FORMAT {file_format_name} TYPE = PARQUET;"
    cur.execute(sql)
    print(f"✅ Ensured file format: {file_format_name}")


def ensure_stage(cur, database, schema, stage_name, source_prefix, storage_integration, file_format):
    sql = build_stage_sql(database, schema, stage_name, source_prefix, storage_integration, file_format)
    cur.execute(sql)
    print(f"✅ Ensured stage: {database}.{schema}.{stage_name}")


def ensure_pipe(cur, database, schema, pipe_name, stage_name, table_name, file_format, pattern):
    sql = build_pipe_sql(database, schema, pipe_name, stage_name, table_name, file_format, pattern)
    cur.execute(sql)
    print(f"✅ Ensured pipe: {database}.{schema}.{pipe_name}")


def copy_table(cur, database, schema, mapping, dry_run=False):
    table_name = mapping["table"]
    source_prefix = mapping["source_prefix"]
    stage_name = mapping.get("stage_name", f"{table_name.lower()}_stage")
    pipe_name = mapping.get("pipe_name", f"{table_name.lower()}_pipe")
    file_format = mapping.get("file_format")
    pattern = mapping.get("pattern", ".*\\.parquet$")
    storage_integration = mapping.get("storage_integration")

    if not file_format:
        file_format = get_copy_defaults()["file_format"]
    if not storage_integration:
        storage_integration = get_copy_defaults()["storage_integration"]

    print(f"\n📦 Configuring pipe-based ingestion for {table_name} from {source_prefix}")

    if dry_run:
        print(build_stage_sql(database, schema, stage_name, source_prefix, storage_integration, file_format))
        print(build_pipe_sql(database, schema, pipe_name, stage_name, table_name, file_format, pattern))
        return

    ensure_file_format(cur, file_format)
    ensure_stage(cur, database, schema, stage_name, source_prefix, storage_integration, file_format)
    ensure_pipe(cur, database, schema, pipe_name, stage_name, table_name, file_format, pattern)
    print(f"✅ Pipe is ready to ingest new landing files into {database}.{schema}.{table_name}")


def list_tables(manifest):
    tables = manifest.get("tables", {})
    print("Available tables:")
    for name in tables:
        print(f"  - {name}: {tables[name]['table']}")


def main():
    parser = argparse.ArgumentParser(description="Copy parquet data from S3 landing folders into Snowflake raw tables.")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST), help="Path to copy manifest JSON.")
    parser.add_argument("--table", help="Process one table only (matches manifest key, e.g. contract or customer)")
    parser.add_argument("--list", action="store_true", help="List all tables in the manifest")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing it")
    parser.add_argument("--create-objects-only", action="store_true", help="Create or replace file format, stage, and pipe objects without executing any manual COPY INTO")
    args = parser.parse_args()

    manifest = get_manifest(args.manifest)
    if args.list:
        list_tables(manifest)
        return 0

    cfg = get_config()
    snow_cfg = cfg["snowflake"]
    copy_defaults = get_copy_defaults(cfg)
    database = snow_cfg["database"]
    raw_schema = snow_cfg["schema_raw"]

    table_items = manifest.get("tables", {})
    if args.table:
        selected = [args.table]
    else:
        selected = list(table_items.keys())

    for key in selected:
        if key not in table_items:
            raise ValueError(f"Unknown table '{key}'. Use --list to view available mappings.")

    if args.dry_run:
        for key in selected:
            table_cfg = dict(table_items[key])
            table_cfg.setdefault("schema", raw_schema)
            table_cfg.setdefault("file_format", manifest.get("file_format", copy_defaults["file_format"]))
            table_cfg.setdefault("storage_integration", manifest.get("storage_integration", copy_defaults["storage_integration"]))
            copy_table(None, database, table_cfg["schema"], table_cfg, dry_run=True)
        return 0

    conn = connect_snowflake(cfg)
    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {database}")
        cur.execute(f"USE SCHEMA {raw_schema}")
        cur.execute(f"USE WAREHOUSE {snow_cfg['warehouse']}")
        cur.execute(f"USE ROLE {snow_cfg['role']}")

        for key in selected:
            table_cfg = dict(table_items[key])
            table_cfg.setdefault("schema", raw_schema)
            table_cfg.setdefault("file_format", manifest.get("file_format", copy_defaults["file_format"]))
            table_cfg.setdefault("storage_integration", manifest.get("storage_integration", copy_defaults["storage_integration"]))

            if args.create_objects_only:
                ensure_file_format(cur, table_cfg["file_format"])
                ensure_stage(cur, database, table_cfg["schema"], table_cfg["stage_name"], table_cfg["source_prefix"], table_cfg["storage_integration"], table_cfg["file_format"])
                ensure_pipe(
                    cur,
                    database,
                    table_cfg["schema"],
                    table_cfg.get("pipe_name", f"{table_cfg['table'].lower()}_pipe"),
                    table_cfg["stage_name"],
                    table_cfg["table"],
                    table_cfg["file_format"],
                    table_cfg.get("pattern", ".*\\.parquet$"),
                )
                continue

            copy_table(cur, database, table_cfg["schema"], table_cfg, dry_run=False)

        conn.commit()
    finally:
        cur.close()
        conn.close()

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"❌ ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
