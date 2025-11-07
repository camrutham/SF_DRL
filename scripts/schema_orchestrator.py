from pathlib import Path
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
