import os
import yaml
import json
from pathlib import Path
import snowflake.connector

# --- Step 1: Load config.json (database & schema) ---
config_path = Path(__file__).resolve().parent / "config.json"

with open(config_path, "r") as f:
    config = json.load(f)

snowflake_cfg = config.get("snowflake", config)
db_name = snowflake_cfg["database"]
schema_raw = snowflake_cfg["schema_raw"]
schema_canonical = snowflake_cfg["schema_canonical"]
schema_curated = snowflake_cfg["schema_curated"]
warehouse = snowflake_cfg["warehouse"]
role = snowflake_cfg.get("role")

print(f"📘 Loaded config: DB={db_name}, WH={warehouse}, Role={role}")


# --- Step 2: Connect to Snowflake ---
conn = snowflake.connector.connect(
    user=snowflake_cfg["user"],
    password=snowflake_cfg["password"],
    account=snowflake_cfg["account"],
    warehouse=warehouse,
    role=role
)
cur = conn.cursor()

# --- Step 3: Define the schema directory ---
base_dir = Path(__file__).resolve().parent.parent / "schema"


# --- Step 4: Map folder layer → schema name ---
schema_map = {
    "raw": schema_raw,
    "canonical": schema_canonical,
    "curated": schema_curated
}

# --- Step 5: Loop through all YAMLs recursively ---
for yaml_path in base_dir.rglob("*.yaml"):
    with open(yaml_path, "r") as f:
        table_def = yaml.safe_load(f)

    table_name = table_def.get("table_name")
    columns = table_def.get("columns", [])

    if not (table_name and columns):
        print(f"⚠️ Skipping {yaml_path.name}: missing 'table_name' or 'columns'.")
        continue

    # Determine layer (raw/canonical/curated)
    parts = yaml_path.parts
    if "schema" not in parts:
        print(f"⚠️ Skipping {yaml_path}: unexpected folder structure.")
        continue

    schema_index = parts.index("schema")
    layer = parts[schema_index + 1].lower()  # raw / canonical / curated

    schema_name = schema_map.get(layer)
    if not schema_name:
        print(f"⚠️ Unknown layer '{layer}' for {yaml_path}. Skipping.")
        continue

    # --- Step 6: Construct CREATE TABLE statement ---
    primary_key = table_def.get("primary_key") or {}
    pk_columns = []
    if isinstance(primary_key, dict):
        pk_columns = primary_key.get("columns", []) or []
    elif isinstance(primary_key, list):
        pk_columns = primary_key
    pk_columns = [str(col) for col in pk_columns]

    pk_name_set = set(pk_columns)
    column_defs = []
    for col in columns:
        col_name = col["name"]
        col_sql = f"{col_name} {col['type']}"
        if col.get("nullable") is False or col_name in pk_name_set:
            col_sql += " NOT NULL"
        column_defs.append(f"  {col_sql}")

    table_constraints = []
    if pk_columns:
        table_constraints.append(f"  PRIMARY KEY ({', '.join(pk_columns)})")

    create_body = ",\n".join(column_defs + table_constraints)
    create_stmt = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{schema_name}.{table_name} (
{create_body}
    )
    """


    # --- Step 7: Execute SQL ---
    try:
        print(f"🧱 Creating: {db_name}.{schema_name}.{table_name}")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema_name}")
        cur.execute(create_stmt)
        print(f"✅ Created {db_name}.{schema_name}.{table_name}")
    except Exception as e:
        print(f"❌ Failed to create {db_name}.{schema_name}.{table_name}: {e}")

# --- Step 8: Cleanup ---
cur.close()
conn.close()
print("🏁 All tables processed successfully.")
