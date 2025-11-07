import os
import yaml
import json
from pathlib import Path
import snowflake.connector

# --- Step 1: Load config.json (database & schema) ---
config_path = Path(__file__).resolve().parent / "config.json"

with open(config_path, "r") as f:
    config = json.load(f)

db_name = config["database"]
schema_raw = config["schema_raw"]
schema_canonical = config["schema_canonical"]
schema_curated = config["schema_curated"]
warehouse = config["warehouse"]
role = config["role"]

print(f"📘 Loaded config: DB={db_name}, WH={warehouse}, Role={role}")


# --- Step 2: Connect to Snowflake ---
conn = snowflake.connector.connect(
    user="chandu2103",
    password="Snowflake@20156606",
    account="IJIPYSX-XZ71784",
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
    column_defs = ",\n  ".join([f"{col['name']} {col['type']}" for col in columns])
    create_stmt = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{schema_name}.{table_name} (
      {column_defs}
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
