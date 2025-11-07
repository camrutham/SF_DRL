import yaml
import json
import snowflake.connector
from pathlib import Path

## Create database and Schema
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
print(f"📘 Config loaded: {db_name}.{schema_raw}")
print(f"📘 Config loaded: {db_name}.{schema_canonical}")
print(f"📘 Config loaded: {db_name}.{schema_curated}")

conn = snowflake.connector.connect(
    user="chandu2103",
    password="Snowflake@20156606",
    account="IJIPYSX-XZ71784",
    warehouse=warehouse
)

cur = conn.cursor()

cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema_raw}")
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema_canonical}")
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema_curated}")


print(f"✅ Database `{db_name}` and schema `{schema_raw}` created successfully.")
print(f"✅ Database `{db_name}` and schema `{schema_canonical}` created successfully.")
print(f"✅ Database `{db_name}` and schema `{schema_curated}` created successfully.")

cur.close()
conn.close()