import snowflake.connector
import json
from pathlib import Path
import sys

def get_config():
    """
    Locate and load config.json automatically.
    Looks in:
      1. ../config/config.json (recommended structure)
      2. ./config.json (same folder fallback)
    """
    possible_paths = [
        Path(__file__).resolve().parent.parent / "config" / "config.json",  # ../config/
        Path(__file__).resolve().parent / "config.json"                     # ./config.json
    ]

    for path in possible_paths:
        if path.exists():
            print(f"📘 Using config file: {path}")
            with open(path, "r") as f:
                return json.load(f)

    print("❌ config.json not found in expected locations.")
    print("👉 Make sure config.json exists in either:")
    print("   1. project_root/config/config.json  OR")
    print("   2. project_root/scripts/config.json")
    sys.exit(1)  # Stop the program gracefully

def connect_snowflake():
    """
    Connect to Snowflake using credentials and settings from config.json
    """
    cfg = get_config()
    conn = snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        role=cfg["role"]
    )
    return conn

def execute_sql(cur, sql, label=None):
    """
    Execute SQL safely and log status
    """
    try:
        cur.execute(sql)
        if label:
            print(f"✅ Executed: {label}")
    except Exception as e:
        print(f"❌ Error executing {label or 'SQL'}: {e}")
