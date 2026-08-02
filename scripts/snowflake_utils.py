import snowflake.connector
import json
from pathlib import Path
import sys

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')


def get_config():
    """
    Locate and load config.json
    """
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    config_name = "config.json"  # Default name, use this until we can load from config
    
    possible_paths = [
        project_root / config_name,  # Try project root first
        script_dir / config_name     # Then scripts dir as fallback
    ]

    for path in possible_paths:
        if path.exists():
            print(f"📘 Using config file: {path}")
            with open(path, "r") as f:
                config = json.load(f)
                # After loading initial config, update paths based on config values
                if "directories" in config and "files" in config:
                    config_dir = project_root / config["directories"]["config"]
                    config_file = config["files"]["config"]
                    final_path = config_dir / config_file
                    if final_path.exists() and final_path != path:
                        print(f"📘 Using configured config location: {final_path}")
                        with open(final_path, "r") as f2:
                            return json.load(f2)
                return config

    print("❌ config.json not found in expected locations.")
    print("👉 Make sure config.json exists in project root or scripts directory")
    sys.exit(1)  # Stop the program gracefully

def connect_snowflake():
    """
    Connect to Snowflake using credentials and settings from config.json
    """
    cfg = get_config()["snowflake"]  # Get Snowflake-specific config
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
