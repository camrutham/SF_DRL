import os
import sys
from pathlib import Path
from typing import Optional
import snowflake.connector

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent


def load_env(env_path: Optional[Path] = None) -> None:
    """
    Load environment variables from a .env file into os.environ (if not already set).
    Searches PROJECT_ROOT/.env first, then SCRIPT_DIR/.env as fallback.
    """
    if env_path is None:
        for p in [PROJECT_ROOT / ".env", SCRIPT_DIR / ".env"]:
            if p.exists():
                env_path = p
                break

    if not env_path or not env_path.exists():
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key, val = key.strip(), val.strip()
            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]
            if key and key not in os.environ:
                os.environ[key] = val


def get_config() -> dict:
    """
    Load configuration from environment variables (.env).
    Returns a unified dict with 'snowflake' connection and schema settings.
    """
    load_env()

    snow_cfg = {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
        "user": os.environ.get("SNOWFLAKE_USER", ""),
        "password": os.environ.get("SNOWFLAKE_PASSWORD", ""),
        "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "compute_wh"),
        "database": os.environ.get("SNOWFLAKE_DATABASE", "sales"),
        "schema_raw": os.environ.get("SNOWFLAKE_SCHEMA_RAW", "sales_raw"),
        "schema_canonical": os.environ.get("SNOWFLAKE_SCHEMA_CANONICAL", "sales_canonical"),
        "schema_curated": os.environ.get("SNOWFLAKE_SCHEMA_CURATED", "sales_curated"),
    }

    return {"snowflake": snow_cfg}


def connect_snowflake():
    """
    Connect to Snowflake using credentials from environment variables (.env).
    """
    cfg = get_config()["snowflake"]
    conn = snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        role=cfg["role"],
    )
    return conn


def execute_sql(cur, sql, label=None):
    """
    Execute SQL safely and log status.
    """
    try:
        cur.execute(sql)
        if label:
            print(f"✅ Executed: {label}")
    except Exception as e:
        print(f"❌ Error executing {label or 'SQL'}: {e}")
