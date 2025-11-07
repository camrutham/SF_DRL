import os
import sys
import json
import logging
from datetime import datetime
import snowflake.connector

# ---------------------------
# Paths and Config
# ---------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "scripts", "config.json")
SQL_FILE = os.path.join(BASE_DIR, "transform", "raw", "sales", "ingest_sales.sql")

LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"ingest_sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# ---------------------------
# Logging setup
# ---------------------------

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,  # use DEBUG to get full details
    format="%(asctime)s [%(levelname)s] %(message)s",
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(console)

# ---------------------------
# Helper Functions
# ---------------------------

def load_config():
    """Load Snowflake connection config from config.json."""
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Configuration file not found at {CONFIG_PATH}")

    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    required_keys = [
        "account", "user", "password", "database",
        "schema_raw", "warehouse", "role"
    ]
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise KeyError(f"Missing keys in config.json: {missing}")

    logging.debug(f"Loaded config: {json.dumps(config, indent=2)}")
    return config


def get_snowflake_connection(cfg):
    """Establish Snowflake connection using config values."""
    logging.info(f"Connecting to Snowflake account: {cfg['account']} as {cfg['user']}")
    conn = snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema_raw"],
        role=cfg["role"]
    )
    logging.info("✅ Snowflake connection established.")
    return conn


def table_is_empty(cursor, table_name):
    """Check if target table is empty."""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logging.info(f"Target table {table_name} currently has {count} rows.")
        return count == 0
    except Exception as e:
        logging.warning(f"Could not check table {table_name}: {e}")
        return True


def extract_sql_block(sql_script, mode):
    """
    Extracts the relevant SQL section for full or incremental load.
    Uses standardized headers:
      -- FULL LOAD SECTION
      -- INCREMENTAL LOAD SECTION
    """
    lower_sql = sql_script.lower()

    if "-- full load section" not in lower_sql or "-- incremental load section" not in lower_sql:
        raise ValueError("SQL script must contain '-- FULL LOAD SECTION' and '-- INCREMENTAL LOAD SECTION' markers.")

    full_start = lower_sql.find("-- full load section")
    incr_start = lower_sql.find("-- incremental load section")

    if mode == "full":
        block = sql_script[full_start:incr_start].strip()
    else:
        block = sql_script[incr_start:].strip()

    logging.debug(f"Extracted SQL block for mode [{mode}] — {len(block)} chars")
    return block


def run_sql_block(cursor, sql_block):
    """Safely execute multiple SQL statements in sequence, with debug logging."""
    statements = [stmt.strip() for stmt in sql_block.split(";") if stmt.strip()]
    logging.info(f"Executing {len(statements)} SQL statements in this block...")

    for i, stmt in enumerate(statements, start=1):
        if stmt.startswith("--") or stmt.startswith("/*"):
            continue
        try:
            preview = stmt.replace("\n", " ")[:150]
            logging.debug(f"Running statement {i}/{len(statements)}: {preview}")
            cursor.execute(stmt)
            logging.info(f"✅ Statement {i}/{len(statements)} executed successfully.")
        except Exception as e:
            logging.error(f"❌ Error executing statement {i}: {e}", exc_info=True)
            raise


def log_merge_stats(cursor):
    """Fetch Snowflake MERGE results (if any)."""
    try:
        cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
        result = cursor.fetchall()
        if result:
            logging.info(f"MERGE stats: {result}")
    except Exception as e:
        logging.warning(f"Could not fetch merge stats: {e}")


# ---------------------------
# Diagnostics Helpers
# ---------------------------

def log_stage_diagnostics(cursor):
    """List stage files and count rows visible from Python session."""
    try:
        logging.info("Checking stage visibility...")
        cursor.execute("LIST @sales.external_stage.sales_stage;")
        result = cursor.fetchall()
        logging.info(f"Stage files visible: {len(result)}")
        for row in result:
            logging.debug(f"Stage file: {row}")

        logging.info("Counting rows directly from stage file...")
        cursor.execute("SELECT COUNT(*) FROM @sales.external_stage.sales_stage/sales.parquet;")
        count = cursor.fetchone()[0]
        logging.info(f"✅ Rows visible from stage: {count}")

    except Exception as e:
        logging.error(f"Stage diagnostics failed: {e}", exc_info=True)


# ---------------------------
# Main Logic
# ---------------------------

def main():
    start_time = datetime.now()
    logging.info("=== Starting RAW Sales Ingestion ===")

    full_refresh_flag = "--full-refresh" in sys.argv
    conn = None

    try:
        cfg = load_config()

        if not os.path.exists(SQL_FILE):
            raise FileNotFoundError(f"SQL file not found at {SQL_FILE}")

        conn = get_snowflake_connection(cfg)
        cur = conn.cursor()

        # Explicitly set correct context
        logging.info("Setting Snowflake session context...")
        cur.execute(f"USE ROLE {cfg['role']};")
        cur.execute(f"USE DATABASE {cfg['database']};")
        cur.execute(f"USE WAREHOUSE {cfg['warehouse']};")
        cur.execute(f"USE SCHEMA {cfg['schema_raw']};")

        cur.execute("SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA();")
        logging.info(f"Current context: {cur.fetchall()}")

        # Stage diagnostics
        log_stage_diagnostics(cur)

        with open(SQL_FILE, "r") as f:
            sql_script = f.read()

        target_table = f"{cfg['schema_raw']}.sales_raw"

        if full_refresh_flag or table_is_empty(cur, target_table):
            mode = "full"
            logging.info("🧱 Running FULL REFRESH mode.")
        else:
            mode = "incremental"
            logging.info("🔁 Running INCREMENTAL mode.")

        sql_block = extract_sql_block(sql_script, mode).strip()
        logging.info(f"SQL block extracted ({len(sql_block)} chars).")
        logging.debug(f"--- SQL Block Start ---\n{sql_block}\n--- SQL Block End ---")

        # Execute the SQL block
        run_sql_block(cur, sql_block)

        if mode == "incremental":
            log_merge_stats(cur)

        # Verify data load
        cur.execute(f"SELECT COUNT(*) FROM {target_table}")
        count = cur.fetchone()[0]
        logging.info(f"✅ Row count after {mode.upper()} load: {count}")

        conn.commit()
        logging.info(f"✅ {mode.upper()} ingestion completed successfully.")

    except Exception as e:
        logging.error(f"❌ Ingestion failed: {e}", exc_info=True)
        raise

    finally:
        if conn:
            conn.close()
            logging.info("Connection closed.")
        duration = (datetime.now() - start_time).total_seconds()
        logging.info(f"=== Process finished in {duration:.2f} seconds ===")


if __name__ == "__main__":
    main()
