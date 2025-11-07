import os
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
import snowflake.connector

# ==========================================================
# Paths & Logging Setup
# ==========================================================
# Get script directory and project root
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Configuration paths
CONFIG_FILE = "config.json"
CONFIG_PATH = SCRIPT_DIR / CONFIG_FILE

# SQL paths - relative to transform/raw/sales
SQL_DIR = PROJECT_ROOT / "transform" / "raw" / "sales"
SQL_FILE = SQL_DIR / "ingest_sales.sql"

# Logging setup
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"ingest_sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(console)


# ==========================================================
# Helper Functions
# ==========================================================
def load_config():
    """Load Snowflake configuration."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"❌ Config file not found at {CONFIG_PATH}")
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def connect_snowflake(cfg):
    """Connect to Snowflake."""
    conn = snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema_raw"],
        role=cfg["role"],
    )
    logging.info("✅ Connected to Snowflake.")
    return conn


def extract_sql_block(sql_text, mode):
    """Extract setup statements and FULL or INCREMENTAL block from SQL file."""
    sql_lower = sql_text.lower()
    if "-- full load section" not in sql_lower or "-- incremental load section" not in sql_lower:
        raise ValueError("❌ SQL must contain '-- FULL LOAD SECTION' and '-- INCREMENTAL LOAD SECTION'.")
    
    # Find section markers
    setup_start = "-- Step 1: File Format Definition"
    full_marker = "-- FULL LOAD SECTION"
    incr_marker = "-- INCREMENTAL LOAD SECTION"
    
    # Find section positions
    setup_pos = sql_text.find(setup_start)
    full_pos = sql_text.find(full_marker)
    incr_pos = sql_text.find(incr_marker)
    
    # Extract appropriate section and clean it
    def clean_sql_section(text):
        # Split into lines and filter
        lines = []
        for line in text.split('\n'):
            line = line.strip()
            # Keep CREATE statements and non-comment lines
            if line and (line.startswith('CREATE') or not line.startswith('--')):
                lines.append(line)
        return '\n'.join(lines)
    
    # Return appropriate section
    if mode == "setup":
        # Extract setup statements (Step 1 through Step 3)
        setup_section = sql_text[setup_pos:full_pos]
        return clean_sql_section(setup_section)
    elif mode == "full":
        # Extract full load section
        section = sql_text[full_pos:incr_pos]
        return clean_sql_section(section)
    else:
        # Extract incremental load section
        section = sql_text[incr_pos:]
        return clean_sql_section(section)


def execute_sql_statements(cur, sql_block):
    """Execute multiple SQL statements by splitting them and executing individually."""
    try:
        # Split statements on semicolons, but handle them more carefully
        raw_statements = sql_block.split(';')
        # Clean and filter statements
        statements = []
        for stmt in raw_statements:
            # Clean up the statement
            cleaned = stmt.strip()
            # Only add non-empty statements
            if cleaned and not cleaned.startswith('--'):
                statements.append(cleaned)
        
        logging.info(f"🧾 Executing {len(statements)} statements...")

        # Execute each statement individually
        for stmt in statements:
            cur.execute(stmt)
            # Fetch results if available
            results = cur.fetchall() if cur.description else None
            if results:
                logging.info(f"✅ Query results: {results}")
        
        logging.info("✅ All statements executed successfully.")

    except Exception as e:
        logging.error(f"❌ Multi-statement execution failed: {e}", exc_info=True)
        raise

# ==========================================================
# Main
# ==========================================================
def main():
    start_time = datetime.now()
    logging.info("🚀 Starting Sales ingestion process...")

    full_refresh = "--full-refresh" in sys.argv
    conn = None

    try:
        # Load config
        cfg = load_config()

        # Validate SQL file
        if not SQL_FILE.exists():
            raise FileNotFoundError(f"❌ SQL file not found at {SQL_FILE}")

        # Read SQL
        with open(SQL_FILE, "r") as f:
            sql_text = f.read()

        # Connect to Snowflake
        conn = connect_snowflake(cfg)
        cur = conn.cursor()

        # Set execution context for setup (use raw schema for all objects)
        cur.execute(f"USE ROLE {cfg['role']}")
        cur.execute(f"USE DATABASE {cfg['database']}")
        cur.execute(f"USE SCHEMA {cfg['schema_raw']}")
        cur.execute(f"USE WAREHOUSE {cfg['warehouse']}")

        # Set up search path to include raw schema
        cur.execute(f"ALTER SESSION SET SEARCH_PATH = {cfg['database']}.{cfg['schema_raw']}")

        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE();")
        context = cur.fetchone()
        logging.info(f"🧭 Context: DB={context[0]}, Schema={context[1]}, Role={context[2]}")

        # First execute setup statements
        logging.info("▶ Setting up file format, integration and stage...")
        setup_block = extract_sql_block(sql_text, "setup")
        execute_sql_statements(cur, setup_block)

        # Then execute the data load section
        mode = "full" if full_refresh else "incremental"
        logging.info(f"▶ Running {mode.upper()} load from SQL file...")
        load_block = extract_sql_block(sql_text, mode)
        
        # Add schema to table references if not present
        table_name = "sales_raw"
        schema_prefix = f"{cfg['schema_raw']}."
        load_block = load_block.replace(f" {table_name}", f" {schema_prefix}{table_name}")
        
        execute_sql_statements(cur, load_block)

        conn.commit()
        logging.info("✅ All statements executed and committed successfully.")

    except Exception as e:
        logging.error(f"❌ Ingestion failed: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logging.info("🔒 Connection closed.")
        duration = (datetime.now() - start_time).total_seconds()
        logging.info(f"⏱️ Duration: {duration:.2f}s")
        logging.info(f"📄 Log file saved at: {LOG_FILE}")


if __name__ == "__main__":
    main()
