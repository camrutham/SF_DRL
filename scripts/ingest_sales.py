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
    setup_marker = "-- ============================================================================"
    full_marker = "-- ============================================================================\n-- FULL LOAD SECTION"
    incr_marker = "-- ============================================================================\n-- INCREMENTAL LOAD SECTION"
    
    # Find section positions
    setup_pos = sql_text.find(setup_marker)
    full_pos = sql_text.find(full_marker)
    incr_pos = sql_text.find(incr_marker)
    
    # Return appropriate section
    if mode == "setup":
        # Everything from start to full load section, excluding setup marker
        return sql_text[setup_pos + len(setup_marker):full_pos].strip()
    elif mode == "full":
        # Everything between full and incremental sections, excluding full marker
        section = sql_text[full_pos + len(full_marker):incr_pos].strip()
        if section.startswith("--"):
            section = section[section.find("\n") + 1:].strip()
        return section
    else:
        # Everything after incremental section, excluding incremental marker
        section = sql_text[incr_pos + len(incr_marker):].strip()
        if section.startswith("--"):
            section = section[section.find("\n") + 1:].strip()
        return section


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

        # Set execution context
        cur.execute(f"USE ROLE {cfg['role']}")
        cur.execute(f"USE DATABASE {cfg['database']}")
        cur.execute(f"USE SCHEMA {cfg['schema_raw']}")
        cur.execute(f"USE WAREHOUSE {cfg['warehouse']}")

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
