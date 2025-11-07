import os
import json
import logging
import sys
import argparse
from datetime import datetime
from pathlib import Path
import snowflake.connector
from typing import Optional, List

# ==========================================================
# Constants & Configuration
# ==========================================================
class Config:
    # Project structure
    SCRIPT_DIR = Path(__file__).resolve().parent
    PROJECT_ROOT = SCRIPT_DIR.parent
    CONFIG_FILE = "config.json"
    CONFIG_PATH = SCRIPT_DIR / CONFIG_FILE
    LOG_DIR = PROJECT_ROOT / "logs"
    
    # SQL mappings are provided externally via scripts/ingest_manifest.json
    # Keep empty here to avoid hard-coded entity entries in the script.
    SQL_PATHS = {}
    SQL_FILES = {}
    TARGET_TABLES = {}

# Load optional external manifest to override SQL mappings (keeps defaults above)
MANIFEST_PATH = Config.SCRIPT_DIR / "ingest_manifest.json"
def _load_manifest():
    """If an external manifest exists, override the SQL_PATHS/SQL_FILES/TARGET_TABLES.

    Manifest format (JSON):
    {
      "sql_paths": { "sales": "transform/raw/sales", ... },
      "sql_files": { "sales": "ingest_sales.sql", ... },
      "target_tables": { "sales": "sales_raw", ... }
    }
    """
    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(
            f"Ingestion manifest not found at {MANIFEST_PATH}. Please create scripts/ingest_manifest.json with sql_paths, sql_files and target_tables mappings."
        )
    try:
        with open(MANIFEST_PATH, "r") as mf:
            manifest = json.load(mf)

        # Override sql paths: convert relative strings to absolute Paths under project root
        if "sql_paths" in manifest:
            for k, v in manifest["sql_paths"].items():
                try:
                    Config.SQL_PATHS[k] = Config.PROJECT_ROOT / Path(v)
                except Exception:
                    logging.warning(f"Could not load sql_path for '{k}' from manifest: {v}")

        # Override sql file names
        if "sql_files" in manifest:
            for k, v in manifest["sql_files"].items():
                Config.SQL_FILES[k] = v

        # Override target table names
        if "target_tables" in manifest:
            for k, v in manifest["target_tables"].items():
                Config.TARGET_TABLES[k] = v

        # Use print here to avoid configuring the logging system at import time.
        print(f"Loaded ingestion manifest from {MANIFEST_PATH}")
    except Exception as e:
        # Print errors instead of logging to avoid early logger configuration
        print(f"Failed to load ingestion manifest {MANIFEST_PATH}: {e}")

# Apply manifest overrides at import time
_load_manifest()

# ==========================================================
# Logging Setup
# ==========================================================
def setup_logging(entity_type: str) -> None:
    """Set up logging for the ingestion process."""
    # Ensure log directory exists
    Config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = Config.LOG_DIR / f"ingest_{entity_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # Configure root logger explicitly: remove existing handlers to avoid duplicates
    logger = logging.getLogger()
    # Remove existing handlers (if any) so our FileHandler will be used
    for h in list(logger.handlers):
        logger.removeHandler(h)

    logger.setLevel(logging.INFO)

    # File handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return log_file

# ==========================================================
# Helper Functions
# ==========================================================
def load_config():
    """Load Snowflake configuration."""
    if not Config.CONFIG_PATH.exists():
        raise FileNotFoundError(f"❌ Config file not found at {Config.CONFIG_PATH}")
    with open(Config.CONFIG_PATH, "r") as f:
        return json.load(f)

def connect_snowflake(cfg):
    """Connect to Snowflake with specific database and schema context."""
    from snowflake_utils import connect_snowflake as connect
    conn = connect()  # This gets base connection
    
    # Set additional context specific to ingestion
    cur = conn.cursor()
    snowflake_cfg = cfg["snowflake"]
    cur.execute(f"USE DATABASE {snowflake_cfg['database']}")
    cur.execute(f"USE SCHEMA {snowflake_cfg['schema_raw']}")
    
    logging.info("✅ Connected to Snowflake.")
    return conn

def get_sql_file_path(entity_type: str) -> Path:
    """Get the SQL file path for a given entity type."""
    if entity_type not in Config.SQL_PATHS or entity_type not in Config.SQL_FILES:
        raise ValueError(f"❌ Unknown entity type: {entity_type}")
    
    sql_path = Config.SQL_PATHS[entity_type] / Config.SQL_FILES[entity_type]
    if not sql_path.exists():
        raise FileNotFoundError(f"❌ SQL file not found at {sql_path}")
    return sql_path

def extract_sql_block(sql_text: str, mode: str) -> str:
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

def execute_sql_statements(cur, sql_block: str) -> None:
    """Execute multiple SQL statements by splitting them and executing individually."""
    try:
        from snowflake_utils import execute_sql
        
        # Split statements on semicolons, but handle them more carefully
        raw_statements = sql_block.split(';')
        # Clean and filter statements
        statements = []
        for stmt in raw_statements:
            # Clean up the statement
            cleaned = stmt.strip()
            # Only add non-empty statements and non-comment statements
            if cleaned and not cleaned.startswith('--'):
                statements.append(cleaned)
        
        logging.info(f"🧾 Executing {len(statements)} statements...")

        # Execute each statement individually using utility function
        for i, stmt in enumerate(statements, 1):
            execute_sql(cur, stmt, f"Statement {i}/{len(statements)}")
            # Fetch results if available
            results = cur.fetchall() if cur.description else None
            if results:
                logging.info(f"✅ Query results: {results}")
        
        logging.info("✅ All statements executed successfully.")

    except Exception as e:
        logging.error(f"❌ Multi-statement execution failed: {e}", exc_info=True)
        raise

def ingest_entity(entity_type: str, full_refresh: bool, conn) -> None:
    """Ingest data for a specific entity type using a shared Snowflake connection."""
    start_time = datetime.now()
    log_file = setup_logging(entity_type)
    logging.info(f"🚀 Starting {entity_type.title()} ingestion process...")
    try:
        # Load config and get SQL file
        cfg = load_config()
        sql_file = get_sql_file_path(entity_type)

        # Read SQL
        with open(sql_file, "r") as f:
            sql_text = f.read()

        cur = conn.cursor()
        snowflake_cfg = cfg["snowflake"]
        cur.execute(f"USE ROLE {snowflake_cfg['role']}")
        cur.execute(f"USE DATABASE {snowflake_cfg['database']}")
        cur.execute(f"USE SCHEMA {snowflake_cfg['schema_raw']}")
        cur.execute(f"USE WAREHOUSE {snowflake_cfg['warehouse']}")
        cur.execute(f"ALTER SESSION SET SEARCH_PATH = {snowflake_cfg['database']}.{snowflake_cfg['schema_raw']}")

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
        if entity_type in Config.TARGET_TABLES:
            table_name = Config.TARGET_TABLES[entity_type]
            schema_prefix = f"{snowflake_cfg['schema_raw']}."
            load_block = load_block.replace(f" {table_name}", f" {schema_prefix}{table_name}")

        execute_sql_statements(cur, load_block)

        conn.commit()
        logging.info("✅ All statements executed and committed successfully.")

    except Exception as e:
        logging.error(f"❌ Ingestion failed: {e}", exc_info=True)
        raise
    finally:
        duration = (datetime.now() - start_time).total_seconds()
        logging.info(f"⏱️ Duration: {duration:.2f}s")
        logging.info(f"📄 Log file saved at: {log_file}")

def get_available_entities() -> List[str]:
    """Get list of available entity types for ingestion."""
    return list(Config.SQL_PATHS.keys())

# ==========================================================
# Main
# ==========================================================
def main():
    """Main entry point for the data ingestion script."""
    parser = argparse.ArgumentParser(description="Ingest data into Snowflake.")
    parser.add_argument(
        "entities", 
        nargs="*", 
        help="Entity types to ingest (e.g., sales customer product). If none specified, all entities will be processed."
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Perform a full refresh instead of incremental load"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available entity types"
    )
    
    args = parser.parse_args()
    
    # List available entities if requested
    if args.list:
        print("\nAvailable entity types:")
        for entity in get_available_entities():
            print(f"  - {entity}")
        return
    
    # Determine which entities to process
    entities_to_process = args.entities if args.entities else get_available_entities()
    
    # Validate entities
    available_entities = get_available_entities()
    invalid_entities = [e for e in entities_to_process if e not in available_entities]
    if invalid_entities:
        print(f"❌ Error: Unknown entity types: {', '.join(invalid_entities)}")
        print("\nAvailable entity types:")
        for entity in available_entities:
            print(f"  - {entity}")
        sys.exit(1)
    
    # Create a single Snowflake connection for all entities
    cfg = load_config()
    conn = connect_snowflake(cfg)
    success = True
    try:
        for entity in entities_to_process:
            try:
                ingest_entity(entity, args.full_refresh, conn)
            except Exception as e:
                print(f"❌ Error processing {entity}: {str(e)}")
                success = False
    finally:
        conn.close()
        logging.info("🔒 Connection closed.")
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()