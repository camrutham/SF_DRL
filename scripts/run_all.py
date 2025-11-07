import subprocess
import sys

# Run schema_orchestrator.py first
print("Running schema_orchestrator.py...")
schema_result = subprocess.run([sys.executable, "scripts/schema_orchestrator.py"], check=True)

# Run ingest_data.py next
print("Running ingest_data.py...")
ingest_result = subprocess.run([sys.executable, "scripts/ingest_data.py"], check=True)

print("All steps completed.")
