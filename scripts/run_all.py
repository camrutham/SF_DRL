import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent


def run_step(step_name: str, command: list[str]) -> None:
    print(f"\n===== Running {step_name} =====")
    result = subprocess.run(command, cwd=str(ROOT_DIR), check=False)
    if result.returncode != 0:
        raise SystemExit(f"{step_name} failed with exit code {result.returncode}")


# 1. Create schemas / setup objects
run_step("schema_orchestrator.py", [sys.executable, "scripts/schema_orchestrator.py"])

# 2. Load raw data from source to Snowflake raw tables
run_step("ingest_data.py", [sys.executable, "scripts/ingest_data.py"])

# 3. Run dbt models for raw-to-canonical load
run_step(
    "dbt run",
    [
        "dbt",
        "run",
        "--profiles-dir",
        str(ROOT_DIR),
        "--project-dir",
        str(ROOT_DIR),
        "--select",
        "sf_e2e.staging.stg_direct_sales",
        "sf_e2e.staging.stg_indirect_sales",
        "sf_e2e.canonical.dim_customer",
        "sf_e2e.canonical.dim_product",
        "sf_e2e.canonical.dim_contract",
        "sf_e2e.canonical.sales_fact_direct",
        "sf_e2e.canonical.sales_fact_indirect",
        "sf_e2e.canonical.sales_fact",
        "sf_e2e.curated.customer_analytics",
        "sf_e2e.curated.product_performance",
        "sf_e2e.curated.sales_summary",
    ],
)

print("\nAll workflow steps completed successfully.")
