#!/usr/bin/env python3
"""
snowflake_transforms.py
=======================
Direct Snowflake Native Transformation Engine (Stream-Based CDC Architecture):
Executes pure SQL transformation models directly from the `transform/` directory
using Snowflake Change Data Capture (CDC) Streams for zero full-table-scan incremental processing:

  - Canonical Models (Stream CDC DML):
      transform/canonical/domaindata/contract_dim.sql (CONTRACT_RAW_STREAM)
      transform/canonical/masterdata/customer_dim.sql (CUSTOMER_RAW_STREAM)
      transform/canonical/masterdata/product_dim.sql (PRODUCT_RAW_STREAM)
      transform/canonical/sales/sales_fact.sql (DIRECT & INDIRECT SALE LINES RAW STREAMS)

  - Curated Models (Stream CDC Rollup DML):
      transform/curated/customer_analytics.sql (CUSTOMER_DIM_STREAM, SALES_FACT_STREAM_CUSTOMER)
      transform/curated/product_performance.sql (PRODUCT_DIM_STREAM, SALES_FACT_STREAM_PRODUCT)
      transform/curated/sales_summary.sql (SALES_FACT_STREAM_SUMMARY)

  - Staging Views & CDC Streams:
      transform/raw/masterdata/streams.sql
      transform/raw/domaindata/streams.sql
      transform/raw/sales/streams.sql
      transform/canonical/masterdata/streams.sql
      transform/canonical/sales/streams.sql

Features:
  - High-Performance CDC: Queries only changed rows recorded in Snowflake Streams.
  - Skip on Empty Streams: Evaluates SYSTEM$STREAM_HAS_DATA() before executing DML; skips empty streams with 0 warehouse cost.
  - Full Refresh (--full-refresh): Truncates target table, queries base table, and resets stream offsets.
  - Dependency-Aware Ordering: Execution order is a topological sort of
      (a) SQL lineage (tables/streams a model reads that another model produces),
      (b) module rules in templates/module_dependencies.csv (e.g. curated -> canonical),
      (c) optional '-- depends_on: TABLE' header hints in model SQL.
    A failed model blocks only its downstream dependents; independent models still run.

Usage:
  python scripts/snowflake_transforms.py --show-deps            # Print dependency graph & execution order
  python scripts/snowflake_transforms.py --deploy               # Deploy staging views & CDC streams
  python scripts/snowflake_transforms.py --stage all            # Execute all transformations (Stream CDC incremental)
  python scripts/snowflake_transforms.py --stage all --full-refresh # Full refresh (truncate & reload from base)
  python scripts/snowflake_transforms.py --stage canonical      # Execute canonical stage only
  python scripts/snowflake_transforms.py --stage curated        # Execute curated stage only
  python scripts/snowflake_transforms.py --dry-run              # Preview SQL statements without executing
"""

import argparse
import csv
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TRANSFORM_DIR = PROJECT_ROOT / "transform"
TEMPLATES_DIR = PROJECT_ROOT / "templates"
LOG_DIR = PROJECT_ROOT / "logs"

from snowflake_utils import connect_snowflake, get_config


def setup_logging(label: str = "transforms") -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{label}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logger = logging.getLogger()
    for h in list(logger.handlers):
        logger.removeHandler(h)
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return log_file


def format_sql(raw_sql: str, cfg: dict) -> str:
    snow_cfg = cfg["snowflake"]
    return (
        raw_sql.replace("{DATABASE}", snow_cfg["database"])
        .replace("{SCHEMA_RAW}", snow_cfg["schema_raw"])
        .replace("{SCHEMA_CANONICAL}", snow_cfg["schema_canonical"])
        .replace("{SCHEMA_CURATED}", snow_cfg["schema_curated"])
        .replace("{WAREHOUSE}", snow_cfg.get("warehouse", "COMPUTE_WH"))
    )


def load_stream_mappings(cfg: dict = None) -> List[dict]:
    """Load stream to base table mappings from templates/stream_mappings.csv."""
    mapping_file = TEMPLATES_DIR / "stream_mappings.csv"
    if not mapping_file.exists():
        logging.warning(f"⚠️ Stream mapping template not found: {mapping_file}")
        return []

    mappings = []
    with open(mapping_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("stream_name") and row.get("base_table_name"):
                mappings.append(row)
    return mappings


def transform_sql_for_full_refresh(sql_text: str, stream_mappings: List[dict] = None) -> str:
    """
    Transform stream-based CDC SQL into full-refresh SQL against base tables.
    Dynamically swaps stream names to base table names using templates/stream_mappings.csv.
    """
    if stream_mappings is None:
        stream_mappings = load_stream_mappings()

    sql = sql_text

    # 1. Dynamically swap stream names to base table names from template CSV
    for mapping in stream_mappings:
        stream_name = mapping["stream_name"].strip()
        base_table = mapping["base_table_name"].strip()
        sql = sql.replace(stream_name, base_table)

    # 2. Swap the stream filter (METADATA$ACTION = 'INSERT') to a no-op (WHERE 1=1)
    sql = re.sub(
        r"WHERE\s+([a-zA-Z0-9_]+\.)?METADATA\$ACTION\s*=\s*'INSERT'",
        "WHERE 1=1",
        sql,
        flags=re.IGNORECASE,
    )

    return sql


def extract_statements(sql_text: str) -> List[str]:
    """Split SQL file into executable statements handling comments and semicolons."""
    raw_stmts = [s.strip() for s in sql_text.split(";") if s.strip()]
    clean_stmts = []
    for s in raw_stmts:
        lines = [l for l in s.split("\n") if not l.strip().startswith("--") and l.strip()]
        if lines:
            clean_stmts.append("\n".join(lines))
    return clean_stmts


def execute_sql_file(cur, file_path: Path, cfg: dict, dry_run: bool = False) -> None:
    """Read, format, and execute DDL statements (views, streams) from a SQL file."""
    if not file_path.exists():
        logging.warning(f"File not found: {file_path}")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        raw_sql = f.read()

    formatted_sql = format_sql(raw_sql, cfg)
    stmts = extract_statements(formatted_sql)

    rel_path = file_path.relative_to(PROJECT_ROOT)
    logging.info(f"📄 Processing {rel_path} ({len(stmts)} statements)...")
    for stmt in stmts:
        summary_line = stmt.strip().split("\n")[0][:80]
        if dry_run:
            logging.info(f"[DRY-RUN]\n{stmt}\n")
        else:
            logging.info(f"⏳ Executing: {summary_line}...")
            cur.execute(stmt)
            logging.info(f"✅ Executed: {summary_line}")


# ==========================================================
# Deployment Manifest & Filesystem Model Auto-Discovery
# Decodes models, stages, schemas, tables, and CDC streams
# directly from transform/ directory hierarchy and SQL files
# ==========================================================
def load_deploy_manifest() -> List[Path]:
    """Load deployment file paths dynamically from templates/deploy_manifest.csv."""
    manifest_file = TEMPLATES_DIR / "deploy_manifest.csv"
    if not manifest_file.exists():
        logging.warning(f"⚠️ Deploy manifest template not found: {manifest_file}")
        return []

    entries = []
    with open(manifest_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("file_path"):
                step = int(row.get("step", 999))
                entries.append((step, (PROJECT_ROOT / row["file_path"].strip()).resolve()))
    entries.sort(key=lambda x: x[0])
    return [e[1] for e in entries]


def load_module_dependencies() -> List[dict]:
    """
    Load module-level dependencies from templates/module_dependencies.csv.

    Columns: module, depends_on, description
      - module:     path relative to transform/ (e.g. 'canonical/sales', or a whole layer like 'curated').
                    A rule applies to the module itself and every sub-module beneath it.
      - depends_on: ';'-separated module paths or layers (e.g. 'canonical/masterdata;canonical/domaindata',
                    or 'canonical' for the whole layer).
    """
    dep_file = TEMPLATES_DIR / "module_dependencies.csv"
    if not dep_file.exists():
        logging.warning(f"⚠️ Module dependency template not found: {dep_file}")
        return []

    rules = []
    with open(dep_file, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            module = (row.get("module") or "").strip().strip("/").lower()
            if not module:
                continue
            deps = [d.strip().strip("/").lower() for d in (row.get("depends_on") or "").split(";") if d.strip()]
            rules.append({"module": module, "depends_on": deps})
    return rules


def _module_matches(module: str, selector: str) -> bool:
    """True if `module` equals `selector` or lives beneath it (e.g. 'canonical/sales' matches 'canonical')."""
    return module == selector or module.startswith(selector + "/")


def _strip_sql_comments(sql_text: str) -> str:
    sql_text = re.sub(r"/\*.*?\*/", " ", sql_text, flags=re.DOTALL)
    return "\n".join(line.split("--", 1)[0] for line in sql_text.splitlines())


def _sort_key(model: dict):
    return (STAGE_ORDER.index(model["stage"]) if model["stage"] in STAGE_ORDER else 99,
            model["module"], model["file"].name.lower())


STAGE_ORDER = ["canonical", "curated"]
SCHEMA_REF_PATTERN = re.compile(r"\{SCHEMA_[A-Z_]+\}\.([A-Za-z0-9_]+)", re.IGNORECASE)
DEPENDS_ON_HINT_PATTERN = re.compile(r"--\s*depends_on\s*:\s*(.+)", re.IGNORECASE)


def _scan_model_files(cfg: dict) -> List[dict]:
    """Scan transform/<stage>/ for model SQL files and decode their metadata (no ordering)."""
    snow_cfg = cfg["snowflake"]
    db = snow_cfg["database"]

    stream_map = {m["stream_name"].upper(): m for m in load_stream_mappings(cfg)}
    deploy_files = {p.resolve() for p in load_deploy_manifest()}

    models = []
    for stg in STAGE_ORDER:
        stg_dir = TRANSFORM_DIR / stg
        if not stg_dir.exists():
            continue

        for sql_file in stg_dir.rglob("*.sql"):
            # Exclude infrastructure DDL files (streams, views in deploy_manifest, or named streams.sql)
            if sql_file.resolve() in deploy_files:
                continue
            if sql_file.name.lower() in ("streams.sql", "create_streams.sql") or "streams" in [p.lower() for p in sql_file.parts]:
                continue

            try:
                content = sql_file.read_text(encoding="utf-8")
            except Exception as e:
                logging.warning(f"Could not read SQL file {sql_file}: {e}")
                continue

            table_name = sql_file.stem.upper()
            module = sql_file.parent.relative_to(TRANSFORM_DIR).as_posix().lower()
            code = _strip_sql_comments(content)

            # 1. CDC streams referenced by the model (+ reset DDL for full refresh)
            qualified_streams, reset_sqls = [], []
            for s_name, m_info in stream_map.items():
                if re.search(rf"\b{re.escape(s_name)}\b", code, re.IGNORECASE):
                    layer = m_info.get("schema_layer", "raw")
                    s_schema_name = snow_cfg.get(f"schema_{layer}", f"sales_{layer}")
                    base_table = m_info["base_table_name"]
                    desc = m_info.get("description", f"CDC stream on {base_table}")
                    qualified_streams.append(f"{db}.{s_schema_name}.{m_info['stream_name']}")
                    reset_sqls.append(
                        f"CREATE OR REPLACE STREAM {db}.{s_schema_name}.{m_info['stream_name']} "
                        f"ON TABLE {db}.{s_schema_name}.{base_table} APPEND_ONLY = FALSE "
                        f"COMMENT = '{desc}';"
                    )

            # 2. Tables read by the model (streams resolved to their base tables)
            reads = set()
            for ref in SCHEMA_REF_PATTERN.findall(code):
                ref = ref.upper()
                reads.add(stream_map[ref]["base_table_name"].upper() if ref in stream_map else ref)
            reads.discard(table_name)

            # 3. Optional explicit hint in SQL header:  -- depends_on: TABLE_A, TABLE_B
            hints = set()
            for hint in DEPENDS_ON_HINT_PATTERN.findall(content):
                hints.update(h.strip().upper() for h in hint.split(",") if h.strip())

            models.append({
                "table": table_name,
                "file": sql_file,
                "schema_key": f"schema_{stg}",
                "stage": stg,
                "module": module,
                "streams": qualified_streams,
                "reset_streams_sql": reset_sqls,
                "reads": reads,
                "depends_on_hints": hints,
            })
    return models


def build_dependency_graph(models: List[dict]) -> dict:
    """
    Resolve each model's upstream models from three sources:
      1. SQL lineage  - tables/streams the model reads that another model produces.
      2. Module rules - templates/module_dependencies.csv (e.g. curated -> canonical).
      3. SQL hints    - optional '-- depends_on: TABLE' header comments.
    Returns {table_name: {"upstream": set, "reasons": {upstream: [reason, ...]}}}.
    """
    producers = {m["table"]: m for m in models}
    known_modules = {m["module"] for m in models}
    rules = load_module_dependencies()

    for rule in rules:
        for dep in rule["depends_on"]:
            if not any(_module_matches(km, dep) for km in known_modules):
                logging.warning(f"⚠️ module_dependencies.csv: '{rule['module']}' depends on unknown module '{dep}'")

    graph = {m["table"]: {"upstream": set(), "reasons": {}} for m in models}

    def add_edge(model_tbl: str, upstream_tbl: str, reason: str):
        if model_tbl == upstream_tbl:
            return
        node = graph[model_tbl]
        node["upstream"].add(upstream_tbl)
        node["reasons"].setdefault(upstream_tbl, []).append(reason)

    for m in models:
        for tbl in m["reads"]:
            if tbl in producers:
                add_edge(m["table"], tbl, "sql")
        for tbl in m["depends_on_hints"]:
            if tbl in producers:
                add_edge(m["table"], tbl, "hint")
            else:
                logging.warning(f"⚠️ {m['table']}: depends_on hint '{tbl}' does not match any model")
        for rule in rules:
            if not _module_matches(m["module"], rule["module"]):
                continue
            for dep in rule["depends_on"]:
                for up in models:
                    if _module_matches(up["module"], dep) and not _module_matches(up["module"], m["module"]):
                        add_edge(m["table"], up["table"], f"module:{rule['module']}->{dep}")
    return graph


def topological_sort(models: List[dict], graph: dict) -> List[dict]:
    """Kahn's algorithm with deterministic tie-breaking (stage, module, file). Raises on cycles."""
    by_table = {m["table"]: m for m in models}
    remaining = {t: set(graph[t]["upstream"]) & by_table.keys() for t in by_table}
    ordered = []
    while remaining:
        ready = sorted((by_table[t] for t, ups in remaining.items() if not ups), key=_sort_key)
        if not ready:
            cycle = ", ".join(f"{t} <- {sorted(u)}" for t, u in remaining.items())
            raise ValueError(f"Circular model/module dependency detected: {cycle}")
        nxt = ready[0]
        ordered.append(nxt)
        del remaining[nxt["table"]]
        for ups in remaining.values():
            ups.discard(nxt["table"])
    return ordered


def discover_models(stage: Optional[str] = None, cfg: dict = None) -> List[dict]:
    """
    Auto-discover transformation models from transform/ and return them in dependency order.
    - Stage, schema, module and table name are decoded from the file path.
    - CDC streams and upstream tables are decoded from the SQL text.
    - Ordering comes from SQL lineage + templates/module_dependencies.csv (+ optional SQL hints).
    The graph is always built across ALL stages; `stage` only filters the returned list.
    Each model gets 'depends_on' (sorted upstream table names) and 'dependency_reasons'.
    """
    if cfg is None:
        cfg = get_config()
    all_models = _scan_model_files(cfg)
    graph = build_dependency_graph(all_models)
    for m in all_models:
        m["depends_on"] = sorted(graph[m["table"]]["upstream"])
        m["dependency_reasons"] = graph[m["table"]]["reasons"]
    ordered = topological_sort(all_models, graph)
    return [m for m in ordered if stage is None or m["stage"] == stage]


def describe_dependencies(models: List[dict]) -> str:
    """Human-readable execution plan with each model's upstream dependencies."""
    lines = ["Execution plan (dependency order):"]
    for i, m in enumerate(models, 1):
        lines.append(f"  {i}. [{m['module']}] {m['table']}")
        for up in m["depends_on"]:
            lines.append(f"       <- {up}  ({', '.join(m['dependency_reasons'].get(up, []))})")
    return "\n".join(lines)


# Backward compatibility alias
load_models = discover_models


def execute_model(cur, model_spec: dict, cfg: dict, full_refresh: bool = False, dry_run: bool = False) -> str:
    """Read and execute a transformation model SQL file directly against Snowflake using CDC Streams."""
    snow_cfg = cfg["snowflake"]
    db = snow_cfg["database"]
    schema = snow_cfg[model_spec["schema_key"]]
    table = model_spec["table"]
    file_path = model_spec["file"]

    if not file_path.exists():
        raise FileNotFoundError(f"Model file not found: {file_path}")

    rel_path = file_path.relative_to(PROJECT_ROOT)
    logging.info(f"📄 Running model: {table} ({rel_path})...")

    # 1. CDC Stream Check (Incremental only)
    if not full_refresh and model_spec.get("streams"):
        has_any_data = False
        stream_names = [format_sql(s, cfg) for s in model_spec["streams"]]
        if dry_run:
            has_any_data = True
            logging.info(f"[DRY-RUN] Checked SYSTEM$STREAM_HAS_DATA on: {', '.join(stream_names)}")
        else:
            for s_name in stream_names:
                try:
                    cur.execute(f"SELECT SYSTEM$STREAM_HAS_DATA('{s_name}')")
                    row = cur.fetchone()
                    if row and row[0]:
                        has_any_data = True
                        logging.info(f"⚡ CDC Stream {s_name} has new data.")
                        break
                except Exception as stream_err:
                    logging.warning(f"⚠️ Could not check stream {s_name}: {stream_err}. Proceeding with execution.")
                    has_any_data = True
                    break

            if not has_any_data:
                logging.info(f"⏭️ Skipping {table}: No new CDC changes in streams ({', '.join(stream_names)}).")
                return f"SKIPPED: {table} (No CDC stream data)"

    # 2. If full refresh requested, truncate target table first
    if full_refresh:
        truncate_sql = f"TRUNCATE TABLE {db}.{schema}.{table};"
        if dry_run:
            logging.info(f"[DRY-RUN] {truncate_sql}")
        else:
            logging.info(f"🧹 Truncating target table: {db}.{schema}.{table}...")
            cur.execute(truncate_sql)
            logging.info(f"✅ Truncated: {db}.{schema}.{table}")

    with open(file_path, "r", encoding="utf-8") as f:
        raw_sql = f.read()

    formatted_sql = format_sql(raw_sql, cfg)
    if full_refresh:
        formatted_sql = transform_sql_for_full_refresh(formatted_sql)

    stmts = extract_statements(formatted_sql)

    rows_affected = 0
    for stmt in stmts:
        summary_line = stmt.strip().split("\n")[0][:80]
        if dry_run:
            logging.info(f"[DRY-RUN]\n{stmt}\n")
        else:
            logging.info(f"⏳ Executing DML: {summary_line}...")
            cur.execute(stmt)
            if cur.rowcount and cur.rowcount > 0:
                rows_affected += cur.rowcount
            logging.info(f"✅ Executed DML for {table}")

    # 3. If full refresh, reset stream offsets so they start fresh from now
    if full_refresh and model_spec.get("reset_streams_sql"):
        for reset_stmt_tmpl in model_spec["reset_streams_sql"]:
            reset_stmt = format_sql(reset_stmt_tmpl, cfg)
            if dry_run:
                logging.info(f"[DRY-RUN] Reset stream: {reset_stmt}")
            else:
                logging.info(f"🔄 Resetting stream offset: {reset_stmt[:80]}...")
                cur.execute(reset_stmt)
                logging.info("✅ Stream offset reset.")

    return f"SUCCESS: {table} loaded (Full refresh: {full_refresh}). Rows affected: {rows_affected}"


def deploy_all(cur, cfg: dict, dry_run: bool = False) -> None:
    """Deploy transformation views and CDC streams dynamically from templates/deploy_manifest.csv."""
    logging.info("🚀 Deploying Snowflake Native Transformation Objects & CDC Streams...")
    deploy_order = load_deploy_manifest()

    for sql_file in deploy_order:
        execute_sql_file(cur, sql_file, cfg, dry_run=dry_run)

    logging.info("✅ All Snowflake Native transformation objects & CDC streams successfully deployed.")


def run_models(cur, cfg: dict, stage: Optional[str] = None, full_refresh: bool = False, dry_run: bool = False) -> str:
    """
    Execute models in dependency order.
    - If a model fails, every model downstream of it is BLOCKED; independent models still run.
    - Upstream models outside the selected stage are assumed to be already loaded.
    - Raises RuntimeError at the end if any model failed or was blocked.
    """
    models = discover_models(stage=stage, cfg=cfg)
    selected = {m["table"] for m in models}
    logging.info(describe_dependencies(models))

    failed, results = {}, []
    for model in models:
        external = [u for u in model["depends_on"] if u not in selected]
        if external:
            logging.info(f"ℹ️ {model['table']}: upstream {external} outside stage '{stage}' - assumed already loaded.")

        broken = [u for u in model["depends_on"] if u in failed]
        if broken:
            failed[model["table"]] = f"blocked by {broken}"
            msg = f"BLOCKED: {model['table']} (upstream failed: {', '.join(broken)})"
            logging.error(f"⛔ {msg}")
            results.append(msg)
            continue

        try:
            results.append(execute_model(cur, model, cfg, full_refresh=full_refresh, dry_run=dry_run))
        except Exception as exc:
            failed[model["table"]] = str(exc)
            msg = f"FAILED: {model['table']} ({exc})"
            logging.error(f"❌ {msg}", exc_info=True)
            results.append(msg)

    summary = "\n".join(results)
    if failed:
        raise RuntimeError(f"{len(failed)} model(s) failed or were blocked:\n{summary}")
    return summary


def run_canonical(cur, cfg: dict, full_refresh: bool = False, dry_run: bool = False) -> str:
    """Execute canonical layer models in dependency order."""
    logging.info(f"🔄 Running Stage 2: Canonical Transformations (full_refresh={full_refresh})...")
    summary = run_models(cur, cfg, stage="canonical", full_refresh=full_refresh, dry_run=dry_run)
    logging.info(f"✅ Canonical Stage Complete:\n{summary}")
    return summary


def run_curated(cur, cfg: dict, full_refresh: bool = False, dry_run: bool = False) -> str:
    """Execute curated layer models in dependency order (canonical upstreams assumed loaded)."""
    logging.info(f"🔄 Running Stage 3: Curated Transformations (full_refresh={full_refresh})...")
    summary = run_models(cur, cfg, stage="curated", full_refresh=full_refresh, dry_run=dry_run)
    logging.info(f"✅ Curated Stage Complete:\n{summary}")
    return summary


def run_full_pipeline(cur, cfg: dict, full_refresh: bool = False, dry_run: bool = False) -> str:
    """Execute every model across canonical and curated as one dependency graph."""
    logging.info(f"🚀 Running Complete Transformation Pipeline (full_refresh={full_refresh})...")
    res = run_models(cur, cfg, stage=None, full_refresh=full_refresh, dry_run=dry_run)
    summary = f"=== FULL TRANSFORMATION PIPELINE COMPLETE (Full refresh: {full_refresh}) ===\n{res}"
    logging.info(f"🏁 Pipeline Finished:\n{summary}")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Snowflake Native Data Transformation Engine (Stream-Based CDC Architecture)."
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Deploy Snowflake transformation views and streams directly from transform/.",
    )
    parser.add_argument(
        "--stage",
        choices=["all", "canonical", "curated", "transforms"],
        default="all",
        help="Which transformation stage to run: 'canonical', 'curated', 'transforms' (both), or 'all' (default).",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Perform a full refresh (truncate targets and reload all data from base tables).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview SQL statements without executing.",
    )
    parser.add_argument(
        "--show-deps",
        action="store_true",
        help="Print the resolved model/module dependency graph and execution order, then exit.",
    )
    args = parser.parse_args()

    setup_logging("transforms")
    cfg = get_config()

    stage = args.stage
    if stage in ("transforms", "dbt"):
        stage = "all"

    if args.show_deps:
        print(describe_dependencies(discover_models(stage=None if stage == "all" else stage, cfg=cfg)))
        return 0

    if args.dry_run:
        logging.info("ℹ️ Running in DRY-RUN mode.")

    if not args.dry_run:
        conn = connect_snowflake()
        cur = conn.cursor()
    else:
        conn = None
        cur = None

    try:
        if args.deploy:
            deploy_all(cur, cfg, dry_run=args.dry_run)

        if not args.deploy:
            if stage == "canonical":
                run_canonical(cur, cfg, full_refresh=args.full_refresh, dry_run=args.dry_run)
            elif stage == "curated":
                run_curated(cur, cfg, full_refresh=args.full_refresh, dry_run=args.dry_run)
            elif stage == "all":
                run_full_pipeline(cur, cfg, full_refresh=args.full_refresh, dry_run=args.dry_run)

        if conn and not args.dry_run:
            conn.commit()
    except Exception as exc:
        logging.error(f"❌ Transformation execution failed: {exc}", exc_info=True)
        return 1
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    logging.info("🏁 Snowflake Native Transformations completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
