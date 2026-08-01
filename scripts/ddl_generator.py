import yaml
from pathlib import Path

def load_yaml_tables(base_dir):
    """Recursively load all YAML table definitions"""
    for yaml_path in Path(base_dir).rglob("*.yaml"):
        with open(yaml_path, "r") as f:
            yield yaml.safe_load(f), yaml_path

def render_create_table_sql(table_def, database, schema):
    """Generate CREATE TABLE SQL directly without using Jinja template"""
    table_name = table_def["table_name"]
    columns = table_def["columns"]
    cluster_by = table_def.get("cluster_by") or table_def.get("clustering_keys")

    primary_key = table_def.get("primary_key") or {}
    pk_columns = []
    if isinstance(primary_key, dict):
        pk_columns = primary_key.get("columns", []) or []
    elif isinstance(primary_key, list):
        pk_columns = primary_key
    pk_columns = [str(col) for col in pk_columns]

    # Start with CREATE TABLE
    sql = [f"CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} ("]

    # Add columns
    pk_name_set = set(pk_columns)
    column_defs = []
    for col in columns:
        col_sql = f"  {col['name']} {col['type']}"
        if col.get('not_null') or col.get('nullable') is False or col['name'] in pk_name_set:
            col_sql += " NOT NULL"
        column_defs.append(col_sql)

    if pk_columns:
        column_defs.append(f"  PRIMARY KEY ({', '.join(pk_columns)})")

    sql.append(",\n".join(column_defs))
    sql.append(")")

    # Add CLUSTER BY if specified
    if cluster_by:
        cluster_cols = cluster_by if isinstance(cluster_by, list) else [cluster_by]
        sql.append(f"CLUSTER BY ({', '.join(cluster_cols)})")

    # End with semicolon
    sql.append(";")

    return "\n".join(sql)
