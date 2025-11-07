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
    cluster_by = table_def.get("cluster_by")
    
    # Start with CREATE TABLE
    sql = [f"CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} ("]
    
    # Add columns
    column_defs = []
    for col in columns:
        col_sql = f"  {col['name']} {col['type']}"
        if col.get('not_null'):
            col_sql += " NOT NULL"
        column_defs.append(col_sql)
    
    sql.append(",\n".join(column_defs))
    sql.append(")")
    
    # Add CLUSTER BY if specified
    if cluster_by:
        sql.append(f"CLUSTER BY ({cluster_by})")
    
    # End with semicolon
    sql.append(";")
    
    return "\n".join(sql)
