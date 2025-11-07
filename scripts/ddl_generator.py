import yaml
from pathlib import Path
from jinja2 import Template

def load_yaml_tables(base_dir):
    """Recursively load all YAML table definitions"""
    for yaml_path in Path(base_dir).rglob("*.yaml"):
        with open(yaml_path, "r") as f:
            yield yaml.safe_load(f), yaml_path

def render_create_table_sql(table_def, database, schema):
    """Render SQL from Jinja template"""
    template_path = Path(__file__).resolve().parent.parent / "templates" / "create_table.sql.j2"
    with open(template_path, "r") as tfile:
        template = Template(tfile.read())

    sql = template.render(
        database=database,
        schema=schema,
        table_name=table_def["table_name"],
        columns=table_def["columns"],
        cluster_by=table_def.get("cluster_by")
    )
    return sql.strip()
