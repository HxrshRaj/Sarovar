"""LLM-safe metadata built from docs/catalog.yml. PII columns are removed entirely: their names, types and
descriptions never reach a prompt or an embedding. Only schema-level metadata is used - never data values."""
from sarovar import catalog

from .config import DATA_END, DATA_START

LAYER_SCHEMA = {"source": "raw", "curated": "curated", "views": "analytics"}


def all_tables(cat=None):
    """{ 'raw.transactions': {...spec, 'schema': 'raw', 'name': 'transactions', 'kind': 'table'|'view'} }"""
    cat = cat or catalog.load()
    out = {}
    for layer, schema in LAYER_SCHEMA.items():
        for name, spec in cat[layer].items():
            s = dict(spec)
            s.update(schema=schema, name=name, kind="view" if layer == "views" else "table", layer=layer)
            cols = [dict(c) for c in spec["columns"]]
            if layer == "source":
                cols.append({"name": "dt", "type": "varchar", "partition": True,
                             "description": "Partition: creation date YYYY-MM-DD (string)"})
            s["columns"] = cols
            s["partition_col"] = "dt" if layer == "source" else spec.get("partition_column")
            out[f"{schema}.{name}"] = s
    return out


def safe_columns(spec):
    return [c for c in spec["columns"] if not c.get("pii")]


def table_doc(key, spec):
    """Prompt/embedding text for one table - PII columns excluded."""
    cols = "\n".join(f"  - {c['name']} ({c['type']}): {c['description']}" for c in safe_columns(spec))
    part = ""
    if spec.get("partition_col"):
        req = "REQUIRED" if spec.get("partition_filter_required") else "recommended"
        part = f"\n  Partition column: {spec['partition_col']} (string 'YYYY-MM-DD'); a literal filter on it is {req}."
    return f"lake.{key} [{spec['kind']}] - {spec['description']}{part}\n{cols}"


def embedding_docs(cat=None):
    """[(table_key, column_or_None, text)] - one doc per table and one per non-PII column."""
    docs = []
    for key, spec in all_tables(cat).items():
        summary = f"{spec['kind']} lake.{key}: {spec['description']} Columns: " + \
            ", ".join(c["name"] for c in safe_columns(spec))
        docs.append((key, None, summary))
        for c in safe_columns(spec):
            docs.append((key, c["name"], f"column {c['name']} of lake.{key} ({spec['description'].split('.')[0]}): {c['description']}"))
    return docs


def coverage_note():
    return f"Data covers payment dates {DATA_START} to {DATA_END}. Amounts are in paise (100 paise = 1 rupee)."
