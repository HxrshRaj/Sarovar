import pyarrow as pa
import yaml

from .config import CATALOG_PATH

_ARROW = {"bigint": pa.int64(), "varchar": pa.string(), "timestamp(3)": pa.timestamp("ms"),
          "integer": pa.int32(), "double": pa.float64()}


def load(path=None):
    with open(path or CATALOG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def source_tables(cat=None):
    return list((cat or load())["source"].keys())


def arrow_schema(table, cat=None):
    cols = (cat or load())["source"][table]["columns"]
    return pa.schema([(c["name"], _ARROW[c["type"]]) for c in cols])


def column_names(table, cat=None):
    return [c["name"] for c in (cat or load())["source"][table]["columns"]]


def pii_columns(cat=None):
    """{table: [pii column names]} across all layers."""
    cat = cat or load()
    out = {}
    for layer in ("source", "curated", "views"):
        for t, spec in cat.get(layer, {}).items():
            cols = [c["name"] for c in spec["columns"] if c.get("pii")]
            if cols:
                out[t] = cols
    return out
