"""Generate docs/data-dictionary.md from docs/catalog.yml (run: make docs). Keeps the catalog and docs in sync."""
import os
import sys

import yaml

ROOT = os.path.join(os.path.dirname(__file__), "..")


def main():
    cat = yaml.safe_load(open(os.path.join(ROOT, "docs", "catalog.yml"), encoding="utf-8"))
    out = ["# Data dictionary", "",
           "_Generated from [`catalog.yml`](catalog.yml) by `scripts/gen_docs.py` - do not edit by hand. All data is synthetic._", ""]
    layers = [("source", "raw", "Raw layer (mirrors OLTP tables; Parquet, partitioned by `dt`)"),
              ("curated", "curated", "Curated layer (Spark, Parquet)"),
              ("views", "analytics", "Analyst views (Trino views)")]
    for key, schema, title in layers:
        out += [f"## {title}", ""]
        for t, spec in cat[key].items():
            part = "dt" if key == "source" else spec.get("partition_column")
            out += [f"### `lake.{schema}.{t}`", "", spec["description"], "",
                    f"- **Owner:** {spec['owner']}" + (f" | **Partition column:** `{part}`" if part else ""), "",
                    "| Column | Type | PII | Description |", "|---|---|---|---|"]
            for c in spec["columns"]:
                pii = f"**PII** ({c.get('pii_class', 'pii')})" if c.get("pii") else ""
                out.append(f"| `{c['name']}` | {c['type']} | {pii} | {c['description']} |")
            if key == "source":
                out.append("| `dt` | varchar |  | Partition: creation date `YYYY-MM-DD` (directory name, derived from `created_at`) |")
            out.append("")
    open(os.path.join(ROOT, "docs", "data-dictionary.md"), "w", encoding="utf-8").write("\n".join(out))
    print("wrote docs/data-dictionary.md")


if __name__ == "__main__":
    sys.exit(main())
