"""Sarovar AI layer: PII-safe text-to-SQL, SQL guard, data discovery, MCP server. All data is synthetic."""
import os
import sys

_here = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.join(_here, "..", "..", "airflow", "include"), "/opt/sarovar/lib"):
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)
os.environ.setdefault("CATALOG_PATH", next((p for p in (os.path.join(_here, "..", "..", "docs", "catalog.yml"), "/opt/sarovar/catalog.yml") if os.path.exists(p)), "docs/catalog.yml"))
