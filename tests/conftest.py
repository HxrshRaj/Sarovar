import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
for p in (os.path.join(ROOT, "airflow", "include"), ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)
os.environ.setdefault("CATALOG_PATH", os.path.join(ROOT, "docs", "catalog.yml"))


def _stack_up():
    try:
        import psycopg2
        from sarovar.config import OLTP_DSN, s3_client
        psycopg2.connect(OLTP_DSN, connect_timeout=2).close()
        s3_client().list_buckets()
        return True
    except Exception:
        return False


def pytest_collection_modifyitems(config, items):
    if _stack_up():
        return
    skip = pytest.mark.skip(reason="core stack not running (make up)")
    for it in items:
        if "integration" in it.keywords:
            it.add_marker(skip)
