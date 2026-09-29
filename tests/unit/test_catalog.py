"""Catalog is the single source of truth: generated docs must be in sync, PII tags must exist."""
import os
import subprocess
import sys

from sarovar import catalog

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def test_every_column_has_description_and_type():
    cat = catalog.load()
    for layer in ("source", "curated", "views"):
        for t, spec in cat[layer].items():
            assert spec.get("owner") and spec.get("description"), t
            for c in spec["columns"]:
                assert c.get("description") and c.get("type"), f"{t}.{c['name']}"


def test_expected_pii_columns_tagged():
    pii = catalog.pii_columns()
    assert set(pii["users"]) == {"full_name", "phone", "email", "vpa"}
    assert pii["merchants"] == ["contact_phone"]
    assert pii["transactions"] == ["upi_ref"]


def test_data_dictionary_in_sync_with_catalog():
    path = os.path.join(ROOT, "docs", "data-dictionary.md")
    before = open(path, encoding="utf-8").read()
    subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "gen_docs.py")], check=True, capture_output=True)
    after = open(path, encoding="utf-8").read()
    assert before.replace("\r\n", "\n") == after.replace("\r\n", "\n"), "run `python scripts/gen_docs.py` and commit"
