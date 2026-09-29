"""Against the live synthetic OLTP data: NO real PII value (every phone/email/vpa/name/upi_ref in the source DB)
may appear in any prompt, embedding input, or audit log produced by the AI layer."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ai"))

import psycopg2
import pytest

import sarovar_ai  # noqa: F401
from sarovar.config import OLTP_DSN
from sarovar_ai import metadata
from sarovar_ai.text2sql import build_messages

pytestmark = pytest.mark.integration


def pii_values():
    conn = psycopg2.connect(OLTP_DSN)
    cur = conn.cursor()
    vals = set()
    cur.execute("SELECT full_name, phone, email, vpa FROM users")
    for row in cur.fetchall():
        vals.update(row)
    cur.execute("SELECT contact_phone FROM merchants")
    vals.update(r[0] for r in cur.fetchall())
    cur.execute("SELECT upi_ref FROM transactions")
    vals.update(r[0] for r in cur.fetchall())
    conn.close()
    return {v for v in vals if v}


def test_no_real_pii_value_in_any_prompt_or_embedding_input():
    values = pii_values()
    assert len(values) > 10000  # thousands of users x4 + merchants + every upi_ref (300k with the default seed)
    texts = []
    all_keys = list(metadata.all_tables().keys())
    for i in range(len(all_keys)):
        for m in build_messages("How many payments were attempted on 2026-09-15?", all_keys[: i + 1]):
            texts.append(m["content"])
    texts += [d[2] for d in metadata.embedding_docs()]
    blob = "\n".join(texts)
    hits = [v for v in values if v in blob]
    assert hits == [], f"{len(hits)} real PII values found in prompt/embedding text"
    # substring-safe token check for short ids, too
    assert not any(tok in values for tok in blob.replace(",", " ").replace("(", " ").split())
