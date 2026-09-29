"""PII guardrails: no PII value may appear in any prompt, embedding input, or log."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ai"))

import pytest

import sarovar_ai  # noqa: F401
from sarovar_ai import llm as llm_mod
from sarovar_ai import metadata
from sarovar_ai.audit import AuditLog
from sarovar_ai.llm import OllamaClient
from sarovar_ai.redact import PromptPIIError, Redactor
from sarovar_ai.text2sql import Text2SQL, build_messages

PII_SAMPLES = ["9876543210", "+91 98765 43210", "+919812345678", "priya.sharma1@example.org", "priya1@sarovar",
               "403912345678", "ABCDE1234F", "1234 5678 9012"]


def test_redactor_catches_each_identifier_kind():
    r = Redactor()
    for v in PII_SAMPLES:
        clean, f = r.redact(f"look up {v} please")
        assert v not in clean and f, v


def test_redactor_leaves_plain_analytics_text_alone():
    r = Redactor()
    for q in ["How many payments failed on 2026-09-05?", "GMV between 2026-09-01 and 2026-09-07 in rupees",
              "merchant_id 63 refund_amount_paise > 1000000", "top 5 merchants by 100.0 * failed / attempts"]:
        clean, f = r.redact(q)
        assert clean == q and not f, (q, f)


def test_known_names_are_redacted():
    r = Redactor(known_names=["Priya Sharma", "Rahul Verma"])
    clean, f = r.redact("payments by priya sharma and Rahul Verma last week")
    assert "riya" not in clean and "ahul" not in clean and f == ["NAME", "NAME"]


def test_assert_clean_raises():
    with pytest.raises(PromptPIIError):
        Redactor().assert_clean("call 9876543210")


def test_metadata_never_contains_pii_columns():
    cat = metadata.all_tables()
    pii = {c["name"] for spec in cat.values() for c in spec["columns"] if c.get("pii")}
    assert pii == {"full_name", "phone", "email", "vpa", "contact_phone", "upi_ref"}
    blobs = [metadata.table_doc(k, v) for k, v in cat.items()] + [d[2] for d in metadata.embedding_docs()]
    for blob in blobs:
        for col in pii:
            assert col not in blob, f"PII column {col} leaked into LLM/embedding metadata"
    assert not any(d[1] in pii for d in metadata.embedding_docs())


class FakeResp:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


@pytest.fixture
def captured(monkeypatch):
    calls = []

    def fake_post(url, json=None, timeout=None):
        calls.append({"url": url, "json": json})
        if url.endswith("/api/embed"):
            return FakeResp({"embeddings": [[0.0] * 3 for _ in json["input"]]})
        return FakeResp({"message": {"content": "```sql\nSELECT count(*) AS n FROM lake.raw.transactions WHERE dt = '2026-09-15' LIMIT 1\n```"},
                         "eval_count": 1, "prompt_eval_count": 1, "total_duration": 1})
    monkeypatch.setattr(llm_mod.requests, "post", fake_post)
    return calls


def test_client_refuses_pii_in_chat_and_embeddings(captured):
    c = OllamaClient(Redactor())
    with pytest.raises(PromptPIIError):
        c.chat([{"role": "user", "content": "phone 9876543210"}])
    with pytest.raises(PromptPIIError):
        c.embed(["email a.b@example.org"])
    assert captured == []  # nothing left the process


class FakeTrino:
    def cursor(self):
        return self

    def execute(self, q):
        pass

    def fetchall(self):
        return [[json.dumps({"inputTableColumnInfos": [{"table": {"schemaTable": {"schema": "raw", "table": "transactions"}},
                             "estimate": {"outputRowCount": 100}}]})]]


def test_end_to_end_pipeline_sends_no_pii_and_logs_none(captured, monkeypatch, tmp_path):
    from sarovar_ai import discovery
    monkeypatch.setattr(discovery, "retrieve", lambda llm, q, k=3, **kw: [{"table": "raw.transactions", "score": 1.0}, {"table": "raw.users", "score": 0.9}])
    red = Redactor(known_names=["Priya Sharma"])
    audit = AuditLog(path=str(tmp_path / "audit.jsonl"), redactor=red)
    t2s = Text2SQL(FakeTrino(), llm=OllamaClient(red, record=True), redactor=red, audit=audit)
    q = ("How many payments did priya sharma make on 2026-09-15? her phone is 9876543210, "
         "email priya.sharma1@example.org, vpa priya1@sarovar, ref 403912345678")
    out = t2s.answer(q, execute=False)
    sent = json.dumps([c["json"] for c in captured])
    for v in ["9876543210", "priya.sharma1@example.org", "priya1@sarovar", "403912345678", "priya sharma", "Priya Sharma"]:
        assert v not in sent, f"{v} reached the model"
    assert set(out["pii_redacted_from_question"]) >= {"PHONE", "EMAIL", "VPA", "UPI_REF", "NAME"}
    logs = json.dumps(audit.records) + open(tmp_path / "audit.jsonl").read()
    for v in ["9876543210", "priya.sharma1@example.org", "priya1@sarovar", "403912345678", "priya sharma"]:
        assert v not in logs.lower(), f"{v} reached the audit log"
    assert out["guard"]["ok"]


def test_prompt_contains_only_metadata_and_question():
    msgs = build_messages("How many payments on 2026-09-15?", ["raw.transactions", "raw.users"])
    text = "\n".join(m["content"] for m in msgs)
    assert "lake.raw.users" in text and "phone" not in text and "upi_ref" not in text
    Redactor().assert_clean(text)  # the full prompt passes the redactor with zero findings


def test_generated_sql_touching_pii_is_blocked_before_execution(captured, monkeypatch):
    from sarovar_ai import discovery
    monkeypatch.setattr(discovery, "retrieve", lambda *a, **k: [{"table": "raw.users", "score": 1.0}])

    def post(url, json=None, timeout=None):
        return FakeResp({"message": {"content": "```sql\nSELECT phone FROM lake.raw.users LIMIT 5\n```"}, "eval_count": 1, "prompt_eval_count": 1, "total_duration": 1})
    monkeypatch.setattr(llm_mod.requests, "post", post)
    t2s = Text2SQL(FakeTrino(), audit=AuditLog(path=None))
    out = t2s.answer("show phone numbers of users", execute=True, repair=False)
    assert out["guard"]["ok"] is False and out["rows"] is None
    assert any("PII" in e for e in out["guard"]["errors"])
