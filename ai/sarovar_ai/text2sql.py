"""Question -> retrieve tables -> local LLM writes SQL -> guard (static + EXPLAIN) -> optional guarded execution.

PII rules enforced here:
  * the question is redacted before use; the prompt contains ONLY the question and catalog metadata
    (PII columns removed) - never a data value, never a query result
  * the OllamaClient re-checks every outgoing text (defence in depth)
  * generated SQL is validated (no PII columns) before it can run
"""
import re
import time

from . import discovery, metadata, sql_guard
from .audit import AuditLog
from .config import DATA_END, DATA_START, MAX_LIMIT
from .llm import OllamaClient
from .redact import Redactor

SYSTEM = f"""You are a data analyst assistant that writes ONE read-only Trino SQL query.
Rules:
- Use ONLY the tables and columns listed under SCHEMA. Always write the full name, e.g. lake.raw.transactions.
- The query MUST end with LIMIT n (n <= {MAX_LIMIT}).
- Tables marked "a literal filter ... is REQUIRED" MUST have a WHERE predicate on their partition column using string literals,
  e.g. dt = '2026-09-15' or dt BETWEEN '2026-09-01' AND '2026-09-07' (dt is a string 'YYYY-MM-DD', not a date type).
- {metadata.coverage_note()} Amounts are paise; divide by 100.0 for rupees.
- Prefer the analytics views and curated tables over raw tables when they contain the answer.
- Never select or filter personal data. Do not use SELECT *.
- Reply with the SQL only, inside a ```sql code block."""

EXAMPLES = """Example 1
Question: How many payments were attempted on 2026-09-03?
```sql
SELECT count(*) AS payments FROM lake.raw.transactions WHERE dt = '2026-09-03' LIMIT 1
```
Example 2
Question: What was the total GMV in rupees between 2026-09-01 and 2026-09-05?
```sql
SELECT sum(gmv_rupees) AS gmv FROM lake.analytics.daily_gmv WHERE dt BETWEEN '2026-09-01' AND '2026-09-05' LIMIT 1
```"""


def build_messages(question, tables, cat_tables=None):
    cat_tables = cat_tables or metadata.all_tables()
    schema = "\n\n".join(metadata.table_doc(t, cat_tables[t]) for t in tables)
    user = f"SCHEMA\n{schema}\n\n{EXAMPLES}\n\nQuestion: {question}"
    return [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}]


def extract_sql(text):
    m = re.search(r"```(?:sql)?\s*(.*?)```", text, re.S | re.I)
    sql = (m.group(1) if m else text).strip()
    if not m:
        m2 = re.search(r"\b(with|select)\b.*", text, re.S | re.I)
        sql = m2.group(0).strip() if m2 else sql
    return sql.rstrip(";").strip()


class Text2SQL:
    def __init__(self, trino_conn, llm=None, redactor=None, audit=None, cat_tables=None):
        self.redactor = redactor or Redactor()
        self.llm = llm or OllamaClient(self.redactor)
        self.trino = trino_conn
        self.audit = audit or AuditLog(redactor=self.redactor)
        self.cat_tables = cat_tables or metadata.all_tables()

    def answer(self, question, k=3, repair=True, execute=True, tables=None):
        t0 = time.time()
        clean_q, findings = self.redactor.redact(question)
        out = {"question": clean_q, "pii_redacted_from_question": sorted(set(findings)), "attempts": [], "sql": None,
               "guard": None, "cost": None, "columns": None, "rows": None, "error": None}
        try:
            retrieved = ([{"table": t, "score": None} for t in tables] if tables
                         else discovery.retrieve(self.llm, clean_q, k=k))
            out["retrieved"] = retrieved
            names = [r["table"] for r in retrieved]
            messages = build_messages(clean_q, names, self.cat_tables)
            for attempt in range(2 if repair else 1):
                text, stats = self.llm.chat(messages)
                sql = extract_sql(text)
                rep, cost = sql_guard.check(sql, self.trino)
                out["attempts"].append({"sql": sql, "errors": rep.errors, "llm": stats})
                out["sql"], out["guard"], out["cost"] = sql, {"ok": rep.ok, "errors": rep.errors}, cost
                if rep.ok:
                    break
                messages = messages + [{"role": "assistant", "content": f"```sql\n{sql}\n```"},
                                       {"role": "user", "content": "That query was rejected: " + "; ".join(rep.errors)[:500] +
                                        "\nFix it and reply with the corrected SQL only."}]
            if out["guard"]["ok"] and execute:
                cur = self.trino.cursor()
                cur.execute(out["sql"])
                out["columns"] = [d[0] for d in cur.description]
                out["rows"] = cur.fetchmany(MAX_LIMIT)
        except Exception as e:  # noqa: BLE001 - report, never crash the console
            out["error"] = f"{type(e).__name__}: {str(getattr(e, 'message', e))[:300]}"
        out["elapsed_s"] = round(time.time() - t0, 2)
        self.audit.write(question=out["question"], retrieved=[r["table"] for r in out.get("retrieved", [])], sql=out["sql"],
                         guard=out["guard"], est_scan_rows=(out["cost"] or {}).get("estimated_scan_rows"),
                         rows_returned=None if out["rows"] is None else len(out["rows"]), error=out["error"])
        return out
