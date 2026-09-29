"""Sarovar Analyst Console API. ALL DATA IS SYNTHETIC; the LLM is a local open-weight model (Ollama).

POST /api/ask                 question -> retrieved tables, SQL, EXPLAIN cost, guarded execution, results
GET  /api/health/pipeline     last DAG run status per DAG (from the Airflow metadata DB) + freshness per table
GET  /api/health/dq           live data-quality checks (latest partition per table, schema drift, freshness)
GET  /api/catalog             catalog search (keyword, optional semantic) with PII tags
"""
import os
import sys
from datetime import datetime

import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (os.path.join(HERE, "..", "..", "ai"), "/app/ai"):
    if os.path.isdir(p) and p not in sys.path:
        sys.path.insert(0, p)
import sarovar_ai  # noqa: E402,F401  (adds the shared sarovar lib + catalog path)

from sarovar import dq, trino_utils  # noqa: E402
from sarovar.config import OLTP_DSN, s3_client  # noqa: E402
from sarovar.extract import get_watermark  # noqa: E402
from sarovar_ai import discovery, metadata  # noqa: E402
from sarovar_ai.config import LLM_MODEL  # noqa: E402
from sarovar_ai.llm import OllamaClient  # noqa: E402
from sarovar_ai.redact import Redactor  # noqa: E402
from sarovar_ai.text2sql import Text2SQL  # noqa: E402

AIRFLOW_DSN = os.environ.get("AIRFLOW_DB_DSN", OLTP_DSN.rsplit("/", 1)[0] + "/airflow")
LABELS = {"data": "synthetic", "model": f"local open-weight model via Ollama: {LLM_MODEL}",
          "note": "All data is synthetic. Not production. The local model makes mistakes; always read the SQL."}

app = FastAPI(title="Sarovar Analyst Console API", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
_state = {}


def _known_names():
    """Optional defence in depth: exact-name redaction. Names are held in memory only, never logged."""
    if os.environ.get("REDACT_KNOWN_NAMES", "1") != "1":
        return []
    try:
        conn = psycopg2.connect(OLTP_DSN, connect_timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT full_name FROM users")
        names = [r[0] for r in cur.fetchall()]
        conn.close()
        return names
    except Exception:  # noqa: BLE001
        return []


def t2s():
    if "t2s" not in _state:
        red = Redactor(known_names=_known_names())
        _state["t2s"] = Text2SQL(trino_utils.connect(user="analyst-console"), llm=OllamaClient(red), redactor=red)
    return _state["t2s"]


class Ask(BaseModel):
    question: str = Field(min_length=3, max_length=500)
    k: int = Field(3, ge=1, le=6)
    repair: bool = True


@app.get("/api/labels")
def labels():
    return LABELS


@app.post("/api/ask")
def ask(body: Ask):
    out = t2s().answer(body.question, k=body.k, repair=body.repair, execute=True)
    out["labels"] = LABELS
    return out


# ------------------------------------------------------------------ pipeline health
def _q(dsn, sql, args=()):
    conn = psycopg2.connect(dsn, connect_timeout=3)
    try:
        cur = conn.cursor()
        cur.execute(sql, args)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


@app.get("/api/health/pipeline")
def pipeline_health():
    dags = []
    try:
        for dag_id in ("sarovar_ingest", "sarovar_transform", "sarovar_freshness"):
            runs = _q(AIRFLOW_DSN, "SELECT run_id, state, logical_date, start_date, end_date FROM dag_run WHERE dag_id=%s "
                                   "ORDER BY start_date DESC NULLS LAST LIMIT 5", (dag_id,))
            counts = _q(AIRFLOW_DSN, "SELECT state, count(*) AS n FROM dag_run WHERE dag_id=%s GROUP BY state", (dag_id,))
            tasks = _q(AIRFLOW_DSN, "SELECT task_id, state FROM task_instance WHERE dag_id=%s AND run_id=%s ORDER BY task_id",
                       (dag_id, runs[0]["run_id"])) if runs else []
            dags.append({"dag_id": dag_id, "last_run": runs[0] if runs else None, "recent_runs": runs,
                         "run_counts": {c["state"]: c["n"] for c in counts}, "last_run_tasks": tasks})
    except Exception as e:  # noqa: BLE001
        raise HTTPException(503, f"Airflow metadata DB unreachable: {e}")
    s3 = s3_client()
    now = datetime.utcnow()
    fresh = []
    conn = psycopg2.connect(OLTP_DSN, connect_timeout=3)
    cur = conn.cursor()
    for t in ("users", "merchants", "transactions", "refunds"):
        wm = get_watermark(s3, t)
        hi = datetime.fromisoformat(wm["high_watermark"]) if wm else None
        cur.execute(f"SELECT max(updated_at) FROM {t}")
        src = cur.fetchone()[0]
        lag_h = round((now - hi).total_seconds() / 3600, 1) if hi else None
        fresh.append({"table": t, "watermark": hi, "source_max_updated_at": src, "lag_hours_vs_now": lag_h,
                      "status": "ok" if hi and lag_h <= 26 else "stale", "limit_hours": 26})
    conn.close()
    return {"dags": dags, "freshness": fresh, "as_of": now, "labels": LABELS,
            "note": "Data is a static synthetic snapshot ending 2026-09-28; freshness turns 'stale' as wall-clock time passes."}


@app.get("/api/health/dq")
def dq_health():
    results = []
    s3 = s3_client()
    conn = psycopg2.connect(OLTP_DSN, connect_timeout=3)
    try:
        for t in ("users", "merchants", "transactions", "refunds"):
            checks = []
            with conn.cursor() as cur:
                cur.execute(f"SELECT max(created_at::date) FROM {t}")
                latest = cur.fetchone()[0].isoformat()
            checks.append(("schema_drift", lambda t=t: dq.run_schema_check(t)))
            checks.append((f"partition dt={latest} (row count, nulls, unique key)", lambda t=t, l=latest: dq.run_partition_checks(t, l, conn=conn, s3=s3)))
            checks.append(("freshness", lambda t=t: dq.run_table_freshness(t, now=datetime.utcnow(), max_lag_hours=26)))
            for name, fn in checks:
                try:
                    fn()
                    results.append({"table": t, "check": name, "status": "pass", "message": "ok"})
                except dq.DataQualityError as e:
                    results.append({"table": t, "check": name, "status": "fail", "message": str(e)})
                except Exception as e:  # noqa: BLE001
                    results.append({"table": t, "check": name, "status": "error", "message": f"{type(e).__name__}: {e}"})
    finally:
        conn.close()
    return {"results": results, "as_of": datetime.utcnow(), "labels": LABELS}


# ------------------------------------------------------------------ catalog
@app.get("/api/catalog")
def catalog_search(q: str = "", semantic: bool = False):
    tables = metadata.all_tables()
    ranked = None
    if semantic and q:
        try:
            ranked = {r["table"]: r["score"] for r in discovery.retrieve(t2s().llm, Redactor().redact(q)[0], k=len(tables))}
        except Exception as e:  # noqa: BLE001
            raise HTTPException(503, f"semantic search unavailable: {e}")
    out = []
    ql = q.lower().strip()
    for key, spec in tables.items():
        hay = " ".join([key, spec["description"]] + [c["name"] + " " + c["description"] for c in spec["columns"]]).lower()
        if ql and not semantic and not all(w in hay for w in ql.split()):
            continue
        out.append({"table": f"lake.{key}", "kind": spec["kind"], "owner": spec["owner"], "description": spec["description"],
                    "partition_column": spec.get("partition_col"), "partition_filter_required": bool(spec.get("partition_filter_required")),
                    "score": ranked.get(key) if ranked else None,
                    "columns": [{"name": c["name"], "type": c["type"], "description": c["description"], "pii": bool(c.get("pii")),
                                 "pii_class": c.get("pii_class")} for c in spec["columns"]]})
    if ranked:
        out.sort(key=lambda t: -(t["score"] or 0))
    return {"tables": out, "labels": LABELS}
