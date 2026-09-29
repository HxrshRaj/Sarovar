"""Evaluation harness (Phase 7). Nothing here is tuned to flatter the result: gold SQL and questions were written
once, by hand, before the first run (see git history of ai/eval/questions.json).

  python ai/eval/run_eval.py gold                      # sanity: every gold query passes the guard and executes
  python ai/eval/run_eval.py discovery                 # recall@k of table retrieval
  python ai/eval/run_eval.py text2sql --k 3 --repair 1 --tag main    # execution accuracy (slow: local CPU model)
  python ai/eval/run_eval.py pii                       # adversarial PII questions

Execution accuracy = generated SQL executes and returns the same result set as the gold SQL (values compared after
rounding numerics to 2 dp; column *names/order* ignored; row order only compared when gold has ORDER BY).
"""
import argparse
import json
import os
import sys
import time
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import sqlglot  # noqa: E402
from sqlglot import exp  # noqa: E402

import sarovar_ai  # noqa: E402,F401  (adds airflow/include to sys.path)
from sarovar import trino_utils  # noqa: E402
from sarovar_ai import discovery, sql_guard  # noqa: E402
from sarovar_ai.llm import OllamaClient  # noqa: E402
from sarovar_ai.redact import Redactor  # noqa: E402
from sarovar_ai.text2sql import Text2SQL  # noqa: E402

QUESTIONS = json.load(open(os.path.join(HERE, "questions.json"), encoding="utf-8"))
RESULTS = os.path.join(HERE, "results")
os.makedirs(RESULTS, exist_ok=True)


def gold_tables(sql):
    return sorted({f"{t.db}.{t.name}".lower() for t in sqlglot.parse_one(sql, read="trino").find_all(exp.Table)})


def norm_val(v):
    if isinstance(v, (int, float, Decimal)) and not isinstance(v, bool):
        return str(round(float(v), 2))
    return str(v)


def norm_rows(rows, ordered):
    r = [tuple(sorted(norm_val(v) for v in row)) for row in rows]
    return r if ordered else sorted(r)


def run(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    return [d[0] for d in cur.description], cur.fetchall()


def classify(q, res, gold_t):
    """Bucket the outcome of one question."""
    if res.get("error"):
        return "pipeline_error"
    used = gold_tables(res["sql"]) if res["sql"] and res["guard"] and res["guard"]["ok"] else []
    if res["guard"] is None or not res["guard"]["ok"]:
        errs = " ".join((res["guard"] or {}).get("errors", []))
        for key, name in (("does not parse", "guard:sql_syntax"), ("LIMIT", "guard:missing_or_bad_limit"),
                          ("partitioned", "guard:missing_partition_filter"), ("PII", "guard:pii_column"),
                          ("SELECT *", "guard:select_star"), ("allow-list", "guard:unknown_table"),
                          ("qualified", "guard:unqualified_table"), ("EXPLAIN failed", "guard:explain_failed_bad_column_or_type"),
                          ("estimated scan", "guard:cost_gate")):
            if key in errs:
                return name
        return "guard:other"
    if res.get("exec_error"):
        return "execution_error"
    if not res["correct"]:
        retrieved = {r["table"] for r in res.get("retrieved", [])}
        if not set(gold_t) <= retrieved:
            return "wrong_result:retrieval_missed_a_gold_table"
        return "wrong_result:wrong_table_choice" if set(used) != set(gold_t) else "wrong_result:same_tables_wrong_logic"
    return "correct"


def cmd_gold(conn):
    bad = 0
    for q in QUESTIONS:
        rep, cost = sql_guard.check(q["gold_sql"], conn)
        try:
            cols, rows = run(conn, q["gold_sql"])
            status = f"rows={len(rows)} est_scan={cost and cost['estimated_scan_rows']}"
        except Exception as e:  # noqa: BLE001
            status = f"EXEC ERROR {e}"
            bad += 1
        if not rep.ok:
            bad += 1
            status += f" GUARD {rep.errors}"
        print(q["id"], status)
    print("gold problems:", bad)
    return bad


def cmd_discovery(llm, ks=(1, 3, 5)):
    rows = []
    for q in QUESTIONS:
        gt = gold_tables(q["gold_sql"])
        got = [r["table"] for r in discovery.retrieve(llm, q["question"], k=max(ks), n_docs=60)]
        rows.append({"id": q["id"], "gold": gt, "retrieved": got})
    out = {"n_questions": len(rows), "embed_model": llm.embed_model}
    for k in ks:
        rec = [len(set(r["gold"]) & set(r["retrieved"][:k])) / len(r["gold"]) for r in rows]
        out[f"recall@{k}"] = round(sum(rec) / len(rec), 3)
        out[f"all_gold_in_top{k}"] = round(sum(set(r["gold"]) <= set(r["retrieved"][:k]) for r in rows) / len(rows), 3)
        out[f"any_gold_in_top{k}"] = round(sum(bool(set(r["gold"]) & set(r["retrieved"][:k])) for r in rows) / len(rows), 3)
    out["per_question"] = rows
    json.dump(out, open(os.path.join(RESULTS, "discovery.json"), "w"), indent=1)
    print({k: v for k, v in out.items() if k != "per_question"})
    misses = [(r["id"], r["gold"], r["retrieved"][:3]) for r in rows if not set(r["gold"]) <= set(r["retrieved"][:3])]
    print("questions where top-3 misses a gold table:", len(misses))
    for m in misses:
        print("  ", m)


def cmd_text2sql(conn, k, repair, tag, limit, oracle=False):
    path = os.path.join(RESULTS, f"text2sql_{tag}.json")
    done = {r["id"]: r for r in json.load(open(path))["per_question"]} if os.path.exists(path) else {}
    red = Redactor()
    llm = OllamaClient(red)
    t2s = Text2SQL(conn, llm=llm, redactor=red)
    for q in QUESTIONS[:limit]:
        if q["id"] in done:
            continue
        gold_cols, gold_rows = run(conn, q["gold_sql"])
        gt = gold_tables(q["gold_sql"])
        ordered = "order by" in q["gold_sql"].lower()
        res = t2s.answer(q["question"], k=k, repair=bool(repair), execute=False, tables=gt if oracle else None)
        res["correct"] = False
        res["exec_error"] = None
        if res["guard"] and res["guard"]["ok"]:
            try:
                cols, rows = run(conn, res["sql"])
                res["correct"] = norm_rows(rows, ordered) == norm_rows(gold_rows, ordered)
                res["n_rows"], res["gold_rows"] = len(rows), len(gold_rows)
            except Exception as e:  # noqa: BLE001
                res["exec_error"] = str(getattr(e, "message", e))[:200]
        res["gold_sql"], res["gold_tables"], res["id"], res["difficulty"] = q["gold_sql"], gt, q["id"], q["difficulty"]
        res["question"] = q["question"]
        res["outcome"] = classify(q, res, gt)
        res.pop("rows", None)
        done[q["id"]] = res
        print(q["id"], q["difficulty"], res["outcome"], f"{res['elapsed_s']}s", "attempts=", len(res["attempts"]), flush=True)
        summarize(done, k, repair, tag, path)
    return done


def summarize(done, k, repair, tag, path):
    items = [done[i] for i in sorted(done)]
    n = len(items)
    correct = sum(r["outcome"] == "correct" for r in items)
    first_try = sum(1 for r in items if r["attempts"] and not r["attempts"][0]["errors"])
    by_diff = {}
    for r in items:
        d = by_diff.setdefault(r["difficulty"], [0, 0])
        d[1] += 1
        d[0] += r["outcome"] == "correct"
    outcomes = {}
    for r in items:
        outcomes[r["outcome"]] = outcomes.get(r["outcome"], 0) + 1
    summary = {"tag": tag, "model": OllamaClient().model, "k_tables": k, "repair_attempts": repair, "n": n,
               "execution_accuracy": round(correct / n, 3), "correct": correct,
               "passed_guard_first_attempt": first_try,
               "by_difficulty": {d: f"{c}/{t}" for d, (c, t) in by_diff.items()}, "outcomes": dict(sorted(outcomes.items(), key=lambda x: -x[1])),
               "mean_seconds_per_question": round(sum(r["elapsed_s"] for r in items) / n, 1)}
    json.dump({"summary": summary, "per_question": items}, open(path, "w"), indent=1, default=str)
    return summary


def cmd_pii(conn, file="pii_adversarial.json", out_name="pii_adversarial.json"):
    red = Redactor()
    llm = OllamaClient(red, record=True)
    t2s = Text2SQL(conn, llm=llm, redactor=red)
    rows = []
    for q in json.load(open(os.path.join(HERE, file))):
        r = t2s.answer(q["question"], k=3, repair=False, execute=False)
        blocked = not (r["guard"] and r["guard"]["ok"])
        rows.append({"id": q["id"], "question_as_seen_by_model": r["question"], "redacted": r["pii_redacted_from_question"],
                     "sql": r["sql"], "blocked": blocked, "guard_errors": (r["guard"] or {}).get("errors"), "error": r["error"]})
        print(q["id"], "BLOCKED" if blocked else "!! NOT BLOCKED", r["sql"])
    leaked = [t for t in llm.sent if red.redact(t)[1]]
    out = {"n": len(rows), "blocked": sum(r["blocked"] for r in rows), "prompts_sent": len(llm.sent),
           "prompts_containing_pii_like_text": len(leaked), "per_question": rows}
    json.dump(out, open(os.path.join(RESULTS, out_name), "w"), indent=1)
    print({k: v for k, v in out.items() if k != "per_question"})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["gold", "discovery", "text2sql", "pii", "pii_heldout"])
    ap.add_argument("--k", type=int, default=3)
    ap.add_argument("--repair", type=int, default=1)
    ap.add_argument("--tag", default="main")
    ap.add_argument("--limit", type=int, default=999)
    ap.add_argument("--oracle", type=int, default=0, help="1 = give the model the gold tables (isolates model errors from retrieval errors)")
    a = ap.parse_args()
    conn = trino_utils.connect()
    if a.cmd == "gold":
        sys.exit(1 if cmd_gold(conn) else 0)
    elif a.cmd == "discovery":
        cmd_discovery(OllamaClient())
    elif a.cmd == "text2sql":
        cmd_text2sql(conn, a.k, a.repair, a.tag, a.limit, bool(a.oracle))
    elif a.cmd == "pii_heldout":
        cmd_pii(conn, "pii_heldout.json", "pii_heldout.json")
    else:
        cmd_pii(conn)
