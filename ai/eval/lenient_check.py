"""Post-hoc audit of the strict metric: how many 'wrong_result' answers are equal as multisets (only row ORDER differs,
typically ties under ORDER BY)? Reported next to - never instead of - the strict number."""
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import sarovar_ai  # noqa: F401
from sarovar import trino_utils
from run_eval import norm_rows, run

conn = trino_utils.connect()
tag = sys.argv[1] if len(sys.argv) > 1 else "main"
items = json.load(open(os.path.join(HERE, "results", f"text2sql_{tag}.json")))["per_question"]
order_only = []
for r in items:
    if r["outcome"].startswith("wrong_result") and r["sql"]:
        _, g = run(conn, r["gold_sql"]); _, a = run(conn, r["sql"])
        if norm_rows(g, False) == norm_rows(a, False):
            order_only.append(r["id"])
print(tag, "wrong answers that match as multisets (order-only differences):", order_only)
