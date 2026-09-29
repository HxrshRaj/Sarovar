"""Same analytical aggregation on the OLTP PostgreSQL source vs Trino over Parquet, plus an interference test:
point-lookup latency on PostgreSQL alone vs while analytic queries hammer PostgreSQL vs while they hit Trino.
Output: docs/measurements/oltp_vs_olap.json (all numbers in docs/oltp-vs-olap.md come from here)."""
import json
import os
import random
import statistics
import sys
import threading
import time

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "include"))
from sarovar import trino_utils  # noqa: E402
from sarovar.config import OLTP_DSN  # noqa: E402

OUT = os.path.join(os.path.dirname(__file__), "..", "docs", "measurements", "oltp_vs_olap.json")
REPS = 7
PG_SQL = """SELECT m.category, t.created_at::date AS d, count(*) AS n, sum(t.amount_paise)/100.0 AS gmv
FROM transactions t JOIN merchants m ON m.merchant_id = t.merchant_id
WHERE t.status = 'SUCCESS' GROUP BY 1, 2"""
TR_SQL = PG_SQL.replace("transactions t", "lake.raw.transactions t").replace("JOIN merchants m", "JOIN lake.raw.merchants m") \
    .replace("t.created_at::date", "CAST(t.created_at AS date)")


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def pg_analytic(conn):
    cur = conn.cursor()
    t0 = time.perf_counter()
    cur.execute(PG_SQL)
    n = len(cur.fetchall())
    return (time.perf_counter() - t0) * 1000, n


def tr_analytic():
    cur = trino_utils.connect().cursor()
    t0 = time.perf_counter()
    cur.execute(TR_SQL)
    n = len(cur.fetchall())
    return (time.perf_counter() - t0) * 1000, n


def lookups(n=400):
    conn = psycopg2.connect(OLTP_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    ids = [random.randint(1, 300000) for _ in range(n)]
    lat = []
    for i in ids:
        t0 = time.perf_counter()
        cur.execute("SELECT * FROM transactions WHERE txn_id = %s", (i,))
        cur.fetchall()
        lat.append((time.perf_counter() - t0) * 1000)
    conn.close()
    return {"p50_ms": round(pct(lat, 50), 2), "p95_ms": round(pct(lat, 95), 2), "p99_ms": round(pct(lat, 99), 2), "n": n}


def with_load(kind, workers=4):
    stop = threading.Event()
    count = [0]

    def loop():
        conn = psycopg2.connect(OLTP_DSN) if kind == "pg" else None
        while not stop.is_set():
            pg_analytic(conn) if kind == "pg" else tr_analytic()
            count[0] += 1
        if conn:
            conn.close()
    ths = [threading.Thread(target=loop) for _ in range(workers)]
    [t.start() for t in ths]
    time.sleep(1.5)
    res = lookups()
    stop.set()
    [t.join() for t in ths]
    res["analytic_queries_completed_during_test"] = count[0]
    return res


def main():
    random.seed(1)
    res = {"rows": {}, "hardware": "Windows 11 laptop, 12 logical CPUs, Docker Desktop limited to 8 GB RAM; PG container 768 MB, Trino container 2.5 GB"}
    conn = psycopg2.connect(OLTP_DSN)
    cur = conn.cursor()
    for t in ("transactions", "merchants"):
        cur.execute(f"SELECT count(*), pg_total_relation_size('{t}') FROM {t}")
        n, sz = cur.fetchone()
        res["rows"][t] = {"rows": n, "postgres_total_relation_bytes": sz}
    # PG plan-level numbers
    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " + PG_SQL)
    plan = "\n".join(r[0] for r in cur.fetchall())
    res["postgres_explain_analyze"] = plan
    pg_t = [pg_analytic(conn)[0] for _ in range(REPS)]
    tr_t = [tr_analytic()[0] for _ in range(REPS)]
    rows_pg = pg_analytic(conn)[1]
    cur2 = trino_utils.connect().cursor()
    cur2.execute(TR_SQL)
    rows_tr = len(cur2.fetchall())
    import requests
    q = requests.get(f"http://localhost:8080/v1/query/{cur2.query_id}", headers={"X-Trino-User": "sarovar"}).json()["queryStats"]
    res["aggregation"] = {"result_rows_pg": rows_pg, "result_rows_trino": rows_tr,
                          "postgres_ms": {"median": round(statistics.median(pg_t), 1), "min": round(min(pg_t), 1), "max": round(max(pg_t), 1)},
                          "trino_ms": {"median": round(statistics.median(tr_t), 1), "min": round(min(tr_t), 1), "max": round(max(tr_t), 1)},
                          "trino_physical_input": q["physicalInputDataSize"], "trino_input_rows": q["physicalInputPositions"]}
    print(json.dumps(res["aggregation"], indent=1))
    res["point_lookup_latency"] = {"idle_postgres": lookups(),
                                   "while_4_analytic_workers_on_postgres": with_load("pg"),
                                   "while_4_analytic_workers_on_trino": with_load("trino")}
    print(json.dumps(res["point_lookup_latency"], indent=1))
    json.dump(res, open(OUT, "w"), indent=2)


if __name__ == "__main__":
    main()
