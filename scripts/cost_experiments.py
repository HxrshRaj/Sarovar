"""Phase 5 measurements. Every number in docs/cost-aware-querying.md and docs/oltp-vs-olap.md comes from this
script's output (docs/measurements/*.json). Run: make cost-experiments

Trino stats come from the query's own REST stats (/v1/query/<id>): physicalInputDataSize, splits, wall/cpu time.
Each query is run WARM_UP + N times; we report the median wall time and the (deterministic) bytes/splits of the last run.
"""
import io
import json
import os
import statistics
import sys
import threading
import time

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "include"))
from sarovar import trino_utils  # noqa: E402
from sarovar.config import OLTP_DSN, S3_BUCKET, TRINO_HOST, TRINO_PORT, s3_client  # noqa: E402

OUT = os.path.join(os.path.dirname(__file__), "..", "docs", "measurements")
os.makedirs(OUT, exist_ok=True)
REPS = 5


def trino_run(sql, fetch=True):
    conn = trino_utils.connect(schema="raw")
    cur = conn.cursor()
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall() if fetch else []
    wall = (time.perf_counter() - t0) * 1000
    qid = cur.query_id
    q = requests.get(f"http://{TRINO_HOST}:{TRINO_PORT}/v1/query/{qid}", headers={"X-Trino-User": "sarovar"}).json()
    s = q["queryStats"]

    def size(x):  # "12.3MB" -> bytes
        u = {"B": 1, "kB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3}
        for k in ("GB", "MB", "kB", "B"):
            if x.endswith(k):
                return float(x[: -len(k)]) * u[k]
        return float(x)
    return {"wall_ms": wall, "rows_returned": len(rows), "physical_input_bytes": size(s["physicalInputDataSize"]),
            "physical_input_rows": s["physicalInputPositions"], "splits": s["totalDrivers"],
            "query_id": qid}


def bench(label, sql, fetch=True):
    trino_run(sql, fetch)  # warm-up (metadata + file listing caches)
    runs = [trino_run(sql, fetch) for _ in range(REPS)]
    last = dict(runs[-1])
    last["wall_ms_median"] = round(statistics.median(r["wall_ms"] for r in runs), 1)
    last["wall_ms_min"] = round(min(r["wall_ms"] for r in runs), 1)
    last.pop("wall_ms")
    last.update(label=label, sql=" ".join(sql.split()))
    print(f"{label:55s} bytes={last['physical_input_bytes']:>12,.0f} rows_in={last['physical_input_rows']:>9,} "
          f"drivers={last['splits']:>5} wall_med={last['wall_ms_median']:>8.1f}ms")
    return last


def small_file_setup():
    """Same 7 days of transactions as (a) 100 small files per partition, (b) one compacted file per partition."""
    s3 = s3_client()
    conn = trino_utils.connect()
    days = [f"2026-09-{d:02d}" for d in range(1, 8)]
    for variant, nfiles in (("transactions_small", 100), ("transactions_compacted", 1)):
        for dt in days:
            for o in s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"exp/{variant}/dt={dt}/").get("Contents", []):
                s3.delete_object(Bucket=S3_BUCKET, Key=o["Key"])
            tbl = pq.read_table(io.BytesIO(s3.get_object(Bucket=S3_BUCKET, Key=f"raw/transactions/dt={dt}/data.parquet")["Body"].read()))
            n = tbl.num_rows
            step = -(-n // nfiles)
            for i in range(nfiles):
                part = tbl.slice(i * step, step)
                if part.num_rows == 0:
                    continue
                buf = io.BytesIO()
                pq.write_table(part, buf, compression="snappy", coerce_timestamps="ms")
                s3.put_object(Bucket=S3_BUCKET, Key=f"exp/{variant}/dt={dt}/part-{i:04d}.parquet", Body=buf.getvalue())
        trino_utils.run("CREATE SCHEMA IF NOT EXISTS lake.exp", conn)
        ddl = trino_utils.table_ddl("raw", "transactions").replace("lake.raw.transactions", f"lake.exp.{variant}") \
            .replace("s3://lake/raw/transactions/", f"s3://{S3_BUCKET}/exp/{variant}/")
        trino_utils.run(ddl, conn)
        trino_utils.run(f"CALL lake.system.sync_partition_metadata('exp', '{variant}', 'FULL')", conn)
    return days


def storage_growth():
    s3 = s3_client()
    rows = []
    for p in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix="raw/transactions/"):
        for o in p.get("Contents", []):
            dt = o["Key"].split("dt=")[1].split("/")[0]
            m = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=f"_state/transactions/dt={dt}.json")["Body"].read())
            rows.append({"dt": dt, "rows": m["rows"], "bytes": o["Size"]})
    rows.sort(key=lambda r: r["dt"])
    cum = 0
    for r in rows:
        cum += r["bytes"]
        r["cumulative_bytes"] = cum
        r["bytes_per_row"] = round(r["bytes"] / r["rows"], 2)
    return rows


def main():
    res = {"trino_experiments": [], "notes": {"reps": REPS, "cluster": "single Trino node (coordinator+worker), Docker, 2.5 GB limit"}}
    E = res["trino_experiments"]
    # E1: full scan vs partition filter
    E.append(bench("E1a full scan: no dt filter", "SELECT count(*), sum(amount_paise) FROM lake.raw.transactions"))
    E.append(bench("E1b partition filter: dt = one day", "SELECT count(*), sum(amount_paise) FROM lake.raw.transactions WHERE dt = '2026-09-15'"))
    E.append(bench("E1c partition filter: 7 days", "SELECT count(*), sum(amount_paise) FROM lake.raw.transactions WHERE dt BETWEEN '2026-09-09' AND '2026-09-15'"))
    E.append(bench("E1d NON-partition filter (created_at) - still full scan",
                   "SELECT count(*), sum(amount_paise) FROM lake.raw.transactions WHERE created_at >= TIMESTAMP '2026-09-15 00:00:00' AND created_at < TIMESTAMP '2026-09-16 00:00:00'"))
    # E2: SELECT * vs few columns (same 7 days, rows fetched)
    E.append(bench("E2a SELECT * (7 days)", "SELECT * FROM lake.raw.transactions WHERE dt BETWEEN '2026-09-09' AND '2026-09-15'"))
    E.append(bench("E2b SELECT 2 columns (7 days)", "SELECT txn_id, amount_paise FROM lake.raw.transactions WHERE dt BETWEEN '2026-09-09' AND '2026-09-15'"))
    # E3: small vs compacted files
    small_file_setup()
    q = "SELECT status, count(*), sum(amount_paise) FROM lake.exp.{t} WHERE dt BETWEEN '2026-09-01' AND '2026-09-07' GROUP BY status"
    E.append(bench("E3a many small files (100/partition, 700 files)", q.format(t="transactions_small")))
    E.append(bench("E3b compacted (1 file/partition, 7 files)", q.format(t="transactions_compacted")))
    res["storage_growth_transactions"] = storage_growth()
    json.dump(res, open(os.path.join(OUT, "trino_experiments.json"), "w"), indent=2)

    # EXPLAIN / EXPLAIN ANALYZE excerpts for the doc
    ex = {}
    for name, sql in (("full_scan", "SELECT count(*), sum(amount_paise) FROM lake.raw.transactions"),
                      ("partition_pruned", "SELECT count(*), sum(amount_paise) FROM lake.raw.transactions WHERE dt = '2026-09-15'"),
                      ("non_partition_filter", "SELECT count(*) FROM lake.raw.transactions WHERE created_at >= TIMESTAMP '2026-09-15 00:00:00' AND created_at < TIMESTAMP '2026-09-16 00:00:00'")):
        ex[name] = {"explain": trino_utils.run("EXPLAIN " + sql)[0][0], "explain_analyze": trino_utils.run("EXPLAIN ANALYZE " + sql)[0][0]}
    json.dump(ex, open(os.path.join(OUT, "explain_plans.json"), "w"), indent=2)
    print("wrote", OUT)


if __name__ == "__main__":
    main()
