"""Incremental, idempotent extract: PostgreSQL -> partitioned Parquet on S3/MinIO.

Watermark logic
  * The extract window is [lo, hi) on updated_at. hi = the Airflow data_interval_end.
    lo = data_interval_start, except for the first interval (initial load) where lo = epoch.
    Ad-hoc backfills can override both through DAG params.
  * Rows whose updated_at falls in the window tell us WHICH dt partitions (created_at date) changed;
    a late status update touches an old partition.
  * Every changed partition is rebuilt in full from the source and overwritten at a deterministic key
    (raw/<table>/dt=YYYY-MM-DD/data.parquet), so re-running never appends duplicates.
  * A per-table high watermark object (_state/<table>/watermark.json) records progress for freshness checks.
"""
import hashlib
import io
import json
from datetime import date, datetime, timedelta

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

from . import catalog
from .config import OLTP_DSN, S3_BUCKET, s3_client
from .logs import get_logger

EPOCH = datetime(1970, 1, 1)


def raw_key(table, dt):
    return f"raw/{table}/dt={dt}/data.parquet"


def changed_partitions(conn, table, lo, hi):
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT created_at::date FROM {table} "
                    "WHERE updated_at >= %s AND updated_at < %s ORDER BY 1", (lo, hi))
        return [r[0].isoformat() for r in cur.fetchall()]


def read_partition(conn, table, dt, cat=None):
    cat = cat or catalog.load()
    cols = catalog.column_names(table, cat)
    pk = cat["source"][table]["pk"]
    d0 = date.fromisoformat(dt)
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(cols)} FROM {table} WHERE created_at >= %s AND created_at < %s ORDER BY {pk}",
                    (d0, d0 + timedelta(days=1)))
        rows = cur.fetchall()
    data = {c: [r[i] for r in rows] for i, c in enumerate(cols)}
    return pa.Table.from_pydict(data, schema=catalog.arrow_schema(table, cat))


def parquet_bytes(tbl):
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="snappy", use_dictionary=True, write_statistics=True,
                   coerce_timestamps="ms")
    return buf.getvalue()


def write_partition(s3, table, dt, tbl):
    """Partition-level overwrite: put the deterministic key, then drop any other stray objects."""
    body = parquet_bytes(tbl)
    key = raw_key(table, dt)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body)
    prefix = f"raw/{table}/dt={dt}/"
    for o in s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix).get("Contents", []):
        if o["Key"] != key:
            s3.delete_object(Bucket=S3_BUCKET, Key=o["Key"])
    manifest = {"table": table, "dt": dt, "rows": tbl.num_rows, "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest()}
    s3.put_object(Bucket=S3_BUCKET, Key=f"_state/{table}/dt={dt}.json",
                  Body=json.dumps(manifest, sort_keys=True).encode())
    return manifest


def get_watermark(s3, table):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=f"_state/{table}/watermark.json")["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None


def _update_watermark(s3, table, hi, run_id):
    cur = get_watermark(s3, table)
    new = max(x for x in [cur and cur["high_watermark"], hi.isoformat()] if x)  # never regress on re-runs
    s3.put_object(Bucket=S3_BUCKET, Key=f"_state/{table}/watermark.json",
                  Body=json.dumps({"high_watermark": new, "last_run_id": run_id}).encode())
    return new


def extract_table(table, lo, hi, ctx=None, only_partitions=None):
    """Extract one table for window [lo, hi). Returns list of partition manifests."""
    log = get_logger(ctx)
    cat = catalog.load()
    s3 = s3_client()
    conn = psycopg2.connect(OLTP_DSN)
    try:
        parts = only_partitions or changed_partitions(conn, table, lo, hi)
        log("extract_start", table=table, window_lo=lo.isoformat(), window_hi=hi.isoformat(),
            partitions_changed=len(parts))
        out = []
        for dt in parts:
            tbl = read_partition(conn, table, dt, cat)
            m = write_partition(s3, table, dt, tbl)
            log("partition_written", table=table, partition=f"dt={dt}", rows=m["rows"], bytes=m["bytes"],
                sha256=m["sha256"][:12])
            out.append(m)
        wm = _update_watermark(s3, table, hi, (ctx or {}).get("run_id"))
        log("extract_done", table=table, partitions=len(out), rows=sum(m["rows"] for m in out), high_watermark=wm)
        return out
    finally:
        conn.close()
