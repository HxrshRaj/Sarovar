"""Data-quality checks. Each check raises DataQualityError with a message that says
WHAT failed, WHERE (table / partition) and the numbers, so an on-call engineer can act on it.

Pure checks operate on pyarrow tables / plain values so tests can inject bad data.
"""
import io
from datetime import date, datetime, timedelta

import psycopg2
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import catalog
from .config import OLTP_DSN, S3_BUCKET, s3_client
from .extract import get_watermark, raw_key
from .logs import get_logger


class DataQualityError(Exception):
    def __init__(self, check, table, partition, detail):
        self.check, self.table, self.partition, self.detail = check, table, partition, detail
        super().__init__(f"DQ FAIL [{check}] {table} {partition or ''}: {detail}".replace("  ", " "))


# ---------------------------------------------------------------- pure checks
def check_row_count(table, dt, source_rows, target_rows):
    if source_rows != target_rows:
        raise DataQualityError("row_count", table, f"dt={dt}",
                               f"source has {source_rows} rows but lake partition has {target_rows} "
                               f"(diff {target_rows - source_rows:+d}); re-run extract for this partition")


def check_not_null(tbl, table, dt, columns):
    for c in columns:
        n = tbl[c].null_count
        if n:
            raise DataQualityError("not_null", table, f"dt={dt}", f"column '{c}' has {n} NULL value(s) of {tbl.num_rows} rows")


def check_unique(tbl, table, dt, key):
    dups = tbl.num_rows - len(pc.unique(tbl[key]))
    if dups:
        raise DataQualityError("unique_key", table, f"dt={dt}", f"key '{key}' has {dups} duplicate value(s) among {tbl.num_rows} rows")


def check_partition_exists(exists, table, dt):
    if not exists:
        raise DataQualityError("partition_exists", table, f"dt={dt}", "expected partition is missing from the lake (no data.parquet)")


def check_freshness(table, watermark_hi, now, max_lag_hours, source_max_updated=None):
    """Fails if the pipeline watermark is older than max_lag, or source holds changes far ahead of it."""
    if watermark_hi is None:
        raise DataQualityError("freshness", table, None, "no watermark recorded: the table has never been loaded")
    lag = now - watermark_hi
    if lag > timedelta(hours=max_lag_hours):
        raise DataQualityError("freshness", table, None,
                               f"watermark {watermark_hi:%Y-%m-%d %H:%M} is {lag.total_seconds()/3600:.1f}h behind "
                               f"{now:%Y-%m-%d %H:%M}, limit {max_lag_hours}h")
    if source_max_updated is not None and source_max_updated - watermark_hi > timedelta(hours=max_lag_hours):
        raise DataQualityError("freshness", table, None,
                               f"source has changes up to {source_max_updated:%Y-%m-%d %H:%M}, "
                               f"{(source_max_updated - watermark_hi).total_seconds()/3600:.1f}h past the loaded watermark")


def check_schema(table, source_columns, catalog_columns):
    """source_columns / catalog_columns: {name: type-family}. Detects drift in the source schema."""
    added = sorted(set(source_columns) - set(catalog_columns))
    dropped = sorted(set(catalog_columns) - set(source_columns))
    changed = sorted(c for c in set(source_columns) & set(catalog_columns) if source_columns[c] != catalog_columns[c])
    if added or dropped or changed:
        raise DataQualityError("schema_drift", table, None,
                               f"source schema differs from catalog: added={added} dropped={dropped} type_changed={changed}")


# ---------------------------------------------------------------- IO-backed checks
_PG_FAMILY = {"bigint": "int", "integer": "int", "text": "str", "character varying": "str",
              "timestamp without time zone": "ts", "double precision": "float", "numeric": "float"}
_CAT_FAMILY = {"bigint": "int", "integer": "int", "varchar": "str", "timestamp(3)": "ts", "double": "float"}


def source_schema(conn, table):
    with conn.cursor() as cur:
        cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name=%s", (table,))
        return {n: _PG_FAMILY.get(t, t) for n, t in cur.fetchall()}


def read_lake_partition(s3, table, dt):
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=raw_key(table, dt))["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None
    return pq.read_table(io.BytesIO(body))


def run_partition_checks(table, dt, ctx=None, conn=None, s3=None):
    """Row count vs source, key nulls, key uniqueness for one partition. Raises on first failure."""
    log = get_logger(ctx)
    cat = catalog.load()
    s3 = s3 or s3_client()
    own = conn is None
    conn = conn or psycopg2.connect(OLTP_DSN)
    try:
        tbl = read_lake_partition(s3, table, dt)
        check_partition_exists(tbl is not None, table, dt)
        d0 = date.fromisoformat(dt)
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {table} WHERE created_at >= %s AND created_at < %s", (d0, d0 + timedelta(days=1)))
            src = cur.fetchone()[0]
        spec = cat["source"][table]
        key_cols = [spec["pk"], "created_at", "updated_at"] + [c["name"] for c in spec["columns"]
                                                                if c["name"].endswith("_id") and c["name"] != spec["pk"]]
        for name, fn in (("row_count", lambda: check_row_count(table, dt, src, tbl.num_rows)),
                         ("not_null", lambda: check_not_null(tbl, table, dt, key_cols)),
                         ("unique_key", lambda: check_unique(tbl, table, dt, spec["pk"]))):
            try:
                fn()
            except DataQualityError as e:
                log("dq_check_failed", level="ERROR", table=table, partition=f"dt={dt}", check=name,
                    source_rows=src, target_rows=tbl.num_rows, message=str(e))
                raise
        log("dq_partition_ok", table=table, partition=f"dt={dt}", source_rows=src, target_rows=tbl.num_rows,
            checks=["row_count", "not_null", "unique_key"])
        return {"table": table, "dt": dt, "rows": tbl.num_rows}
    finally:
        if own:
            conn.close()


def run_table_freshness(table, now, max_lag_hours=26, ctx=None, compare_source=True):
    log = get_logger(ctx)
    s3 = s3_client()
    wm = get_watermark(s3, table)
    hi = datetime.fromisoformat(wm["high_watermark"]) if wm else None
    conn = psycopg2.connect(OLTP_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT max(updated_at) FROM {table}")
            src_max = cur.fetchone()[0]
    finally:
        conn.close()
    try:
        check_freshness(table, hi, now, max_lag_hours, src_max if compare_source else None)
    except DataQualityError as e:
        log("dq_check_failed", level="ERROR", table=table, check="freshness", message=str(e))
        raise
    log("dq_freshness_ok", table=table, watermark=hi.isoformat(), source_max_updated=src_max.isoformat(),
        now=now.isoformat(), max_lag_hours=max_lag_hours)


def run_schema_check(table, ctx=None):
    log = get_logger(ctx)
    cat = catalog.load()
    conn = psycopg2.connect(OLTP_DSN)
    try:
        src = source_schema(conn, table)
    finally:
        conn.close()
    exp = {c["name"]: _CAT_FAMILY[c["type"]] for c in cat["source"][table]["columns"]}
    try:
        check_schema(table, src, exp)
    except DataQualityError as e:
        log("dq_check_failed", level="ERROR", table=table, check="schema_drift", message=str(e))
        raise
    log("dq_schema_ok", table=table, columns=len(exp))


def reconcile_counts(table, ctx=None, conn=None, s3=None):
    """Cheap full-history reconciliation: source row count per created_at date vs the row count recorded in the lake
    manifest (_state/<table>/dt=*.json), for EVERY partition. Catches rows the incremental window never saw."""
    import json
    log = get_logger(ctx)
    s3 = s3 or s3_client()
    own = conn is None
    conn = conn or psycopg2.connect(OLTP_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT created_at::date, count(*) FROM {table} GROUP BY 1")
            src = {d.isoformat(): n for d, n in cur.fetchall()}
    finally:
        if own:
            conn.close()
    lake = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix=f"_state/{table}/dt="):
        for o in page.get("Contents", []):
            dt = o["Key"].split("dt=")[1].removesuffix(".json")
            lake[dt] = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=o["Key"])["Body"].read())["rows"]
    bad = [f"dt={d}: source {src.get(d, 0)} vs lake {lake.get(d, 'MISSING')}" for d in sorted(set(src) | set(lake))
           if src.get(d, 0) != lake.get(d)]
    if bad:
        log("dq_check_failed", level="ERROR", table=table, check="reconcile", partitions_differing=len(bad), message="; ".join(bad[:5]))
        raise DataQualityError("reconcile", table, None,
                               f"{len(bad)} of {len(set(src) | set(lake))} partitions differ from source: " + "; ".join(bad[:5]) +
                               " - backfill those dates (runbook section 3B)")
    log("reconcile_counts_ok", table=table, partitions=len(src), rows=sum(src.values()))
