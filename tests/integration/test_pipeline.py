"""Integration tests against the running core stack (make up && make seed && DAG catchup done)."""
import hashlib
from datetime import datetime

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sarovar import catalog, dq, extract, validate
from sarovar.config import OLTP_DSN, S3_BUCKET, s3_client
from sarovar.dq import DataQualityError

pytestmark = pytest.mark.integration
DT = "2026-08-20"


def objects(s3, table, dt):
    prefix = f"raw/{table}/dt={dt}/"
    return {o["Key"]: (o["ETag"], o["Size"]) for o in s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix).get("Contents", [])}


def test_extract_is_idempotent_same_date_twice():
    s3 = s3_client()
    lo, hi = datetime(2026, 8, 20), datetime(2026, 8, 21)
    m1 = extract.extract_table("transactions", lo, hi, {"run_id": "test-a"}, only_partitions=[DT])
    o1 = objects(s3, "transactions", DT)
    body1 = s3.get_object(Bucket=S3_BUCKET, Key=extract.raw_key("transactions", DT))["Body"].read()
    m2 = extract.extract_table("transactions", lo, hi, {"run_id": "test-b"}, only_partitions=[DT])
    o2 = objects(s3, "transactions", DT)
    body2 = s3.get_object(Bucket=S3_BUCKET, Key=extract.raw_key("transactions", DT))["Body"].read()
    assert m1[0]["rows"] == m2[0]["rows"] > 0
    assert m1[0]["sha256"] == m2[0]["sha256"]
    assert hashlib.sha256(body1).hexdigest() == hashlib.sha256(body2).hexdigest()
    assert o1 == o2 and len(o2) == 1  # exactly one file, identical ETag and size


def test_full_window_rerun_changes_nothing():
    """Re-running a whole window (all changed partitions) yields identical partition checksums."""
    s3 = s3_client()
    lo, hi = datetime(2026, 8, 25), datetime(2026, 8, 26)
    a = {m["dt"]: m["sha256"] for m in extract.extract_table("refunds", lo, hi, {"run_id": "t1"})}
    b = {m["dt"]: m["sha256"] for m in extract.extract_table("refunds", lo, hi, {"run_id": "t2"})}
    assert a == b and a


def test_partition_row_count_matches_source():
    s3 = s3_client()
    lo, hi = datetime(2026, 8, 20), datetime(2026, 8, 21)
    extract.extract_table("transactions", lo, hi, {}, only_partitions=[DT])
    assert dq.run_partition_checks("transactions", DT)["rows"] > 0


def test_injected_bad_data_is_caught_then_repaired():
    s3 = s3_client()
    key = extract.raw_key("transactions", DT)
    conn = psycopg2.connect(OLTP_DSN)
    good = extract.read_partition(conn, "transactions", DT)
    try:
        # 1. duplicates (row count goes up too, so row_count fires first - assert either data check)
        dup = pa.concat_tables([good, good.slice(0, 3)])
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=extract.parquet_bytes(dup))
        with pytest.raises(DataQualityError) as e:
            dq.run_partition_checks("transactions", DT)
        assert e.value.check == "row_count" and "+3" in str(e.value)
        # 2. same row count, but a null key and a duplicate key
        ids = good["txn_id"].to_pylist()
        ids[0] = None
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=extract.parquet_bytes(good.set_column(0, "txn_id", pa.array(ids, pa.int64()))))
        with pytest.raises(DataQualityError) as e:
            dq.run_partition_checks("transactions", DT)
        assert e.value.check == "not_null"
        ids = good["txn_id"].to_pylist()
        ids[1] = ids[0]
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=extract.parquet_bytes(good.set_column(0, "txn_id", pa.array(ids, pa.int64()))))
        with pytest.raises(DataQualityError) as e:
            dq.run_partition_checks("transactions", DT)
        assert e.value.check == "unique_key"
        # 3. missing partition
        s3.delete_object(Bucket=S3_BUCKET, Key=key)
        with pytest.raises(DataQualityError) as e:
            dq.run_partition_checks("transactions", DT)
        assert e.value.check == "partition_exists"
    finally:
        extract.write_partition(s3, "transactions", DT, good)  # repair (idempotent overwrite)
        conn.close()
    dq.run_partition_checks("transactions", DT)


def test_catalog_matches_live_schema():
    conn = psycopg2.connect(OLTP_DSN)
    try:
        for t in catalog.source_tables():
            live = dq.source_schema(conn, t)
            exp = {c["name"]: dq._CAT_FAMILY[c["type"]] for c in catalog.load()["source"][t]["columns"]}
            assert live == exp, t
    finally:
        conn.close()


def test_curated_outputs_match_oltp_source():
    res = validate.validate_all()
    assert res["daily_merchant_metrics"] > 0 and res["user_cohorts"] > 0
