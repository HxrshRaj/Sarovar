"""Helpers for the simulated on-call drills. snapshot() records ETag/size/sha256 of raw partitions so a drill can prove
whether anything changed."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "include"))
from sarovar.config import S3_BUCKET, s3_client  # noqa: E402


def snapshot(table="transactions", dts=None):
    s3 = s3_client()
    out = {}
    for p in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix=f"raw/{table}/"):
        for o in p.get("Contents", []):
            dt = o["Key"].split("dt=")[1].split("/")[0]
            if dts and dt not in dts:
                continue
            m = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=f"_state/{table}/dt={dt}.json")["Body"].read())
            out[o["Key"]] = {"etag": o["ETag"], "size": o["Size"], "rows": m["rows"], "sha256": m["sha256"][:16]}
    return out


if __name__ == "__main__":
    table = sys.argv[1]
    dts = sys.argv[2].split(",") if len(sys.argv) > 2 else None
    print(json.dumps(snapshot(table, dts), indent=1, sort_keys=True))
