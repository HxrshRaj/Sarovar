"""Independent validation of curated tables against the OLTP source.

Expected values are computed by plain SQL in PostgreSQL (no Spark, no Trino involved); actual values are read
straight from the curated Parquet files on S3 with pyarrow. Any difference raises DataQualityError.
"""
import pandas as pd
import psycopg2
import pyarrow.dataset as ds
import pyarrow.fs as pafs

from .config import OLTP_DSN, S3_BUCKET, S3_ENDPOINT, S3_KEY, S3_SECRET
from .dq import DataQualityError

MERCHANT_SQL = """
SELECT t.created_at::date::text AS dt, t.merchant_id,
       count(*) AS txn_count,
       count(*) FILTER (WHERE t.status='SUCCESS') AS success_count,
       count(*) FILTER (WHERE t.status='FAILED')  AS failed_count,
       count(*) FILTER (WHERE t.status='PENDING') AS pending_count,
       COALESCE(sum(t.amount_paise) FILTER (WHERE t.status='SUCCESS'),0) AS success_amount_paise,
       COALESCE(sum(r.c),0) AS refund_count,
       COALESCE(sum(r.a),0) AS refund_amount_paise
FROM transactions t
LEFT JOIN (SELECT txn_id, count(*) c, sum(amount_paise) a FROM refunds WHERE status <> 'REJECTED' GROUP BY txn_id) r
       ON r.txn_id = t.txn_id
WHERE t.created_at::date::text BETWEEN %(start)s AND %(end)s
GROUP BY 1, 2
"""

COHORT_SQL = """
WITH u AS (SELECT user_id, date_trunc('week', created_at)::date AS cw FROM users),
sizes AS (SELECT cw, count(*) AS cohort_size FROM u GROUP BY cw),
act AS (
  SELECT u.cw, ((date_trunc('week', t.created_at)::date - u.cw) / 7) AS weeks_since_signup,
         count(DISTINCT t.payer_user_id) AS active_users
  FROM transactions t JOIN u ON u.user_id = t.payer_user_id
  WHERE t.status = 'SUCCESS' AND date_trunc('week', t.created_at)::date >= u.cw
  GROUP BY 1, 2)
SELECT to_char(act.cw, 'YYYY-MM-DD') AS cohort_week, act.weeks_since_signup::int AS weeks_since_signup,
       sizes.cohort_size, act.active_users
FROM act JOIN sizes ON sizes.cw = act.cw
"""


def _s3fs():
    return pafs.S3FileSystem(access_key=S3_KEY, secret_key=S3_SECRET,
                             endpoint_override=S3_ENDPOINT.replace("http://", ""), scheme="http")


def _read_curated(table, partition_col):
    d = ds.dataset(f"{S3_BUCKET}/curated/{table}", filesystem=_s3fs(), format="parquet", partitioning="hive")
    df = d.to_table().to_pandas()
    df[partition_col] = df[partition_col].astype(str)
    return df


def _compare(name, exp, act, keys):
    exp = exp.astype({c: "int64" for c in exp.columns if c not in keys and c != "merchant_name"})
    m = exp.merge(act, on=keys, how="outer", suffixes=("_src", "_lake"), indicator=True)
    problems = []
    only_src = m[m["_merge"] == "left_only"]
    only_lake = m[m["_merge"] == "right_only"]
    if len(only_src):
        problems.append(f"{len(only_src)} key(s) only in source SQL, e.g. {only_src[keys].head(3).to_dict('records')}")
    if len(only_lake):
        problems.append(f"{len(only_lake)} key(s) only in curated table, e.g. {only_lake[keys].head(3).to_dict('records')}")
    both = m[m["_merge"] == "both"]
    for c in [c for c in exp.columns if c not in keys and c != "merchant_name"]:
        bad = both[both[f"{c}_src"] != both[f"{c}_lake"]]
        if len(bad):
            problems.append(f"column '{c}' differs on {len(bad)} row(s), e.g. {bad[keys + [c + '_src', c + '_lake']].head(2).to_dict('records')}")
    if problems:
        raise DataQualityError("curated_vs_source", name, None, "; ".join(problems))
    return len(both)


def validate_daily_merchant_metrics(start="0000-01-01", end="9999-12-31"):
    conn = psycopg2.connect(OLTP_DSN)
    try:
        exp = pd.read_sql(MERCHANT_SQL, conn, params={"start": start, "end": end})
    finally:
        conn.close()
    act = _read_curated("daily_merchant_metrics", "dt")
    act = act[(act["dt"] >= start) & (act["dt"] <= end)]
    act = act[[c for c in exp.columns]]
    return _compare("daily_merchant_metrics", exp, act.astype({c: "int64" for c in act.columns if c not in ("dt", "merchant_id")}) if len(act) else act, ["dt", "merchant_id"])


def validate_user_cohorts():
    conn = psycopg2.connect(OLTP_DSN)
    try:
        exp = pd.read_sql(COHORT_SQL, conn)
    finally:
        conn.close()
    act = _read_curated("user_cohorts", "cohort_week")[list(exp.columns)]
    return _compare("user_cohorts", exp, act, ["cohort_week", "weeks_since_signup"])


def validate_all(start="0000-01-01", end="9999-12-31"):
    return {"daily_merchant_metrics": validate_daily_merchant_metrics(start, end),
            "user_cohorts": validate_user_cohorts()}
