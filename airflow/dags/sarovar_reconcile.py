"""sarovar_reconcile: daily source-vs-lake reconciliation: row counts of EVERY partition (cheap) + deep checks on recent ones.

Why it exists (found in drill 2, docs/drill-log.md): an incremental extract driven by updated_at cannot see rows whose
updated_at is *older than the loaded window* (a writer with a skewed clock, a bulk back-fill in the source, restored rows).
Row-count reconciliation of every recent partition catches that class of silent loss."""
from datetime import date, datetime, timedelta

import pendulum
import psycopg2
from airflow.sdk import DAG, Param, get_current_context, task

from sarovar import dq
from sarovar.config import OLTP_DSN
from sarovar.logs import ctx_from_airflow, get_logger

TABLES = ["users", "merchants", "transactions", "refunds"]

with DAG(
    dag_id="sarovar_reconcile",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    params={"lookback_days": Param(14, type="integer", description="Reconcile partitions created in the last N days (source dates)"),
            "reference_date": Param(None, type=["null", "string"], description="YYYY-MM-DD; default = latest created_at date in source")},
    tags=["sarovar", "dq"],
) as dag:
    for t in TABLES:
        @task(task_id=f"reconcile_{t}")
        def _reconcile(t=t):
            context = get_current_context()
            ctx = ctx_from_airflow(context, table=t)
            p = context["params"]
            conn = psycopg2.connect(OLTP_DSN)
            try:
                dq.reconcile_counts(t, ctx, conn=conn)  # every partition, counts only (cheap)
                with conn.cursor() as cur:
                    ref = date.fromisoformat(p["reference_date"]) if p.get("reference_date") else None
                    if ref is None:
                        cur.execute(f"SELECT max(created_at::date) FROM {t}")
                        ref = cur.fetchone()[0]
                    cur.execute(f"SELECT DISTINCT created_at::date FROM {t} WHERE created_at::date > %s ORDER BY 1",
                                (ref - timedelta(days=int(p["lookback_days"])),))
                    dts = [r[0].isoformat() for r in cur.fetchall()]
                get_logger(ctx)("reconcile_start", partitions=len(dts), lookback_days=p["lookback_days"])
                failures = []
                for dt in dts:
                    try:
                        dq.run_partition_checks(t, dt, ctx, conn=conn)
                    except dq.DataQualityError as e:
                        failures.append(str(e))
                if failures:
                    raise dq.DataQualityError("reconcile", t, None, f"{len(failures)} of {len(dts)} recent partitions failed: " + " | ".join(failures[:5]))
                get_logger(ctx)("reconcile_ok", partitions=len(dts))
            finally:
                conn.close()
        _reconcile()
