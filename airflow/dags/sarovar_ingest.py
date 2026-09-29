"""sarovar_ingest: PostgreSQL (OLTP) -> Parquet partitioned by dt=YYYY-MM-DD on MinIO, with data-quality gates.

schema_check -> extract_<table> -> dq_<table> -> register_trino -> watermark_lag
Idempotent (partition-level overwrite). Backfill: trigger with window_start/window_end params or use catchup.
"""
from datetime import datetime, timedelta

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from sarovar import dq, extract, trino_utils
from sarovar.config import PIPELINE_START
from sarovar.logs import ctx_from_airflow, get_logger

TABLES = ["users", "merchants", "transactions", "refunds"]
START = pendulum.parse(PIPELINE_START).naive()


def _naive(x):
    return x.naive() if hasattr(x, "naive") else x


def compute_window(context):
    """[lo, hi) on updated_at. First interval => epoch (initial load). Params override for backfills."""
    p = context["params"]
    lo, hi = context.get("data_interval_start"), context.get("data_interval_end")
    if lo is None or hi is None:  # manual trigger without an interval
        hi = context["dag_run"].run_after
        lo = hi - timedelta(days=1)
    lo, hi = _naive(lo), _naive(hi)
    if lo <= START:
        lo = extract.EPOCH
    if p.get("window_start"):
        lo = datetime.fromisoformat(p["window_start"])
    if p.get("window_end"):
        hi = datetime.fromisoformat(p["window_end"])
    return lo, hi


def _on_failure(context):
    get_logger(ctx_from_airflow(context))("task_failed", level="ERROR", exception=str(context.get("exception")))


with DAG(
    dag_id="sarovar_ingest",
    description="OLTP -> partitioned Parquet lake (synthetic data) with DQ gates",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),  # real data intervals [start, end)
    catchup=True,
    max_active_runs=1,  # partitions are shared across intervals; serialise runs
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10), "on_failure_callback": _on_failure},
    params={
        "window_start": Param(None, type=["null", "string"], description="Backfill override, ISO timestamp (inclusive) on updated_at"),
        "window_end": Param(None, type=["null", "string"], description="Backfill override, ISO timestamp (exclusive) on updated_at"),
        "register_trino": Param(True, type="boolean"),
    },
    tags=["sarovar", "ingest"],
) as dag:

    @task(task_id="schema_check")
    def schema_check():
        ctx = ctx_from_airflow(get_current_context())
        for t in TABLES:
            dq.run_schema_check(t, ctx)

    def make_extract(table):
        @task(task_id=f"extract_{table}")
        def _extract():
            context = get_current_context()
            lo, hi = compute_window(context)
            ctx = ctx_from_airflow(context, table=table)
            return [m["dt"] for m in extract.extract_table(table, lo, hi, ctx)]
        return _extract

    def make_dq(table):
        @task(task_id=f"dq_{table}")
        def _dq(partitions):
            ctx = ctx_from_airflow(get_current_context(), table=table)
            log = get_logger(ctx)
            log("dq_start", partitions=len(partitions))
            for dt in partitions:
                dq.run_partition_checks(table, dt, ctx)
            return partitions
        return _dq

    @task(task_id="register_trino")
    def register_trino(*_):
        context = get_current_context()
        if not context["params"].get("register_trino", True):
            return
        log = get_logger(ctx_from_airflow(context))
        conn = trino_utils.connect()
        for t in TABLES:
            n = trino_utils.register("raw", t, conn)
            log("trino_partitions_synced", table=f"lake.raw.{t}", partitions=n)

    @task(task_id="watermark_lag")
    def watermark_lag(*_):
        context = get_current_context()
        lo, hi = compute_window(context)
        ctx = ctx_from_airflow(context)
        for t in TABLES:
            dq.run_table_freshness(t, now=hi, max_lag_hours=26, ctx=ctx, compare_source=False)

    checks = []
    sc = schema_check()
    for t in TABLES:
        ex = make_extract(t)()
        sc >> ex
        checks.append(make_dq(t)(ex))
    reg = register_trino(*checks)
    watermark_lag(reg)
