"""sarovar_transform: Spark builds curated tables from the raw lake, validates them against the OLTP source with
independent SQL, then registers them in Trino. Default range = 7-day lookback (late updates land up to ~5 days late)."""
import subprocess
from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from sarovar import trino_utils, validate
from sarovar.logs import ctx_from_airflow, get_logger

with DAG(
    dag_id="sarovar_transform",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),  # real data intervals [start, end)
    catchup=False,
    max_active_runs=1,
    params={"start_dt": Param(None, type=["null", "string"], description="YYYY-MM-DD (inclusive); null = 7-day lookback"),
            "end_dt": Param(None, type=["null", "string"], description="YYYY-MM-DD (inclusive)")},
    tags=["sarovar", "spark"],
) as dag:

    def _range(context):
        p = context["params"]
        hi = context.get("data_interval_end") or context["dag_run"].run_after
        end = p.get("end_dt") or (hi - timedelta(days=1)).strftime("%Y-%m-%d")
        start = p.get("start_dt") or (pendulum.parse(end) - timedelta(days=7)).strftime("%Y-%m-%d")
        return start, end

    @task(task_id="spark_build_curated")
    def spark_build():
        context = get_current_context()
        start, end = _range(context)
        log = get_logger(ctx_from_airflow(context))
        log("spark_start", start=start, end=end)
        r = subprocess.run(["python", "/opt/sarovar/spark/build_curated.py", "--start", start, "--end", end],
                           capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError("spark job failed:\n" + r.stderr[-3000:])
        done = [ln for ln in r.stdout.splitlines() if ln.startswith("SPARK_DONE")]
        log("spark_done", summary=done[-1] if done else r.stdout[-300:])
        return {"start": start, "end": end}

    @task(task_id="validate_vs_source")
    def validate_vs_source(rng):
        log = get_logger(ctx_from_airflow(get_current_context()))
        res = validate.validate_all(rng["start"], rng["end"])
        log("curated_validated", rows_compared=res)

    @task(task_id="register_trino")
    def register_trino(*_):
        log = get_logger(ctx_from_airflow(get_current_context()))
        conn = trino_utils.connect()
        for t in ("daily_merchant_metrics", "user_cohorts"):
            log("trino_partitions_synced", table=f"lake.curated.{t}", partitions=trino_utils.register("curated", t, conn))

    r = spark_build()
    register_trino(validate_vs_source(r))
