"""sarovar_freshness: hourly wall-clock freshness monitor. Fails loudly when a table's watermark lags real time
or the source holds changes far ahead of what has been loaded."""
from datetime import datetime, timedelta

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task

from sarovar import dq
from sarovar.logs import ctx_from_airflow

TABLES = ["users", "merchants", "transactions", "refunds"]

with DAG(
    dag_id="sarovar_freshness",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    params={"reference_now": Param(None, type=["null", "string"], description="Override 'now' (ISO) - used for drills"),
            "max_lag_hours": Param(26, type="integer")},
    tags=["sarovar", "dq"],
) as dag:
    for t in TABLES:
        @task(task_id=f"freshness_{t}")
        def _fresh(t=t):
            context = get_current_context()
            p = context["params"]
            now = datetime.fromisoformat(p["reference_now"]) if p.get("reference_now") else datetime.utcnow()
            dq.run_table_freshness(t, now=now, max_lag_hours=p["max_lag_hours"], ctx=ctx_from_airflow(context, table=t))
        _fresh()
