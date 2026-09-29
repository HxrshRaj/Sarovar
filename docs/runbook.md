# Pipeline runbook (Sarovar, synthetic data)

Written for the simulated on-call drills in [drill-log.md](drill-log.md) — **practice material, not real on-call experience**. Commands were executed against the local stack while running those drills.

Conventions: `AF="docker compose exec -T airflow airflow"`. Airflow UI: http://localhost:8081 (DAG → run → task → Logs). Structured logs are JSON lines with `dag_id, run_id, task_id, table, partition, rows`; filter with `grep '"level": "ERROR"'`.

Severity guide: **P1** = curated/analyst data is wrong or missing and consumers are affected; **P2** = pipeline failing but data still within freshness SLA (26 h); **P3** = warning/cosmetic.

## 0. First five minutes (any alert)
1. Which DAG, which run, which task? (`$AF dags list-runs <dag_id> | head`; UI grid view.)
2. Read the **task log** — DQ failures print `DQ FAIL [check] table partition: detail` (row_count, not_null, unique_key, partition_exists, freshness, schema_drift, curated_vs_source, reconcile).
3. Is the data consumers see still correct? (Curated tables are only rebuilt after DQ passes; raw partitions are overwritten atomically per partition.)
4. Decide: retry (safe — every task is idempotent), fix + retry, or escalate.

## 1. Playbook: failed run
| Step | Action |
|---|---|
| Detect | Airflow shows a red task; log has `task_failed` with `exception`. `$AF tasks states-for-dag-run <dag_id> <run_id>` |
| Classify | **Infrastructure** (connection refused / timeout to postgres, minio, trino): fix dependency, retry. **Data** (`DQ FAIL`): do *not* blindly retry — the source data or schema changed; go to the matching section below. **Code** (stack trace): revert/fix, retry. |
| Retry (safe) | Clear the failed task and its downstream. Scheduled runs: `$AF tasks clear <dag_id> -t '<failed_task>' -d -y -s <logical_date> -e <logical_date+1d>`. **Manual/backfill runs have no logical date, so the CLI matches nothing** (found in drill 1) - use the REST API: `TOK=$(curl -s -X POST localhost:8081/auth/token -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \| python -c "import sys,json;print(json.load(sys.stdin)['access_token'])")` then `curl -X POST localhost:8081/api/v2/dags/<dag_id>/clearTaskInstances -H "Authorization: Bearer $TOK" -H 'Content-Type: application/json' -d '{"dry_run":false,"dag_run_id":"<run_id>","only_failed":true,"include_downstream":true}'`. Idempotent partition overwrite means no duplicates. |
| Verify | Task green, `dq_*` tasks green, and `python -m pytest tests/integration -q -k "row_count or idempotent"`. |
| Escalate if | The same task fails after a fix, or the failure is a source schema change/data problem owned by another team (below). |

**Escalation message template**
```
[P2] sarovar pipeline failure
DAG: <dag_id>   Run: <run_id>   Failing task: <task_id> (try <n>)
Window: <window_lo> .. <window_hi>   Table/partition: <table> dt=<...>
Symptom: <first ERROR line from the log>
Impact: <which curated tables/dashboards are stale or at risk; consumers>
Done so far: <retries, checks>   Next step / ask: <what you need from whom>
```

## 2. Playbook: data-freshness breach
Alert: `sarovar_freshness` (hourly) or `watermark_lag` fails: `DQ FAIL [freshness] <table>: watermark … is N h behind …, limit 26h`.
1. Is the scheduler up and the DAG unpaused? `$AF dags list | grep sarovar` (`is_paused`), UI → Scheduler health.
2. Did the latest `sarovar_ingest` run fail or is it stuck (`running` for hours)? `$AF dags list-runs sarovar_ingest | head -5`.
3. Is the *source* stale rather than the pipeline? Compare `select max(updated_at) from <table>` (source) with `_state/<table>/watermark.json` (lake). If the source itself has no new data, the breach is upstream: notify the source-system owner.
4. If runs are missing (scheduler was down): run the **backfill playbook** for the missing intervals.
5. Confirm: `$AF dags trigger sarovar_freshness` goes green.

## 3. Playbook: backfill
Use when partitions are missing/wrong, after an outage, or after a fix to extraction logic.

**A. Whole days by data interval (catch-up):** clear the failed/missing runs; `catchup=True` re-creates missing intervals.
`$AF backfill create --dag-id sarovar_ingest --from-date 2026-09-10 --to-date 2026-09-12 --reprocess-behavior completed` (Airflow 3 syntax, `dags backfill` was removed; confirmed via `--help` but **not executed in the drills** - the drills used option B).

**B. Explicit window on `updated_at` (also for rows with stale `updated_at`):**
```
$AF dags trigger sarovar_ingest --conf '{"window_start":"2026-09-20T00:00:00","window_end":"2026-09-21T00:00:00","register_trino":true}'
```
Every partition that has a row with `updated_at` in the window is rebuilt in full and overwritten (idempotent). Running it twice is harmless.

**C. Rebuild curated tables afterwards:**
`$AF dags trigger sarovar_transform --conf '{"start_dt":"2026-09-01","end_dt":"2026-09-28"}'` (Spark → validation vs OLTP → Trino registration).

**Verify:** `sarovar_reconcile` green (`$AF dags trigger sarovar_reconcile`), `dq_*` tasks green, `validate_vs_source` green.
**Never** delete objects by hand under `raw/`; re-run the extract instead.

## 4. Specific failure signatures
| Log line | Meaning | Action |
|---|---|---|
| `DQ FAIL [schema_drift] <table>: source schema differs from catalog: added=[…] dropped=[…]` | Source team changed a table | Ask owner if intentional; update `docs/catalog.yml` + regenerate docs/DDL, re-run; if a column was dropped/typed differently, treat as P1 (extract would write wrong data, so the gate stops it) |
| `DQ FAIL [row_count] … source has A rows but lake partition has B` | Late/lost rows, partial write, duplicate write | Run backfill B for that date; check for stale `updated_at` (drill 2) |
| `DQ FAIL [unique_key]` / `[not_null]` | Bad source data or bad extract | Inspect source; do not publish; escalate to source owner |
| `DQ FAIL [partition_exists]` | Partition never written / deleted | Backfill B |
| `DQ FAIL [freshness]` | See §2 | |
| `DQ FAIL [curated_vs_source]` | Spark output disagrees with OLTP SQL | Check raw partitions with reconcile; re-run transform |
| `Connection refused` / `EndpointConnectionError` | Dependency down (drill 4) | Restart dependency, clear failed tasks (§1) |
