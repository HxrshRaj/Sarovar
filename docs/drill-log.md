# On-call drill log — SIMULATED PRACTICE

> **This is simulated practice, not real on-call experience.** I injected each fault myself into a local Docker stack, alone, with no pager, no users, no stakeholders and no time pressure. Real on-call adds ambiguity, other people and urgency that none of this reproduces. All data is synthetic. Playbooks: [runbook.md](runbook.md). Timestamps are from the local run on 2026-09-29 (UTC).

Common tooling: Airflow UI (http://localhost:8081), JSON task logs (`dag_id, run_id, task_id, table, partition, rows`), `airflow dags/tasks` CLI, Airflow REST v2, `scripts/drill_helpers.py snapshot` (ETag/size/rows/sha256 of raw partitions).

---
## Drill 1 — source schema change
**Injected:** `ALTER TABLE transactions ADD COLUMN device_id text` and `ALTER TABLE refunds ALTER COLUMN amount_paise TYPE numeric(14,2)` in the OLTP source, then triggered `sarovar_ingest`.

**What broke:** run `manual__2026-09-29T08:41:41.826304+00:00` failed at `schema_check`; the 9 downstream tasks were `upstream_failed` (the gate stopped extraction, so no bad Parquet was written).

**Detected from:** Airflow grid view (red `schema_check`), and the task log:
```
"level": "ERROR", "event": "dq_check_failed", "task_id": "schema_check", "table": "transactions", "check": "schema_drift",
"message": "DQ FAIL [schema_drift] transactions : source schema differs from catalog: added=['device_id'] dropped=[] type_changed=[]"
```
**Escalation text I wrote:**
> [P2] sarovar pipeline failure — DAG `sarovar_ingest`, run `manual__2026-09-29T08:41:41.826304+00:00`, failing task `schema_check` (try 2/2). Source table `transactions` gained column `device_id` (and likely more: the check stops at the first table). No bad data was written; curated tables are unchanged. Asking the payments-core owner: is the schema change intentional? If yes I will add the column to `docs/catalog.yml`; otherwise please revert.

**Fix:** reverted both DDL changes (standing in for the owner reverting), then cleared the failed task. **Two surprises:** (1) `airflow tasks clear` could not target this run (manual runs have no logical date, so its `-s/-e` date filters matched nothing) → used Airflow REST v2 `clearTaskInstances` with `dag_run_id`; (2) after the clear, all four `extract_*` tasks failed with `TypeError: can't compare offset-naive and offset-aware datetimes` — **a real bug in my DAG**: the manual-trigger fallback window used a tz-aware `run_after`. The manual-trigger path had never been executed before this drill. Fixed `_naive()` in `airflow/dags/sarovar_ingest.py`, added `tests/dags/test_compute_window.py`, cleared again → run `success`.

**Improve:** collect *all* drift in one message instead of stopping at the first table (the `refunds` type change was never reported); treat additive columns as a warning with an explicit allow-list; test manual/backfill trigger paths in CI (now partly covered); document the REST clear in the runbook (done).

---
## Drill 2 — late data invisible to the incremental window (silent loss)
**Injected:** 25 extra transactions with `created_at` on 2026-09-10 but `updated_at = 2026-09-10 12:00` (older than any window still to be processed) — mimicking a writer with a skewed clock / a bulk restore. Source `dt=2026-09-10` went 5,300 → 5,325 rows.

**What happened:** a normal ingest run stayed **green** and `sarovar_reconcile` (as first written) also stayed **green**. So the pipeline had a blind spot twice over: the extract keys on `updated_at`, and my reconcile only deep-checked the last 14 days back from the newest date, while the bad date was 18 days back.

**Fix to the detector (drill finding):** `dq.reconcile_counts()` — compare source count per `created_at` date with the row count in the lake manifest for **every** partition (counts only, cheap) — added to `sarovar_reconcile`. Re-run:
```
"event": "dq_check_failed", "task_id": "reconcile_transactions", "check": "reconcile", "partitions_differing": 1, "message": "dt=2026-09-10: source 5325 vs lake 5300"
```
**Escalation text:**
> [P2] sarovar data completeness — DAG `sarovar_reconcile`, run `manual__2026-09-29T08:52:21.831463+00:00`, failing task `reconcile_transactions`. `transactions dt=2026-09-10`: source 5,325 rows vs lake 5,300 (25 missing). Cause: rows arrived with `updated_at` older than the loaded window, so the incremental extract never saw them. Curated daily metrics for 09-10 are understated until backfilled. I am running the backfill playbook; source owner: please check the writer's clock / bulk-insert job.

**Backfill executed (runbook §3B):** `dags trigger sarovar_ingest --conf '{"window_start":"2026-09-10T00:00:00","window_end":"2026-09-11T00:00:00"}'` → 6 partitions rewritten (late updates for 09-05…09-10 live in that window); `dt=2026-09-10` now 5,325 rows (sha256 changed `9a0a2297…` → `24a633c8…`); then `sarovar_transform` for 2026-09-01…09-28 (Spark → validation vs OLTP passed) and `sarovar_reconcile` → success.
**Cleanup:** deleted the 25 injected rows, rewrote the partition; sha256 returned to `9a0a2297…` (byte-identical to before the drill) — evidence of idempotency.
**Improve:** alert on reconcile failures; a scheduled *full* deep check weekly; source-side constraint (`updated_at` must be ≥ previous max) or CDC instead of timestamp polling.

---
## Drill 3 — data-freshness breach (simulated clock)
**Injected:** `sarovar_freshness` triggered with `reference_now = 2026-10-02T12:00:00` (**simulated**: real time cannot advance 3 days in a drill; this is the honest limit of this drill).
**Detected:** all four `freshness_*` tasks failed; log: `DQ FAIL [freshness] transactions : watermark 2026-09-29 08:48 is 75.2h behind 2026-10-02 12:00, limit 26h`.
**Escalation text:**
> [P1 if it were real] sarovar freshness breach — DAG `sarovar_freshness`, run `manual__2026-09-29T09:15:35.941250+00:00`, tasks `freshness_*`. Watermarks are 75.2 h behind (limit 26 h) for users, merchants, transactions, refunds. Checked: scheduler up, `sarovar_ingest` last run success for interval 2026-09-28, no runs since. Dashboards are stale for all data after 2026-09-29.
**Fix (runbook §2 → §3B):** backfill to catch the watermark up. My first attempt used `window_end=2026-10-01`: still 36 h behind the simulated clock → freshness failed again (my arithmetic, not the pipeline). Second backfill `2026-10-01…10-02` → all four `freshness_*` success. Afterwards I reset the watermark files to `2026-09-29T00:00:00` so the stack matches reality.
**Improve:** a real freshness alert must fire *before* the limit (e.g. warn at 18 h); the watermark advancing on an empty window can hide "no upstream data" — pair with a source-activity check.

---
## Drill 4 — task failure from a dependency outage (real fault)
**Injected:** `docker compose stop minio`, then triggered `sarovar_ingest` for window 2026-09-27…28.
**Detected:** run `failed`; `extract_*` tasks failed after the configured retry (2 attempts, 10 s apart); DQ/register/watermark tasks `upstream_failed`. Log: `EndpointConnectionError: Could not connect to the endpoint URL: "http://minio:9000/lake/raw/transactions/dt%3D2026-09-22/data.parquet"`.
**Escalation text:**
> [P2] sarovar ingest failing — DAG `sarovar_ingest`, run `manual__2026-09-29T09:20:17.103329+00:00`, failing tasks `extract_users/merchants/transactions/refunds`. Object store `minio:9000` is unreachable (connection refused). No data loss: extraction is idempotent and raw partitions are overwritten per partition. Restarting the storage service, then re-running failed tasks.
**Fix:** `docker compose start minio`; cleared failed tasks via REST; run → `success`; `pytest -k "row_count or idempotent"` green.
**Improve:** health check + alert on the object store itself; retries with backoff (10 s fixed retries are too short for a real storage outage).

---
## Drill 5 — duplicate run
**Injected:** two `sarovar_ingest` triggers with the identical window (2026-09-27…28), then two extract processes writing the same partitions *concurrently*, outside Airflow.
**Result:** both Airflow runs `success` (serialised by `max_active_runs=1`); `snapshot` of `dt=2026-09-27,28` before vs after: **identical** ETag, size, rows, sha256. The two concurrent writers logged identical `sha256`/row counts for both partitions, and the state afterwards was still identical (`diff` empty).
**Why it holds:** deterministic key + deterministic Parquet bytes + full-partition overwrite, so racing writers write the same object.
**Limits (not tested):** a concurrent writer racing with a *source change* could interleave different snapshots; the last write wins, and `sarovar_reconcile` would flag any divergence.
**Improve:** write to a temporary key + atomic rename/version marker if partitions ever become multi-file.
