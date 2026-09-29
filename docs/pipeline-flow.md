# Pipeline flow: one task traced from source table to partition path

All data is synthetic. Code: [`airflow/dags/sarovar_ingest.py`](../airflow/dags/sarovar_ingest.py), [`airflow/include/sarovar/extract.py`](../airflow/include/sarovar/extract.py).

```
PostgreSQL ──extract_<table>──▶ s3://lake/raw/<table>/dt=YYYY-MM-DD/data.parquet ──dq_<table>──▶ register_trino ──▶ watermark_lag
 (OLTP)         (Airflow)                (MinIO, Parquet+snappy)                 (row count,       (sync partitions)   (freshness)
                                                                                  nulls, unique)
                        └──▶ s3://lake/_state/<table>/{dt=...json, watermark.json}   (manifest: rows, bytes, sha256)
```

Task graph (11 tasks): `schema_check → extract_{users,merchants,transactions,refunds} → dq_<same> → register_trino → watermark_lag`.

## Trace: `sarovar_ingest` → `extract_transactions`, run `scheduled__2026-08-06T00:00:00+00:00`

This run's **data interval** is `[2026-08-05 00:00, 2026-08-06 00:00)` (a `CronDataIntervalTimetable`, so the interval is real; a plain `"@daily"` string in Airflow 3 gives a zero-length interval — I hit exactly that bug and fixed it, see `git log`).

1. **Window.** `compute_window()` sets `lo = data_interval_start`, `hi = data_interval_end` (the *first* interval, ≤ 2026-08-01, uses the epoch as `lo` = initial load). `window_start` / `window_end` DAG params override both for backfills. Log line from the real run:
   `extract_start … "window_lo": "2026-08-05T00:00:00", "window_hi": "2026-08-06T00:00:00", "partitions_changed": 5`
2. **Which partitions changed?** `SELECT DISTINCT created_at::date FROM transactions WHERE updated_at >= lo AND updated_at < hi`. In this window the source had these updates, grouped by the row's `created_at` date (measured with psql):

   | dt (created_at date) | rows with updated_at in window |
   |---|---|
   | 2026-08-01 | 30 |
   | 2026-08-02 | 21 |
   | 2026-08-03 | 25 |
   | 2026-08-04 | 12 |
   | 2026-08-05 | 3219 |

   The 88 rows in the first four are **late-arriving updates** (PENDING payments that resolved days later). They live in *old* partitions, so those partitions must be rewritten.
3. **Rebuild each changed partition in full.** For `dt=2026-08-01`: `SELECT <cols> FROM transactions WHERE created_at >= '2026-08-01' AND created_at < '2026-08-02' ORDER BY txn_id` → 3,695 rows → Arrow table with the catalog's schema → Parquet (snappy, deterministic writer settings).
4. **Partition-level overwrite.** The object key is deterministic: `s3://lake/raw/transactions/dt=2026-08-01/data.parquet`. It is `PUT` (overwrite), then any other object under `dt=2026-08-01/` is deleted. Never append. Manifest written to `_state/transactions/dt=2026-08-01.json`: `{"rows": 3695, "bytes": 144503, "sha256": "9f242fe6…"}`.
5. **Watermark.** `_state/transactions/watermark.json` advances to `max(existing, hi)`; it never regresses when an old interval is re-run.
6. **Result of the task:** 5 partitions, 17,223 rows written (log `extract_done`), XCom = the 5 `dt` values.
7. **`dq_transactions`** receives those `dt`s and for each: partition exists, row count == `SELECT count(*)` at source for that date, no NULLs in key columns, `txn_id` unique.
8. **`register_trino`** runs `CALL lake.system.sync_partition_metadata('raw','transactions','FULL')` so Trino sees `dt=…` partitions.
9. **`watermark_lag`** fails if the table's watermark is more than 26 h behind the interval end.

## Why it is idempotent
Output is a pure function of (source rows for that date, catalog schema). Same key, same bytes → re-running a task or a whole run changes nothing. Proven by `tests/integration/test_pipeline.py::test_extract_is_idempotent_same_date_twice` (row counts, sha256, S3 ETag and object listing identical) and `test_full_window_rerun_changes_nothing`.

Caveat, stated plainly: if the *source changed between the two runs*, the second run correctly produces different data (it converges to the latest source state). Idempotency here means "same inputs → same output, no duplicates", not "frozen history". A run cannot be reproduced "as of that day" because the OLTP source keeps only the latest version of each row.

## Backfills
- **Catch-up:** `catchup=True`, `start_date=2026-08-01`, `max_active_runs=1` (runs share partitions, so they are serialised). Enabling the DAG produced 59 daily runs (2026-08-01 … 2026-09-28), all successful.
- **Ad-hoc:** trigger with `{"window_start": "2026-09-10T00:00:00", "window_end": "2026-09-12T00:00:00"}` — see [runbook.md](runbook.md).
