"""CI smoke: run the full incremental extract for every table (bypassing the Airflow scheduler), then the DQ gates
and an idempotency check. Needs postgres (seeded) + minio only. Exits non-zero on any failure."""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "include"))
from sarovar import catalog, dq, extract  # noqa: E402

lo, hi = extract.EPOCH, datetime(2026, 9, 29)
sha1 = {}
for t in catalog.source_tables():
    parts = extract.extract_table(t, lo, hi, {"dag_id": "smoke", "run_id": "smoke-1", "task_id": f"extract_{t}"})
    sha1[t] = {m["dt"]: m["sha256"] for m in parts}
    for m in parts:
        dq.run_partition_checks(t, m["dt"], {"dag_id": "smoke", "run_id": "smoke-1", "task_id": f"dq_{t}"})
    dq.run_schema_check(t)
    dq.run_table_freshness(t, now=hi, max_lag_hours=26, compare_source=False)
    print(f"{t}: {len(parts)} partitions, {sum(m['rows'] for m in parts)} rows, all DQ checks passed")
for t in catalog.source_tables():  # second full run must be byte-identical
    again = {m["dt"]: m["sha256"] for m in extract.extract_table(t, lo, hi, {"run_id": "smoke-2"})}
    assert again == sha1[t], f"{t}: re-run changed output"
print("idempotency: second full run produced identical checksums for all partitions")
