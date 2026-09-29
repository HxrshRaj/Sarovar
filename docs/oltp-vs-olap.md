# OLTP vs OLAP: why analysts should not query the source (measured)

All data is synthetic. Measured by [`scripts/oltp_vs_olap.py`](../scripts/oltp_vs_olap.py); raw output: [`docs/measurements/oltp_vs_olap.json`](measurements/oltp_vs_olap.json).
Hardware/setup: 12-logical-CPU Windows laptop, Docker limited to 8 GB; PostgreSQL 16 container (768 MB), single-node Trino 483 container (2.5 GB); 300,000 `transactions` (69.7 MB on disk incl. indexes, `pg_total_relation_size`) + 600 `merchants`.

## The query
Daily successful GMV by merchant category (a join + filter + group-by over the whole fact table):
```sql
SELECT m.category, t.created_at::date AS d, count(*) AS n, sum(t.amount_paise)/100.0 AS gmv
FROM transactions t JOIN merchants m ON m.merchant_id = t.merchant_id
WHERE t.status = 'SUCCESS' GROUP BY 1, 2
```
Both engines return the same 590 rows.

## Result 1 — raw speed: at this size PostgreSQL is *faster*
| engine | median (7 runs) | min – max | data read |
|---|---:|---:|---|
| PostgreSQL (row store, seq scan) | **170.0 ms** | 141.5 – 182.6 ms | 4,056 shared buffers × 8 KB ≈ 33 MB (all columns of every row) |
| Trino over Parquet on MinIO | **837.3 ms** | 697.5 – 1,572.1 ms | 7,034,102 B, 300,600 rows (only the needed columns) |

This is the honest result and it should not be hidden: 300k rows fit in PostgreSQL's cache (`Buffers: shared hit=4065`, zero disk reads), and Trino pays fixed costs (planning, split scheduling, S3 round-trips) that dominate at this scale on one node. I did **not** measure a larger dataset, so the crossover point is unmeasured. The claim "OLAP engines are faster" is only true once data or concurrency outgrow the OLTP box, so the case against analysts querying OLTP rests on the other measurements and reasoning below, not on this one.

## Result 2 — bytes touched
PostgreSQL's row layout forced it to read ≈ 33 MB of heap pages to answer a query that needs 4 of 9 columns; Trino read 7.0 MB (≈ 4.7× less), and the gap widens as tables get wider because Parquet reads only referenced columns (see [cost-aware-querying.md](cost-aware-querying.md), E2). A partition filter would cut Trino further; PostgreSQL would need an index or partitioning and still reads whole rows.

## Result 3 — interference with the transactional workload
Point-lookup latency (`SELECT * FROM transactions WHERE txn_id = ?`, 400 random lookups) on PostgreSQL:

| scenario | p50 | p95 | p99 |
|---|---:|---:|---:|
| idle | 2.55 ms | 4.64 ms | 6.01 ms |
| while 4 workers loop the analytic query **on PostgreSQL** (43 analytic queries completed) | 1.38 ms | 4.21 ms | **12.53 ms** |
| while 4 workers loop the analytic query **on Trino** (8 completed) | 2.52 ms | 11.87 ms | 17.94 ms |

Read this carefully — it is *not* a clean win for the argument: p50 and p95 did not degrade under PostgreSQL analytics (p50 got lower, which is warm-cache noise from the idle run being first), only the **p99 tail roughly doubled** (6.0 → 12.5 ms). And running the analytics on Trino hurt the lookups as much (p95 11.9 ms, p99 17.9 ms) because Trino runs on the *same laptop* and competes for the same CPUs — this test does not isolate Trino onto separate hardware, which is what production would do. With one run per scenario and n = 400, differences of this size are within run-to-run noise; treat them as directional only.

## So why not query the OLTP source?
What the measurements support: (a) a row store reads ~4.7× more bytes than columnar Parquet for this query and the gap grows with table width; (b) analytic scans showed a p99 tail effect on transactional lookups even at 4 workers and 300k rows.
What is standard reasoning I did **not** measure here: at production scale a single unbounded scan can evict the buffer cache, hold long snapshots that bloat vacuum, and take locks/replicas behind; analysts can also read PII columns directly; OLTP schemas are normalised for writes, not for analysis; and the lake keeps history / late-updated rows in a governed, partition-pruned layout with data-quality gates (see [pipeline-flow.md](pipeline-flow.md)). The right shape is: OLTP → lake → curated tables/views → analysts, with PII excluded.
