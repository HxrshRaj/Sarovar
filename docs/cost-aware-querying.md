# Cost-aware querying with Trino (measured)

All data is synthetic. Everything below was measured by [`scripts/cost_experiments.py`](../scripts/cost_experiments.py); raw output is in [`docs/measurements/`](measurements/). Reproduce: `python scripts/cost_experiments.py`.

**Setup (what actually ran):** one Trino 483 node (coordinator + worker) in Docker capped at 2.5 GB, Hive connector + Hive Metastore, Parquet (snappy) on MinIO, 300,000 transactions in 59 `dt=` partitions (≈ 11.5 MB total). A 12-logical-CPU Windows laptop with Docker limited to 8 GB. Each query: 1 warm-up + 5 timed runs, median reported. Bytes/rows/drivers come from Trino's own query stats (`/v1/query/<id>`). This is a **small** dataset on **one** node: wall-time differences are small; the **bytes scanned** ratios are what carry over to bigger data.

## Connector choice
Hive connector + a standalone Hive Metastore (Postgres-backed). The Trino 483 docs list only `thrift` (HMS) and `glue` metastores for Hive (no file metastore), and Airflow writes plain `dt=YYYY-MM-DD/` Parquet directories, which map directly onto Hive external partitioned tables and `sync_partition_metadata`. Iceberg (JDBC catalog) would have worked too but adds a table-format layer the ingestion does not need. Not chosen ≠ untried: I did not run Iceberg.

## Reading EXPLAIN (what to look for)
`EXPLAIN` shows the plan *before* running; `EXPLAIN ANALYZE` runs it and adds real rows/bytes/time per operator. On a scan, look at:

1. **`TableScan` / `ScanFilterProject`** — the leaf that reads files.
2. **`Layout: [amount_paise:bigint]`** — the **projection**: only the columns listed are read from Parquet. If you see every column, you did `SELECT *` (or a wide view).
3. **`dt:string:PARTITION_KEY :: [[...]]`** — the **partition constraint** after pruning. A tight range means the metastore only lists those partitions' files.
4. **`filterPredicate = …`** on `ScanFilterProject` — a predicate on a *non-partition* column: it is applied while reading, not by skipping partitions.
5. **`Input: N rows`, `Physical input: X`, `Splits: n`** (EXPLAIN ANALYZE) — what was really read.

### Partition pruned (`WHERE dt = '2026-09-15'`)
```
└─ TableScan[table = lake:raw:transactions]
       Layout: [amount_paise:bigint]
       amount_paise := amount_paise:bigint:REGULAR
       dt:string:PARTITION_KEY
           :: [[2026-09-15]]
       Input: 5603 rows (49.25kB), Physical input: 72.08kB, Splits: 1
```
### Full scan (no `dt` predicate) — how to spot it
```
└─ TableScan[table = lake:raw:transactions]
       Layout: [amount_paise:bigint]
       dt:string:PARTITION_KEY
           :: [[2026-08-01, 2026-09-28]]        <- the whole partition range
```
### Filter on a non-partition column (`created_at`) — looks selective, still opens every partition
```
└─ ScanFilterProject[table = lake:raw:transactions, filterPredicate = ((timestamp(3) '2026-09-15 00:00:00.000' <= created_at) AND (created_at < timestamp(3) '2026-09-16 00:00:00.000'))]
       created_at := created_at:timestamp:REGULAR
       dt:string:PARTITION_KEY
           :: [[2026-08-01, 2026-09-28]]        <- all 59 partitions listed
       Input: 5603 rows
```
The rule of thumb: **a full table scan is a `PARTITION_KEY` range that spans the whole table**. Parquet min/max statistics let the reader discard row groups inside each file (only 5,603 rows were decoded), but all 59 files were still opened (68 drivers, 2.96 MB read, vs 73.8 kB for the `dt` filter).

## Experiments

### E1 — full scan vs partition filter (`SELECT count(*), sum(amount_paise) FROM lake.raw.transactions …`)
| query | bytes read | rows read | drivers | median wall |
|---|---:|---:|---:|---:|
| no `dt` filter (full scan) | 4,249,139 | 300,000 | 68 | 614.5 ms |
| `dt = '2026-09-15'` | 73,807 | 5,603 | 10 | 244.6 ms |
| `dt` between (7 days) | 522,215 | 40,479 | 16 | 281.1 ms |
| `created_at` range (non-partition) | 2,961,616 | 5,603 | 68 | 412.9 ms |

One-day filter: **57.6× fewer bytes** (4,249,139 / 73,807) and 2.5× less wall time. Bytes scale with partitions touched; wall time here is dominated by fixed planning/scheduling overhead, which is why the wall-time gain is much smaller than the bytes gain on a dataset this size.

### E2 — `SELECT *` vs two columns (7 days, all rows fetched)
| query | bytes read | median wall |
|---|---:|---:|
| `SELECT *` | 1,866,632 | 1,366.0 ms |
| `SELECT txn_id, amount_paise` | 1,332,984 | 449.6 ms |

Columnar pruning works, but the byte gap here (1.4×) is modest because the two chosen columns are high-entropy integers that compress poorly, whereas low-cardinality columns (`status`, `failure_code`) compress to almost nothing. The wall-time gap (3×) is mostly serialising 9 columns back to the client. Do not extrapolate a fixed ratio from this run.

**Config gotcha found while measuring:** with Trino's default `parquet.small-file-threshold` (3 MB) every file here is *read whole*, so `SELECT *` and a 2-column query reported identical bytes (1,537,093 for all three 7-day queries; and 11,462,482 for the full scan, the size of all files). I set `parquet.small-file-threshold=0B` in `trino/catalog/lake.properties` to expose column pruning; the default-config runs are kept in `docs/measurements/*_default_config.json`. In production with ≥ 128 MB files this threshold is irrelevant.

### E3 — many small files vs compacted (same 7 days, 38,481 rows, `GROUP BY status`)
| layout | files | bytes read | drivers | median wall |
|---|---:|---:|---:|---:|
| 100 files per partition | 700 | 3,900,452 | 710 | 1,540.7 ms |
| 1 file per partition (compacted) | 7 | 520,414 | 23 | 202.5 ms |

7.6× slower and 7.5× more bytes with small files: each file carries its own footer/metadata and dictionary pages and needs its own split/driver. Ingestion by day-partition (one file per partition) is already the compacted layout; the small-file layout is what streaming micro-batch writers produce.

### E4 — storage growth per partition and what it means for compute
From the partition manifests (`docs/measurements/trino_experiments.json → storage_growth_transactions`): 59 partitions, 300,000 rows, **11,462,482 bytes** in total (≈ 38.3 bytes/row on average); first partition 3,695 rows / 144,503 B, last 5,796 rows / 222,382 B (daily volume grows ~1.6× over the window by construction).
- A **full scan reads everything retained**: cost grows linearly with history (+~0.19 MB per new day here; 4.25 MB actually read for these two columns).
- A **`dt`-filtered query reads only its partitions**: cost is flat as history grows (73.8 kB for one day, whether the table holds 59 days or 590).
- So the compute bill for an unfiltered dashboard grows *every day* while the filtered one does not; that is the whole argument for partition filters, retention policies and compaction. Only ratios are claimed here; I did not measure a monetary cost.

## Practical checklist
1. Filter on the partition column with literals (`dt = '…'`, `dt BETWEEN …`), not functions of it.
2. Check the plan: does the `PARTITION_KEY` range cover the table?
3. Select the columns you need; avoid `SELECT *` on wide tables.
4. Keep files large (compact); avoid thousands of tiny files.
5. Use curated tables/views for dashboards instead of re-aggregating raw.
