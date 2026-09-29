# Limitations and honest caveats

Everything in this repository is a learning/portfolio project. It is **not production**, and none of it ran at production scale.

## Data
- **All data is synthetic**: seeded generator (`source-db/generate.py`, seed 42), Faker for names, `@example.org` emails, fictional `@sarovar` VPAs. No real user data exists anywhere in the repo.
- **Scale actually run:** 20,000 users, 600 merchants, **300,000 transactions**, 6,197 refunds, 59 daily partitions (2026-08-01 … 2026-09-28), ≈ 11.5 MB of Parquet for transactions. Hardware: one Windows 11 laptop, 12 logical CPUs, 15.7 GB RAM of which Docker Desktop was given 8 GB. Nothing here says anything about behaviour at millions or billions of rows.
- The generator's distributions (Zipf merchants, hourly peaks, an injected outage) are plausible, not calibrated against real UPI traffic.
- The OLTP source keeps only the latest version of each row; the lake therefore converges to the latest state and cannot reproduce "as of an earlier day".

## Pipeline
- **Airflow 3.3.2 `standalone`** (development mode, `SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`, LocalExecutor, one container). Not an HA deployment; no auth in front of it.
- Partition rewrites are serialised (`max_active_runs=1`), so throughput is deliberately low (~13–40 s per daily run).
- Deletes in the source are not modelled (no tombstones / soft-deletes).
- The extract window is `updated_at`-based. Rows whose `updated_at` is older than the loaded window are invisible to it; `sarovar_reconcile` detects that after the fact (drill 2) but does not fix it automatically.
- Spark runs in **local mode inside the Airflow container** (PySpark 3.5.3), not on a cluster. No Scala job was written (the brief said "welcome if it fits"; it did not fit the time budget).
- Trino is a **single node** (coordinator + worker) with 2.5 GB. Cost experiments therefore measure bytes/splits/single-node wall time only, on 300k rows (see [cost-aware-querying.md](cost-aware-querying.md)). Iceberg was not tried.
- MinIO image is the last Bitnami-legacy build (2025-05-24), because MinIO no longer publishes images on Docker Hub/Quay; it is unmaintained upstream and used only for local development. Real AWS S3 was **not** exercised (no credentials in the environment): listed as pending.

## Data quality
- Checks are hand-written (row count vs source per partition, key nulls, key uniqueness, freshness, schema drift, curated-vs-source SQL). No statistical/anomaly checks, no distribution drift.
- The freshness panel compares against wall-clock time; because the data is a static snapshot ending 2026-09-28, it turns "stale" after 26 h unless the DAGs are re-run.

## AI layer
- Model: **`llama3.2:3b` (Q4_K_M) via Ollama**, run locally on CPU; embeddings `nomic-embed-text`. A 3B model is weak at SQL: see the measured accuracy and failure modes in [ai-guardrails.md](ai-guardrails.md). Latency is 10–60 s per question on this CPU.
- Evaluation set: 40 hand-written questions + gold SQL over this one schema, written once before the first run. Small n (each question is 2.5 points of accuracy); one run per configuration, temperature 0, fixed seed; results can shift with other Ollama/model versions or hardware.
- **PII protection is layered but not a proof.** Regex redaction catches structured identifiers (phone, email, VPA, 12-digit refs, PAN, Aadhaar-like); free-text personal names are only caught when they exactly match a name in the source (the API loads names into memory to do this). A user typing a name not in that set would reach the *local* model. The SQL guard blocks PII columns by name from a hand-maintained catalog tag; a newly added PII column that nobody tags would not be blocked. Guarantees are tested only against the synthetic data.
- The MCP server was tested with the official Python MCP client over stdio (`mcp==2.2.0`), not with a third-party IDE client.

## On-call drills
- **Simulated practice.** Faults were injected by me into a local stack, alone, with no pager, no stakeholders and no time pressure. This is not on-call experience; see [drill-log.md](drill-log.md).

## Console and deployment
- The Analyst Console runs locally with Docker Compose (`ai` profile). **No public deployment exists**: the full stack needs ≈ 8 GB RAM and a local Ollama, which no free host provides, and I cannot create hosting accounts. A DuckDB-only "demo mode" was *not* built. Deployment is listed as pending in the README.
- Superset dashboards were provisioned via the REST API and their data queries verified through Superset's API; the rendered dashboard was not visually reviewed.
