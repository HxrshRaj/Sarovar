# Sarovar — a mini lakehouse data platform (synthetic, fintech-flavoured)

A learning/portfolio project: an OLTP payments database (UPI-style) → **Airflow** → partitioned **Parquet** on object storage → **Spark** curated tables → **Trino** → analyst layer (Superset, catalog) → a **local-LLM** text-to-SQL assistant with PII guardrails, plus data-quality gates, simulated on-call drills, and a small web console.

> **All data is synthetic** (seeded generator). **Not production.** Local open-weight model. On-call drills are **simulated practice**. See the honesty table below and [docs/limitations.md](docs/limitations.md).

```
                         ┌──────────────── Airflow 3.3.2 (LocalExecutor, standalone) ─────────────────┐
 PostgreSQL 16  ─extract─▶  s3://lake/raw/<table>/dt=YYYY-MM-DD/data.parquet  ─DQ gates─▶ Hive Metastore│
 (OLTP, synthetic)  ▲    │  (MinIO, Parquet+snappy, partition overwrite = idempotent)       │           │
   updated_at       │    │            │ Spark 3.5.3 (local) ─▶ s3://lake/curated/… ─validate vs OLTP SQL │
   watermark        │    └────────────┼──────────────────────────────────────────────────────────────┘
                    │                 ▼
                    │        Trino 483 (Hive connector)  ──▶ Superset 6.1 dashboards / analytics views
                    │                 ▲                          ▲
   sarovar_reconcile┘                 │ EXPLAIN (TYPE IO) cost gate
                                      │
 Ollama (llama3.2:3b, nomic-embed-text) ◀── redaction ◀── question ── FastAPI ◀── Next.js console
        ▲ metadata only, PII columns removed        │  SQL guard: SELECT-only · LIMIT · partition filter · no PII
        └────────── pgvector table/column embeddings (discovery)          MCP server (read-only Trino tools)
```

## What's here
| Area | Where | Docs |
|---|---|---|
| OLTP source + seeded generator (late-arriving updates, skew) | `source-db/` | [oltp-schema.md](docs/oltp-schema.md) |
| Airflow DAGs: ingest (incremental, idempotent), transform (Spark), freshness, reconcile | `airflow/` | [pipeline-flow.md](docs/pipeline-flow.md) |
| Spark curated tables + independent validation vs OLTP | `spark/`, `airflow/include/sarovar/validate.py` | |
| Data quality + structured logging | `airflow/include/sarovar/dq.py`, `logs.py` | [runbook.md](docs/runbook.md) |
| Trino, EXPLAIN, cost experiments, OLTP vs OLAP | `trino/`, `scripts/cost_experiments.py` | [cost-aware-querying.md](docs/cost-aware-querying.md), [oltp-vs-olap.md](docs/oltp-vs-olap.md) |
| Superset, views, catalog | `bi/`, `trino/views.sql`, `docs/catalog.yml` | [analyst-layer.md](docs/analyst-layer.md), [data-dictionary.md](docs/data-dictionary.md) |
| AI: text-to-SQL, guard, redaction, discovery, MCP, eval | `ai/` | [ai-guardrails.md](docs/ai-guardrails.md) |
| On-call drills (simulated) | | [drill-log.md](docs/drill-log.md) |
| Analyst Console (FastAPI + Next.js) | `api/`, `frontend/` | screenshots in [docs/screenshots](docs/screenshots) |
| Tests + CI | `tests/`, `.github/workflows/ci.yml` | |

## Run it
Requirements: Docker Desktop, Python 3.11+, (for the AI profile) [Ollama](https://ollama.com) on the host, `make` (or run the commands in the Makefile by hand).

**Hardware actually used / needed:** the full stack ran on a 12-logical-CPU Windows laptop with Docker limited to **8 GB RAM**. Approximate container limits: Airflow 3 GB, Trino 2.5 GB, Hive Metastore 1 GB, Postgres 0.75 GB, MinIO 0.5 GB (core ≈ 8 GB peak); Superset +2 GB (I stopped Airflow to run it); API 0.5 GB, frontend 0.4 GB. Ollama runs on the host CPU (llama3.2:3b ≈ 2 GB RAM). Plan on **≥ 12 GB free RAM for core + ai**, or run profiles separately.

```bash
cp .env.example .env            # local dev defaults, no real secrets
python -m venv .venv && .venv/Scripts/python -m pip install -r requirements-dev.txt   # (bin/ on Linux/macOS)

# --- profile: core  (postgres, minio, hive-metastore, trino, airflow)
make up                          # docker compose --profile core up -d --build
make seed                        # 300k synthetic transactions, seed 42
make ingest                      # unpause sarovar_ingest: catch-up runs 2026-08-01..2026-09-28 (59 runs, ~15-30 min)
make ingest-wait                 # blocks until all 59 succeed
make transform                   # Spark curated tables + validation vs OLTP + Trino registration
make trino-init                  # register tables, ANALYZE (stats for the SQL guard), analyst views
make test                        # unit + integration + DAG integrity

# --- profile: bi   (Superset; stop airflow first on an 8 GB VM)
make up-bi && python bi/provision_superset.py      # http://localhost:8088

# --- profile: ai   (API + console; needs Ollama with llama3.2:3b and nomic-embed-text pulled)
ollama pull llama3.2:3b && ollama pull nomic-embed-text
make build-index                 # embed catalog metadata (PII columns excluded) into pgvector
make up-ai                       # API http://localhost:8000, console http://localhost:3000
```
Ports: Airflow UI 8081, Trino 8080, MinIO console 9001, Postgres 5433, API 8000, console 3000, Superset 8088. `.env.example` lists every setting; **never commit `.env`**.

## Real / synthetic / simulated / pending
| Item | Status | Evidence / note |
|---|---|---|
| Pipeline (Airflow → Parquet → Spark → Trino) runs end to end | **Real** (local) | 59 ingest runs succeeded; transform validated vs OLTP SQL (32,262 + 134 rows compared, 0 diffs) |
| Idempotency | **Real** | `tests/integration/test_pipeline.py`, drill 5 (identical ETag/size/rows/sha256) |
| Query-cost measurements | **Real, small** | 300k rows, single Trino node; numbers in `docs/measurements/` |
| Text-to-SQL accuracy | **Real, low** | 47.5 % execution accuracy (19/40) with llama3.2:3b — see [ai-guardrails.md](docs/ai-guardrails.md) |
| PII guardrail tests | **Real** | unit + live-data tests; regex/keyword based, not a proof |
| All data (users, merchants, payments, refunds) | **Synthetic** | Faker + seeded numpy; no real person's data anywhere |
| On-call drills | **Simulated** | injected by me alone on a local stack — [drill-log.md](docs/drill-log.md) |
| Real AWS S3 pass | **Pending** | no AWS credentials in the environment; everything ran on MinIO |
| Public deployment / hosted demo | **Pending** | full stack does not fit a free tier; DuckDB "demo mode" not built; screenshots instead |
| Production readiness / scale | **Not claimed** | 300k rows, one laptop |
| Superset dashboard visual review | **Not done** | provisioned via API; queries verified through Superset's API |

## Honest headline numbers (all from runs in this repo)
- 300,000 transactions / 20,000 users / 600 merchants / 6,197 refunds; 59 daily partitions; 11.5 MB Parquet (transactions).
- Partition filter on one day: **57.6×** fewer bytes scanned than a full scan (73,807 B vs 4,249,139 B); compacted vs 700 small files: 7.6× faster (202 ms vs 1,541 ms).
- On this small data PostgreSQL was **faster** than single-node Trino for the same aggregation (170 ms vs 837 ms) — see [oltp-vs-olap.md](docs/oltp-vs-olap.md) for why that does not remove the case against querying OLTP.
- Data-discovery recall@3 = 0.91 (strict, gold-SQL-derived labels, n = 40); text-to-SQL execution accuracy 0.475.

## Repo layout
`source-db/ airflow/ spark/ trino/ bi/ ai/ api/ frontend/ docs/ tests/ scripts/` — plus `docker-compose.yml` (profiles `core`, `ai`, `bi`), `Makefile`, `.env.example`.
