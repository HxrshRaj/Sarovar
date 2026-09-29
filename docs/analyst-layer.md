# Analyst layer (Phase 6)

All data is synthetic.

## Reusable views — `lake.analytics.*`
Defined in [`trino/views.sql`](../trino/views.sql), applied by `scripts/trino_init.py` (idempotent). Column-level docs are in the generated [data dictionary](data-dictionary.md).

| view | reads | use it for | notes |
|---|---|---|---|
| `daily_gmv` | `curated.daily_merchant_metrics` | GMV, attempts, success rate per day | filter on `dt` |
| `merchant_leaderboard` | `curated.daily_merchant_metrics` | merchant ranking, failure & refund rate | aggregates all days (small curated table) |
| `failure_reasons` | `raw.transactions` | failures by UPI code per day | **always filter `dt`** — reads the raw fact table |
| `retention_weekly` | `curated.user_cohorts` | signup-cohort retention % | |

None of the views exposes a PII column. Example: `SELECT * FROM lake.analytics.daily_gmv WHERE dt BETWEEN '2026-09-01' AND '2026-09-28' ORDER BY dt`.

## Superset dashboard
Superset 6.1.0 (`bi` profile, custom image adds the `trino` driver) connected to Trino as `trino://sarovar@trino:8080/lake`. [`bi/provision_superset.py`](../bi/provision_superset.py) creates, through the REST API, one database connection, 4 datasets (the views above), 6 charts (GMV KPI, daily GMV, success-rate line, top-10 merchants, failure-code donut, retention heatmap) and one dashboard *"Sarovar analytics (synthetic data, local model)"*.

Verified: the connection works and every view returns data through Superset's SQL Lab API (`/api/v1/sqllab/execute/`), and the chart-data API returns rows for a query on `daily_gmv`. **Not verified visually:** the built-in browser pane refused to render during this session, so I did not view the finished dashboard; the position layout is untested in a UI. Treat the dashboard as "provisioned and queryable", not "reviewed".

Run: `make up-bi && python bi/provision_superset.py` → http://localhost:8088 (local-dev login `admin`/`admin`, set in `docker-compose.yml`; never expose this).
RAM: Superset needs ~1–2 GB more; on an 8 GB Docker VM stop Airflow first (`docker compose stop airflow`) — that is what I did.

## Data catalog
[`docs/catalog.yml`](catalog.yml) is the single source of truth: per table description, owner, partition column; per column type, description and `pii` tag.
- **Data dictionary** [`docs/data-dictionary.md`](data-dictionary.md) is *generated* (`python scripts/gen_docs.py`); a unit test fails if the committed file is stale.
- **Kept in sync with the real schemas** by tests: `test_catalog_matches_live_schema` (catalog ⇔ PostgreSQL columns/types) and `test_trino_schemas_match_catalog` (catalog ⇔ Trino `information_schema` for raw, curated and analytics). The ingestion DAG's `schema_check` task also fails a run on source schema drift.
- The same file drives Trino DDL, the LLM context (PII columns removed), the discovery embeddings and the Analyst Console catalog browser.
- OpenMetadata was not used (RAM budget); this is a deliberately small alternative.
