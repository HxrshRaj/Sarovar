# Sarovar - run from Git Bash / WSL / Linux / macOS. Requires Docker, Python 3.11+.
# Profiles: core (postgres, minio, hive-metastore, trino, airflow) | ai (+api, frontend) | bi (+superset)
PY ?= python
VENV ?= .venv
PYBIN := $(VENV)/bin/python
ifeq ($(OS),Windows_NT)
PYBIN := $(VENV)/Scripts/python
endif
DC := docker compose
AF := $(DC) exec -T airflow airflow

.PHONY: help env venv up up-ai up-bi down clean seed ingest ingest-wait transform trino-init dq \
        test test-unit test-integration test-dags docs verify-core verify-p1 verify-p2 verify-p3 verify-p4 \
        drills logs

help:
	@grep -E '^[a-z0-9-]+:' Makefile | cut -d: -f1 | tr '\n' ' '; echo

env:
	@test -f .env || cp .env.example .env

venv:
	$(PY) -m venv $(VENV) && $(PYBIN) -m pip install -q -r requirements-dev.txt

up: env
	$(DC) --profile core up -d --build
up-ai: env
	$(DC) --profile ai up -d --build
up-bi: env
	$(DC) --profile bi up -d --build
down:
	$(DC) --profile core --profile ai --profile bi down
clean:
	$(DC) --profile core --profile ai --profile bi down -v

# ---- Phase 1: OLTP source (synthetic)
seed:
	$(PYBIN) source-db/generate.py --txns 300000 --users 20000 --merchants 600 --seed 42
verify-p1:
	$(DC) exec -T postgres psql -U sarovar -d sarovar_oltp -c "select 'users',count(*) from users union all select 'merchants',count(*) from merchants union all select 'transactions',count(*) from transactions union all select 'refunds',count(*) from refunds"

# ---- Phase 2/4: ingestion (Airflow catch-up over 2026-08-01..2026-09-28) + DQ
ingest:
	$(AF) dags unpause sarovar_ingest
	$(AF) dags unpause sarovar_freshness
ingest-wait:
	$(PYBIN) scripts/wait_for_runs.py sarovar_ingest 59
transform:
	$(AF) dags unpause sarovar_transform
	$(AF) dags trigger sarovar_transform --conf '{"start_dt":"2026-08-01","end_dt":"2026-09-28"}'
trino-init:
	$(PYBIN) scripts/trino_init.py
dq:
	$(PYBIN) -m pytest tests/integration -q -k "idempotent or row_count or injected or catalog"

# ---- tests
test-unit:
	$(PYBIN) -m pytest tests/unit -q
test-integration:
	$(PYBIN) -m pytest tests/integration -q
test-dags:
	$(DC) exec -T airflow python -m pytest /opt/sarovar/tests/dags -q -p no:cacheprovider
test: test-unit test-integration test-dags

docs:
	$(PYBIN) scripts/gen_docs.py

logs:
	$(DC) logs -f --tail 50 airflow
