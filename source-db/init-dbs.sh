#!/bin/bash
set -e
for db in "$OLTP_DB" airflow metastore vectors; do
  psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE $db"
done
psql -U "$POSTGRES_USER" -d vectors -c "CREATE EXTENSION IF NOT EXISTS vector"
psql -U "$POSTGRES_USER" -d "$OLTP_DB" -f /schema/01_schema.sql
