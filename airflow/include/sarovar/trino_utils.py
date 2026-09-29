"""Trino helpers: DDL generated from docs/catalog.yml, partition registration, views."""
import re

import trino

from . import catalog
from .config import S3_BUCKET, TRINO_HOST, TRINO_PORT

SCHEMAS = {"raw": f"s3://{S3_BUCKET}/raw/", "curated": f"s3://{S3_BUCKET}/curated/", "analytics": None}


def connect(user="sarovar", catalog_name="lake", schema=None):
    return trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=user, catalog=catalog_name, schema=schema)


def run(sql, conn=None):
    conn = conn or connect()
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def table_ddl(schema, table, cat=None):
    cat = cat or catalog.load()
    if schema == "raw":
        spec = cat["source"][table]
        cols = [(c["name"], c["type"]) for c in spec["columns"]] + [("dt", "varchar")]
        part = "dt"
    else:
        spec = cat["curated"][table]
        cols = [(c["name"], c["type"]) for c in spec["columns"] if not c.get("partition")]
        cols += [(c["name"], c["type"]) for c in spec["columns"] if c.get("partition")]
        part = spec["partition_column"]
    body = ",\n  ".join(f'{n} {t}' for n, t in cols)
    return (f"CREATE TABLE IF NOT EXISTS lake.{schema}.{table} (\n  {body}\n) WITH (\n"
            f"  format = 'PARQUET',\n  partitioned_by = ARRAY['{part}'],\n"
            f"  external_location = 's3://{S3_BUCKET}/{schema}/{table}/'\n)")


def register(schema, table, conn=None):
    """Create the external table if needed and sync partitions from the object store layout."""
    conn = conn or connect()
    run(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}", conn)
    run(table_ddl(schema, table), conn)
    run(f"CALL lake.system.sync_partition_metadata('{schema}', '{table}', 'FULL')", conn)
    n = run(f'SELECT count(*) FROM lake.{schema}."{table}$partitions"', conn)[0][0]
    return n


def split_statements(sql_text):
    text = re.sub(r"--[^\n]*", "", sql_text)
    return [s.strip() for s in text.split(";") if s.strip()]
