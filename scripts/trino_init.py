"""Create schemas, register raw + curated external tables (partition sync), apply analyst views. Idempotent."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "include"))
from sarovar import catalog, trino_utils  # noqa: E402


def main(views_only=False):
    cat = catalog.load()
    conn = trino_utils.connect()
    for s in ("raw", "curated", "analytics"):
        trino_utils.run(f"CREATE SCHEMA IF NOT EXISTS lake.{s}", conn)
    if not views_only:
        for t in cat["source"]:
            print("raw", t, "partitions:", trino_utils.register("raw", t, conn))
        for t in cat["curated"]:
            print("curated", t, "partitions:", trino_utils.register("curated", t, conn))
    sql = open(os.path.join(os.path.dirname(__file__), "..", "trino", "views.sql"), encoding="utf-8").read()
    for stmt in trino_utils.split_statements(sql):
        trino_utils.run(stmt, conn)
    print("views applied:", [r[0] for r in trino_utils.run("SHOW TABLES FROM lake.analytics", conn)])


if __name__ == "__main__":
    main("--views-only" in sys.argv)
