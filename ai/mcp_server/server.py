"""Read-only Trino MCP server (stdio). Exposes the lakehouse to an MCP client (e.g. an IDE agent) safely:
  list_tables / describe_table  - catalog metadata only, PII columns removed
  run_query                     - SQL guard (SELECT only, LIMIT, partition filter, no PII, EXPLAIN cost gate) then execute

It is deliberately NOT a general Trino proxy. Configure a client with:
  {"mcpServers": {"sarovar-trino": {"command": "python", "args": ["ai/mcp_server/server.py"]}}}
Env: TRINO_HOST / TRINO_PORT (default localhost:8080)."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import sarovar_ai  # noqa: E402,F401
from mcp.server.mcpserver import MCPServer  # noqa: E402

from sarovar import trino_utils  # noqa: E402
from sarovar_ai import metadata, sql_guard  # noqa: E402
from sarovar_ai.config import MAX_LIMIT  # noqa: E402

mcp = MCPServer("sarovar-trino", instructions=(
    "Read-only access to the Sarovar lakehouse (SYNTHETIC data). Use list_tables, describe_table, then run_query. "
    "Queries must be a single SELECT with a LIMIT and a literal partition filter (dt) on partitioned fact tables; "
    "personal-data columns are not available."))


@mcp.tool()
def list_tables() -> str:
    """List queryable tables/views with one-line descriptions (no PII columns are ever listed)."""
    return json.dumps([{"table": f"lake.{k}", "kind": v["kind"], "partition_column": v.get("partition_col"),
                        "partition_filter_required": bool(v.get("partition_filter_required")), "description": v["description"]}
                       for k, v in metadata.all_tables().items()], indent=1)


@mcp.tool()
def describe_table(table: str) -> str:
    """Describe one table (e.g. 'lake.raw.transactions'): columns, types, descriptions. PII columns are omitted."""
    key = table.lower().removeprefix("lake.")
    spec = metadata.all_tables().get(key)
    if not spec:
        return f"unknown table '{table}'. Call list_tables."
    return metadata.table_doc(key, spec)


@mcp.tool()
def run_query(sql: str) -> str:
    """Run a guarded read-only Trino query. Returns JSON {columns, rows, estimated_scan_rows} or {error}."""
    conn = trino_utils.connect(user="mcp-readonly")
    rep, cost = sql_guard.check(sql, conn)
    if not rep.ok:
        return json.dumps({"error": "query rejected by guard", "reasons": rep.errors})
    cur = conn.cursor()
    cur.execute(rep.sql)
    rows = cur.fetchmany(MAX_LIMIT)
    return json.dumps({"columns": [d[0] for d in cur.description], "rows": rows, "estimated_scan_rows": cost["estimated_scan_rows"]}, default=str)


if __name__ == "__main__":
    mcp.run()
