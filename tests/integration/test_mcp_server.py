"""Spawn the MCP server over stdio and drive it with the official MCP client."""
import asyncio
import json
import os
import sys

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.trino]
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


async def _session_calls():
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    params = StdioServerParameters(command=sys.executable, args=[os.path.join(ROOT, "ai", "mcp_server", "server.py")])
    out = {}
    async with stdio_client(params) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            out["tools"] = sorted(t.name for t in (await s.list_tools()).tools)
            async def call(name, args):
                res = await s.call_tool(name, args)
                return "".join(c.text for c in res.content if hasattr(c, "text"))
            out["list"] = await call("list_tables", {})
            out["describe_users"] = await call("describe_table", {"table": "lake.raw.users"})
            out["ok"] = await call("run_query", {"sql": "SELECT count(*) AS n FROM lake.raw.transactions WHERE dt = '2026-09-15' LIMIT 1"})
            out["drop"] = await call("run_query", {"sql": "DROP TABLE lake.raw.users"})
            out["pii"] = await call("run_query", {"sql": "SELECT phone FROM lake.raw.users LIMIT 3"})
            out["nofilter"] = await call("run_query", {"sql": "SELECT count(*) FROM lake.raw.transactions LIMIT 1"})
    return out


def test_mcp_tools_and_guard():
    o = asyncio.run(_session_calls())
    assert o["tools"] == ["describe_table", "list_tables", "run_query"]
    assert "lake.raw.transactions" in o["list"]
    assert "phone" not in o["describe_users"] and "email" not in o["describe_users"] and "full_name" not in o["describe_users"]
    assert json.loads(o["ok"])["rows"][0][0] > 0
    for k in ("drop", "pii", "nofilter"):
        assert "rejected by guard" in o[k], k
