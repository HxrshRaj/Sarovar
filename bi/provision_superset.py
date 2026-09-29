"""Provision Superset via its REST API: Trino connection, datasets on analytics views, charts, one dashboard.
Idempotent (looks things up by name). Local dev credentials only (admin/admin from docker-compose).
Usage: python bi/provision_superset.py [http://localhost:8088]"""
import json
import sys

import requests

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8088"
s = requests.Session()
tok = s.post(f"{BASE}/api/v1/security/login", json={"username": "admin", "password": "admin", "provider": "db", "refresh": True}).json()["access_token"]
s.headers["Authorization"] = f"Bearer {tok}"


def find(kind, col, val):
    q = json.dumps({"filters": [{"col": col, "opr": "eq", "value": val}]})
    r = s.get(f"{BASE}/api/v1/{kind}/", params={"q": q}).json()
    return r["result"][0]["id"] if r.get("count") else None


def ensure(kind, col, val, payload):
    i = find(kind, col, val)
    if i:
        return i
    r = s.post(f"{BASE}/api/v1/{kind}/", json=payload)
    if r.status_code >= 300:
        raise SystemExit(f"{kind} create failed: {r.status_code} {r.text}")
    return r.json()["id"]


db = ensure("database", "database_name", "Sarovar Trino (lake, synthetic data)",
            {"database_name": "Sarovar Trino (lake, synthetic data)", "sqlalchemy_uri": "trino://sarovar@trino:8080/lake",
             "expose_in_sqllab": True})
ds = {}
for v in ("daily_gmv", "merchant_leaderboard", "failure_reasons", "retention_weekly"):
    r = s.get(f"{BASE}/api/v1/dataset/", params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": v}]})}).json()
    if r.get("count"):
        ds[v] = r["result"][0]["id"]
    else:
        rr = s.post(f"{BASE}/api/v1/dataset/", json={"database": db, "schema": "analytics", "table_name": v})
        if rr.status_code >= 300:
            raise SystemExit(f"dataset {v}: {rr.status_code} {rr.text}")
        ds[v] = rr.json()["id"]


def sql_metric(expr, label):
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label}


DT_FILTER = [{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": "dt >= '2026-09-01'"}]
CHARTS = [
    ("Total GMV since 2026-09-01 (INR)", "big_number_total", "daily_gmv",
     {"viz_type": "big_number_total", "metric": sql_metric("SUM(gmv_rupees)", "GMV (INR)"), "adhoc_filters": DT_FILTER, "y_axis_format": ",.0f"}),
    ("Daily GMV (INR)", "echarts_timeseries_bar", "daily_gmv",
     {"viz_type": "echarts_timeseries_bar", "x_axis": "dt", "metrics": [sql_metric("SUM(gmv_rupees)", "GMV (INR)")],
      "groupby": [], "adhoc_filters": DT_FILTER, "row_limit": 100, "orientation": "vertical"}),
    ("Daily success rate (%)", "echarts_timeseries_line", "daily_gmv",
     {"viz_type": "echarts_timeseries_line", "x_axis": "dt", "metrics": [sql_metric("AVG(success_rate_pct)", "Success rate %")],
      "groupby": [], "adhoc_filters": DT_FILTER, "row_limit": 100, "y_axis_bounds": [90, 100]}),
    ("Top 10 merchants by GMV", "table", "merchant_leaderboard",
     {"viz_type": "table", "query_mode": "raw", "all_columns": ["merchant_name", "category", "txn_count", "gmv_rupees", "failure_rate_pct", "refund_rate_pct"],
      "order_by_cols": ["[\"gmv_rupees\", false]"], "row_limit": 10, "adhoc_filters": []}),
    ("Failure codes since 2026-09-01", "pie", "failure_reasons",
     {"viz_type": "pie", "groupby": ["failure_code"], "metric": sql_metric("SUM(failed_count)", "Failed payments"),
      "adhoc_filters": DT_FILTER, "row_limit": 10, "donut": True}),
    ("Weekly retention by signup cohort (%)", "heatmap_v2", "retention_weekly",
     {"viz_type": "heatmap_v2", "x_axis": "weeks_since_signup", "groupby": "cohort_week",
      "metric": sql_metric("MAX(retention_pct)", "Retention %"), "adhoc_filters": [], "row_limit": 500, "show_values": True}),
]
chart_ids = []
for name, viz, dsname, params in CHARTS:
    cid = ensure("chart", "slice_name", name, {"slice_name": name, "viz_type": viz, "datasource_id": ds[dsname],
                                               "datasource_type": "table", "params": json.dumps(params)})
    chart_ids.append((cid, name))

TITLE = "Sarovar analytics (synthetic data, local model)"
rows = [chart_ids[0:3], chart_ids[3:6]]
pos = {"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
       "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
       "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": TITLE}}}
for ri, row in enumerate(rows):
    rid = f"ROW-{ri}"
    pos["GRID_ID"]["children"].append(rid)
    pos[rid] = {"type": "ROW", "id": rid, "children": [], "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
    for cid, name in row:
        key = f"CHART-{cid}"
        pos[rid]["children"].append(key)
        pos[key] = {"type": "CHART", "id": key, "children": [], "parents": ["ROOT_ID", "GRID_ID", rid],
                    "meta": {"width": 4, "height": 50, "chartId": cid, "sliceName": name}}
dash = find("dashboard", "dashboard_title", TITLE)
if not dash:
    r = s.post(f"{BASE}/api/v1/dashboard/", json={"dashboard_title": TITLE, "published": True, "position_json": json.dumps(pos), "json_metadata": "{}"})
    if r.status_code >= 300:
        raise SystemExit(f"dashboard create failed: {r.status_code} {r.text}")
    dash = r.json()["id"]
for cid, _ in chart_ids:
    s.put(f"{BASE}/api/v1/chart/{cid}", json={"dashboards": [dash]})
print(f"database={db} datasets={ds} charts={[c for c,_ in chart_ids]} dashboard={dash}")
print(f"open {BASE}/superset/dashboard/{dash}/  (admin / admin, local dev only)")
