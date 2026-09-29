"use client";
import { useEffect, useState } from "react";

const cls = (s) => (["success", "ok", "pass"].includes(s) ? "ok" : ["failed", "stale", "fail", "error"].includes(s) ? "bad" : "muted");
const fmt = (t) => (t ? String(t).replace("T", " ").slice(0, 19) : "-");

export default function Health() {
  const [p, setP] = useState(null);
  const [d, setD] = useState(null);
  const [err, setErr] = useState(null);
  const load = () => {
    setErr(null);
    fetch("/api/health/pipeline").then((r) => (r.ok ? r.json() : r.text().then((t) => Promise.reject(t)))).then(setP).catch((e) => setErr(String(e)));
    fetch("/api/health/dq").then((r) => r.json()).then(setD).catch(() => {});
  };
  useEffect(load, []);
  return (
    <>
      <h1>Pipeline health</h1>
      <p className="sub">Read from the Airflow metadata database, object-store watermarks and live data-quality checks. <button className="ghost" onClick={load}>Refresh</button></p>
      {err && <div className="card bad">{err}</div>}
      {p && <>
        <h2>DAG runs</h2>
        <div className="grid">{p.dags.map((g) => (
          <div className="card" key={g.dag_id}>
            <b>{g.dag_id}</b>
            <p>Last run: <span className={cls(g.last_run?.state)}>{g.last_run?.state || "never"}</span> <span className="muted">{fmt(g.last_run?.end_date)}</span></p>
            <p className="muted">{Object.entries(g.run_counts).map(([k, v]) => `${k}: ${v}`).join(" · ") || "no runs"}</p>
            <div>{g.last_run_tasks.map((t) => <span key={t.task_id} className={"tag " + (t.state === "success" ? "" : "warn")} style={{ marginRight: 4 }}>{t.task_id}: {t.state}</span>)}</div>
          </div>))}</div>
        <h2>Freshness</h2>
        <div className="card wrap"><table><thead><tr><th>Table</th><th>Watermark</th><th>Source max updated_at</th><th>Lag vs now (h)</th><th>Status</th></tr></thead>
          <tbody>{p.freshness.map((f) => <tr key={f.table}><td>{f.table}</td><td>{fmt(f.watermark)}</td><td>{fmt(f.source_max_updated_at)}</td><td>{f.lag_hours_vs_now}</td><td className={cls(f.status)}>{f.status} (limit {f.limit_hours}h)</td></tr>)}</tbody></table>
          <p className="muted">{p.note}</p></div>
      </>}
      {d && <>
        <h2>Data-quality checks (live)</h2>
        <div className="card wrap"><table><thead><tr><th>Table</th><th>Check</th><th>Result</th><th>Message</th></tr></thead>
          <tbody>{d.results.map((r, i) => <tr key={i}><td>{r.table}</td><td>{r.check}</td><td className={cls(r.status)}>{r.status}</td><td className="muted">{r.message}</td></tr>)}</tbody></table></div>
      </>}
    </>
  );
}
