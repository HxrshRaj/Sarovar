"use client";
import { useState } from "react";

const EXAMPLES = [
  "How many payments failed on 2026-09-05?",
  "List the top 5 merchants by total GMV in rupees, with merchant name and GMV.",
  "For each day from 2026-09-20 to 2026-09-24, show the date and the success rate percentage.",
  "How many users are there in each city?",
];

function Results({ columns, rows }) {
  if (!columns) return null;
  return (
    <div className="wrap">
      <table>
        <thead><tr>{columns.map((c) => <th key={c}>{c}</th>)}</tr></thead>
        <tbody>{rows.map((r, i) => <tr key={i}>{r.map((v, j) => <td key={j}>{String(v)}</td>)}</tr>)}</tbody>
      </table>
    </div>
  );
}

export default function Ask() {
  const [q, setQ] = useState(EXAMPLES[0]);
  const [busy, setBusy] = useState(false);
  const [res, setRes] = useState(null);
  const [err, setErr] = useState(null);

  async function submit(e) {
    e?.preventDefault();
    setBusy(true); setErr(null); setRes(null);
    try {
      const r = await fetch("/api/ask", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ question: q }) });
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      setRes(await r.json());
    } catch (x) { setErr(String(x)); } finally { setBusy(false); }
  }

  return (
    <>
      <h1>Ask a question</h1>
      <p className="sub">A local open-weight model writes read-only Trino SQL from table metadata only. Every query is validated (SELECT only, LIMIT, partition filter, no PII columns) and cost-checked with EXPLAIN before it runs. The first question can take 30–60 s on CPU.</p>
      <form onSubmit={submit} className="card">
        <textarea rows={2} value={q} onChange={(e) => setQ(e.target.value)} aria-label="Question" />
        <div className="row" style={{ marginTop: 10 }}>
          <button disabled={busy || q.length < 3}>{busy ? "Thinking…" : "Ask"}</button>
          <span className="muted">Personal data (phones, emails, VPAs, names) is redacted from the question before it reaches the model.</span>
        </div>
        <div className="examples">{EXAMPLES.map((x) => <button type="button" className="ghost" key={x} onClick={() => setQ(x)}>{x.length > 48 ? x.slice(0, 46) + "…" : x}</button>)}</div>
      </form>
      {err && <div className="card bad">Request failed: {err}</div>}
      {res && (
        <>
          {res.pii_redacted_from_question?.length > 0 && <div className="card"><span className="bad">Redacted from your question:</span> {res.pii_redacted_from_question.join(", ")}. The model saw: <code>{res.question}</code></div>}
          <div className="card">
            <h2 style={{ marginTop: 0 }}>1 · Retrieved tables</h2>
            <table><thead><tr><th>Table</th><th>Similarity</th></tr></thead>
              <tbody>{(res.retrieved || []).map((t) => <tr key={t.table}><td><code>lake.{t.table}</code></td><td>{t.score ?? "-"}</td></tr>)}</tbody></table>
          </div>
          <div className="card">
            <h2 style={{ marginTop: 0 }}>2 · Generated SQL</h2>
            <pre>{res.sql || "(no SQL produced)"}</pre>
            {res.guard && <p className={res.guard.ok ? "ok" : "bad"}>{res.guard.ok ? "Guard: passed" : "Guard: rejected — " + res.guard.errors.join("; ")}</p>}
            {res.attempts?.length > 1 && <p className="muted">{res.attempts.length} attempts (the first was rejected and the model was asked to repair it).</p>}
          </div>
          <div className="card">
            <h2 style={{ marginTop: 0 }}>3 · EXPLAIN cost estimate</h2>
            {res.cost ? (<>
              <p>Estimated rows scanned: <b>{res.cost.estimated_scan_rows.toLocaleString()}</b></p>
              <table><thead><tr><th>Scan</th><th>Est. rows</th><th>Partition ranges</th></tr></thead>
                <tbody>{res.cost.scans.map((s, i) => <tr key={i}><td><code>{s.table}</code></td><td>{s.estimated_rows.toLocaleString()}</td><td>{s.partition_ranges || "-"}</td></tr>)}</tbody></table>
            </>) : <p className="muted">No estimate (the query did not reach EXPLAIN).</p>}
          </div>
          <div className="card">
            <h2 style={{ marginTop: 0 }}>4 · Results {res.rows && <span className="muted">({res.rows.length} rows · {res.elapsed_s}s)</span>}</h2>
            {res.error && <p className="bad">{res.error}</p>}
            <Results columns={res.columns} rows={res.rows || []} />
            {!res.rows && !res.error && <p className="muted">Not executed.</p>}
            <p className="muted">Synthetic data · a small local model can be wrong: check the SQL.</p>
          </div>
        </>
      )}
    </>
  );
}
