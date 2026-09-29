"use client";
import { useEffect, useState } from "react";

export default function Catalog() {
  const [q, setQ] = useState("");
  const [semantic, setSemantic] = useState(false);
  const [data, setData] = useState(null);
  const [err, setErr] = useState(null);
  useEffect(() => {
    const t = setTimeout(() => {
      fetch(`/api/catalog?q=${encodeURIComponent(q)}&semantic=${semantic}`)
        .then((r) => (r.ok ? r.json() : r.text().then((x) => Promise.reject(x))))
        .then((j) => { setData(j); setErr(null); })
        .catch((e) => setErr(String(e)));
    }, 250);
    return () => clearTimeout(t);
  }, [q, semantic]);
  return (
    <>
      <h1>Data catalog</h1>
      <p className="sub">Generated from <code>docs/catalog.yml</code>. Columns tagged <span className="pii">PII</span> are excluded from every LLM prompt and embedding and blocked by the SQL guard.</p>
      <div className="card row">
        <input type="text" placeholder="Search tables and columns (e.g. refund, partition, kyc)" value={q} onChange={(e) => setQ(e.target.value)} />
        <label><input type="checkbox" checked={semantic} onChange={(e) => setSemantic(e.target.checked)} /> semantic (embeddings)</label>
      </div>
      {err && <div className="card bad">{err}</div>}
      {data?.tables.map((t) => (
        <div className="card" key={t.table}>
          <div className="row">
            <b><code>{t.table}</code></b><span className="tag">{t.kind}</span><span className="tag">owner: {t.owner}</span>
            {t.partition_column && <span className="tag">partition: {t.partition_column}{t.partition_filter_required ? " (filter required)" : ""}</span>}
            {t.score != null && <span className="tag">similarity {t.score}</span>}
          </div>
          <p className="muted">{t.description}</p>
          <div className="wrap"><table><thead><tr><th>Column</th><th>Type</th><th>Description</th></tr></thead>
            <tbody>{t.columns.map((c) => <tr key={c.name}><td><code>{c.name}</code>{c.pii && <span className="pii" title={c.pii_class}>PII</span>}</td><td>{c.type}</td><td>{c.description}</td></tr>)}</tbody></table></div>
        </div>
      ))}
    </>
  );
}
