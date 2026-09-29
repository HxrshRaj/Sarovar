"""Data discovery: embed table + column descriptions (PII columns excluded) into pgvector; retrieve candidate tables."""
import psycopg2

from . import metadata
from .config import VECTOR_DSN


def _vec(v):
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


def build_index(llm, dsn=VECTOR_DSN):
    docs = metadata.embedding_docs()
    embs = llm.embed([d[2] for d in docs])
    conn = psycopg2.connect(dsn)
    with conn, conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute("DROP TABLE IF EXISTS metadata_embeddings")
        cur.execute(f"CREATE TABLE metadata_embeddings (id serial PRIMARY KEY, table_key text NOT NULL, column_name text, "
                    f"content text NOT NULL, embedding vector({len(embs[0])}) NOT NULL)")
        for (tk, col, text), e in zip(docs, embs):
            cur.execute("INSERT INTO metadata_embeddings (table_key, column_name, content, embedding) VALUES (%s,%s,%s,%s::vector)",
                        (tk, col, text, _vec(e)))
    conn.close()
    return len(docs)


def retrieve(llm, question, k=3, dsn=VECTOR_DSN, n_docs=40):
    """Top-k tables for a (redacted) question. Score of a table = its best-matching doc (cosine similarity)."""
    q = llm.embed([question], prefix="search_query: ")[0]
    conn = psycopg2.connect(dsn)
    with conn.cursor() as cur:
        cur.execute("SELECT table_key, column_name, 1 - (embedding <=> %s::vector) AS sim FROM metadata_embeddings "
                    "ORDER BY embedding <=> %s::vector LIMIT %s", (_vec(q), _vec(q), n_docs))
        rows = cur.fetchall()
    conn.close()
    best = {}
    for tk, col, sim in rows:
        best[tk] = max(best.get(tk, 0), sim)
    ranked = sorted(best.items(), key=lambda x: -x[1])[:k]
    return [{"table": t, "score": round(float(s), 4)} for t, s in ranked]
