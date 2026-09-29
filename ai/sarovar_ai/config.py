import os

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
LLM_MODEL = os.environ.get("LLM_MODEL", "llama3.2:3b")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "nomic-embed-text")
VECTOR_DSN = os.environ.get("VECTOR_DSN", "postgresql://sarovar:sarovar_dev_pw@localhost:5433/vectors")
MAX_LIMIT = int(os.environ.get("MAX_LIMIT", "1000"))
# EXPLAIN cost gate: estimated rows scanned across all table scans (needs ANALYZE stats)
MAX_SCAN_ROWS = int(os.environ.get("MAX_SCAN_ROWS", "200000"))
AUDIT_LOG = os.environ.get("AUDIT_LOG", "ai/audit.log.jsonl")
DATA_START, DATA_END = "2026-08-01", "2026-09-28"  # metadata about coverage, not data values
