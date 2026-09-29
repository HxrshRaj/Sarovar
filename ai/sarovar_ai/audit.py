"""Audit log of every question. Text is re-redacted before it is written, so logs cannot leak PII either."""
import json
import os
import time

from .config import AUDIT_LOG
from .redact import Redactor


class AuditLog:
    def __init__(self, path=AUDIT_LOG, redactor=None):
        self.path, self.redactor = path, redactor or Redactor()
        self.records = []  # in-memory copy (tests inspect it)

    def write(self, **rec):
        clean = json.loads(self.redactor.redact(json.dumps(rec, default=str))[0])
        clean["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        self.records.append(clean)
        if self.path:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(clean) + "\n")
        return clean
