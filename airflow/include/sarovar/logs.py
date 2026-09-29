"""Structured (JSON-lines) logging. Every record carries dag_id, run_id, task_id."""
import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record):
        d = {"ts": datetime.now(timezone.utc).isoformat(), "level": record.levelname, "event": record.getMessage()}
        d.update(getattr(record, "ctx", {}))
        return json.dumps(d, default=str)


def get_logger(ctx=None, name="sarovar"):
    """ctx: dict with dag_id/run_id/task_id (+ fixed fields). Returns emit(event, level=, **fields)."""
    log = logging.getLogger(name)
    if not log.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(JsonFormatter())
        log.addHandler(h)
        log.setLevel(logging.INFO)
        log.propagate = False
    base = dict(ctx or {})

    def emit(event, level="INFO", **fields):
        log.log(getattr(logging, level), event, extra={"ctx": {**base, **fields}})
    return emit


def ctx_from_airflow(context, **extra):
    ti = context["ti"]
    return {"dag_id": ti.dag_id, "run_id": ti.run_id, "task_id": ti.task_id, **extra}
