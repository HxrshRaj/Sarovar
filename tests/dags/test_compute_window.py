"""Regression test for a bug found in drill 1: manual triggers have no data interval and run_after is tz-aware."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

pytest.importorskip("airflow.sdk")

from sarovar_ingest import compute_window  # noqa: E402  (airflow/dags is on sys.path in the container)


def ctx(params=None, lo=None, hi=None, run_after=None):
    return {"params": params or {}, "data_interval_start": lo, "data_interval_end": hi,
            "dag_run": SimpleNamespace(run_after=run_after)}


def test_manual_trigger_without_interval_uses_last_24h():
    now = datetime(2026, 9, 29, 8, 41, tzinfo=timezone.utc)
    lo, hi = compute_window(ctx(run_after=now))
    assert hi == datetime(2026, 9, 29, 8, 41) and lo == hi - timedelta(days=1)


def test_params_override_window():
    lo, hi = compute_window(ctx({"window_start": "2026-09-20T00:00:00", "window_end": "2026-09-21T00:00:00"},
                                run_after=datetime(2026, 9, 29, tzinfo=timezone.utc)))
    assert (lo, hi) == (datetime(2026, 9, 20), datetime(2026, 9, 21))


def test_first_interval_is_initial_load():
    lo, hi = compute_window(ctx(lo=datetime(2026, 8, 1), hi=datetime(2026, 8, 2)))
    assert lo == datetime(1970, 1, 1) and hi == datetime(2026, 8, 2)
