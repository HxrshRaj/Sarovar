"""Data-quality checks: inject bad data, assert the right check fails with a useful message."""
from datetime import datetime

import pyarrow as pa
import pytest

from sarovar import dq
from sarovar.dq import DataQualityError


def tbl(ids, created=None):
    created = created or [datetime(2026, 8, 1)] * len(ids)
    return pa.table({"txn_id": pa.array(ids, pa.int64()), "created_at": pa.array(created, pa.timestamp("ms"))})


def test_row_count_mismatch_message():
    with pytest.raises(DataQualityError) as e:
        dq.check_row_count("transactions", "2026-08-01", 100, 98)
    assert e.value.check == "row_count"
    msg = str(e.value)
    assert "transactions" in msg and "dt=2026-08-01" in msg and "source has 100" in msg and "has 98" in msg


def test_row_count_ok():
    dq.check_row_count("transactions", "2026-08-01", 5, 5)


def test_null_key_detected():
    with pytest.raises(DataQualityError) as e:
        dq.check_not_null(tbl([1, None, 3]), "transactions", "2026-08-01", ["txn_id"])
    assert e.value.check == "not_null" and "'txn_id'" in str(e.value) and "1 NULL" in str(e.value)


def test_duplicate_key_detected():
    with pytest.raises(DataQualityError) as e:
        dq.check_unique(tbl([1, 2, 2, 2]), "transactions", "2026-08-01", "txn_id")
    assert e.value.check == "unique_key" and "2 duplicate" in str(e.value)


def test_clean_table_passes():
    t = tbl([1, 2, 3])
    dq.check_not_null(t, "transactions", "2026-08-01", ["txn_id", "created_at"])
    dq.check_unique(t, "transactions", "2026-08-01", "txn_id")


def test_missing_partition_detected():
    with pytest.raises(DataQualityError) as e:
        dq.check_partition_exists(False, "transactions", "2026-08-09")
    assert e.value.check == "partition_exists" and "dt=2026-08-09" in str(e.value)


def test_freshness_breach_and_ok():
    now = datetime(2026, 9, 29, 12, 0)
    dq.check_freshness("transactions", datetime(2026, 9, 29, 0, 0), now, 26)
    with pytest.raises(DataQualityError) as e:
        dq.check_freshness("transactions", datetime(2026, 9, 27, 0, 0), now, 26)
    assert e.value.check == "freshness" and "limit 26h" in str(e.value)


def test_freshness_source_ahead_of_watermark():
    now = datetime(2026, 9, 29, 12, 0)
    with pytest.raises(DataQualityError) as e:
        dq.check_freshness("transactions", datetime(2026, 9, 29, 0, 0), now, 26,
                           source_max_updated=datetime(2026, 10, 2, 0, 0))
    assert "past the loaded watermark" in str(e.value)


def test_never_loaded_table():
    with pytest.raises(DataQualityError) as e:
        dq.check_freshness("refunds", None, datetime(2026, 9, 29), 26)
    assert "never been loaded" in str(e.value)


def test_schema_drift_detected():
    exp = {"txn_id": "int", "status": "str"}
    dq.check_schema("transactions", dict(exp), exp)
    with pytest.raises(DataQualityError) as e:
        dq.check_schema("transactions", {"txn_id": "int", "status": "str", "device_id": "str"}, exp)
    assert "added=['device_id']" in str(e.value)
    with pytest.raises(DataQualityError) as e:
        dq.check_schema("transactions", {"txn_id": "str", "status": "str"}, exp)
    assert "type_changed=['txn_id']" in str(e.value)
