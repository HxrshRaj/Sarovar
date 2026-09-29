"""The curated-vs-source comparison must fail loudly on any difference."""
import pandas as pd
import pytest

from sarovar.dq import DataQualityError
from sarovar.validate import _compare

KEYS = ["dt", "merchant_id"]


def frame(v):
    return pd.DataFrame({"dt": ["2026-08-01", "2026-08-01"], "merchant_id": [1, 2], "txn_count": [10, 5], "success_amount_paise": v})


def test_identical_passes():
    assert _compare("t", frame([100, 200]), frame([100, 200]), KEYS) == 2


def test_value_diff_fails_with_column_name():
    with pytest.raises(DataQualityError) as e:
        _compare("t", frame([100, 200]), frame([100, 201]), KEYS)
    assert "success_amount_paise" in str(e.value) and "differs on 1 row" in str(e.value)


def test_missing_row_in_lake_fails():
    with pytest.raises(DataQualityError) as e:
        _compare("t", frame([100, 200]), frame([100, 200]).iloc[:1], KEYS)
    assert "only in source SQL" in str(e.value)


def test_extra_row_in_lake_fails():
    with pytest.raises(DataQualityError) as e:
        _compare("t", frame([100, 200]).iloc[:1], frame([100, 200]), KEYS)
    assert "only in curated table" in str(e.value)
