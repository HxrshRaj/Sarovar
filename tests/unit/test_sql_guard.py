"""SQL safety validator: accepts good queries, rejects every category of bad one with a useful message."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ai"))

import pytest

from sarovar_ai import sql_guard as g


def errs(sql):
    return g.validate(sql).errors


GOOD = [
    "SELECT count(*) FROM lake.raw.transactions WHERE dt = '2026-09-15' LIMIT 1",
    "SELECT dt, gmv_rupees FROM lake.analytics.daily_gmv WHERE dt BETWEEN '2026-09-01' AND '2026-09-07' ORDER BY dt LIMIT 10",
    "SELECT m.category, count(*) n FROM lake.raw.transactions t JOIN lake.raw.merchants m ON m.merchant_id = t.merchant_id "
    "WHERE t.dt = '2026-09-14' GROUP BY m.category ORDER BY n DESC LIMIT 20",
    "WITH x AS (SELECT merchant_id, sum(txn_count) n FROM lake.curated.daily_merchant_metrics WHERE dt >= '2026-09-01' GROUP BY 1) SELECT * FROM x LIMIT 5",
    "SELECT city, count(*) FROM lake.raw.users GROUP BY city LIMIT 20",
    "SELECT dt FROM lake.raw.transactions WHERE dt IN ('2026-09-01', '2026-09-02') AND status = 'FAILED' LIMIT 10",
    "SELECT * FROM lake.raw.refunds WHERE dt = '2026-09-01' LIMIT 5",  # refunds has no PII columns
]


@pytest.mark.parametrize("sql", GOOD)
def test_good_queries_pass(sql):
    assert errs(sql) == []


def test_rejects_non_select():
    for sql in ["DROP TABLE lake.raw.users", "DELETE FROM lake.raw.transactions", "INSERT INTO lake.raw.users VALUES (1)",
                "CREATE TABLE x AS SELECT 1", "SHOW TABLES FROM lake.raw", "CALL lake.system.sync_partition_metadata('raw','users','FULL')",
                "EXPLAIN SELECT 1"]:
        assert any("SELECT" in e or "parse" in e for e in errs(sql)), sql


def test_rejects_multiple_statements():
    assert "exactly one" in errs("SELECT 1 LIMIT 1; DROP TABLE lake.raw.users")[0]


def test_requires_limit():
    e = errs("SELECT count(*) FROM lake.raw.transactions WHERE dt = '2026-09-15'")
    assert any("LIMIT" in x for x in e)


def test_limit_too_large():
    assert any("exceeds" in x for x in errs("SELECT dt FROM lake.raw.transactions WHERE dt='2026-09-15' LIMIT 100000"))


def test_requires_partition_filter_on_partitioned_fact():
    e = errs("SELECT count(*) FROM lake.raw.transactions LIMIT 1")
    assert any("partitioned" in x and "dt" in x for x in e)


def test_non_partition_filter_does_not_count():
    e = errs("SELECT count(*) FROM lake.raw.transactions WHERE created_at >= TIMESTAMP '2026-09-15 00:00:00' LIMIT 1")
    assert any("partitioned" in x for x in e)


def test_partition_filter_inside_or_does_not_count():
    e = errs("SELECT count(*) FROM lake.raw.transactions WHERE dt = '2026-09-15' OR status = 'FAILED' LIMIT 1")
    assert any("partitioned" in x for x in e)


def test_function_on_partition_col_does_not_count():
    e = errs("SELECT count(*) FROM lake.raw.transactions WHERE substr(dt, 1, 7) = '2026-09' LIMIT 1")
    assert any("partitioned" in x for x in e)


def test_partition_filter_required_in_each_query_block():
    sql = ("SELECT * FROM (SELECT status FROM lake.raw.transactions) t LIMIT 5")
    assert any("partitioned" in x for x in errs(sql))


def test_view_partition_filter_required():
    assert any("partitioned" in x for x in errs("SELECT * FROM lake.analytics.daily_gmv LIMIT 5"))
    assert errs("SELECT * FROM lake.analytics.merchant_leaderboard LIMIT 5") == []  # no partition column exposed


def test_pii_columns_rejected():
    for col in ("phone", "email", "vpa", "full_name"):
        assert any("PII" in x for x in errs(f"SELECT {col} FROM lake.raw.users LIMIT 5")), col
    assert any("PII" in x for x in errs("SELECT upi_ref FROM lake.raw.transactions WHERE dt='2026-09-15' LIMIT 5"))
    assert any("PII" in x for x in errs("SELECT city FROM lake.raw.users WHERE phone = 'x' LIMIT 5"))  # filtering on PII too


def test_select_star_rejected_when_table_has_pii():
    assert any("SELECT *" in x for x in errs("SELECT * FROM lake.raw.users LIMIT 5"))
    assert any("SELECT *" in x for x in errs("SELECT t.* FROM lake.raw.transactions t WHERE t.dt='2026-09-15' LIMIT 5"))
    assert errs("SELECT count(*) FROM lake.raw.users LIMIT 1") == []  # count(*) is fine


def test_unknown_or_unqualified_tables_rejected():
    assert any("allow-list" in x for x in errs("SELECT 1 FROM lake.raw.secrets LIMIT 1"))
    assert any("qualified" in x for x in errs("SELECT 1 FROM transactions LIMIT 1"))
    assert any("allow-list" in x for x in errs("SELECT 1 FROM lake.information_schema.columns LIMIT 1"))
    assert any("qualified" in x or "allow-list" in x for x in errs("SELECT 1 FROM system.runtime.queries LIMIT 1"))


def test_garbage_does_not_parse():
    assert errs("SELEC nonsense FROM")


class FakeTrino:
    def __init__(self, plan=None, exc=None):
        self.plan, self.exc = plan, exc

    def cursor(self):
        return self

    def execute(self, q):
        if self.exc:
            raise self.exc
        self.q = q

    def fetchall(self):
        import json
        return [[json.dumps(self.plan)]]


def plan(rows, ranges=1):
    return {"inputTableColumnInfos": [{"table": {"schemaTable": {"schema": "raw", "table": "transactions"}},
            "constraint": {"columnConstraints": [{"columnName": "dt", "domain": {"ranges": [{}] * ranges}}]},
            "estimate": {"outputRowCount": rows}}]}


def test_explain_gate_blocks_expensive_scan():
    with pytest.raises(g.GuardError) as e:
        g.explain_check("SELECT 1", FakeTrino(plan(300000, 59)), max_scan_rows=200000)
    assert "300,000" in str(e.value)


def test_explain_gate_passes_cheap_scan_and_reports_cost():
    c = g.explain_check("SELECT 1", FakeTrino(plan(5603)), max_scan_rows=200000)
    assert c["estimated_scan_rows"] == 5603 and c["scans"][0]["partition_ranges"] == 1


def test_explain_failure_is_a_guard_error():
    with pytest.raises(g.GuardError) as e:
        g.explain_check("SELECT nope", FakeTrino(exc=RuntimeError("Column 'nope' cannot be resolved")))
    assert "cannot be resolved" in str(e.value)


def test_explain_unknown_stats_fail_closed():
    with pytest.raises(g.GuardError) as e:
        g.explain_check("SELECT 1", FakeTrino(plan(float("nan"))))
    assert "ANALYZE" in str(e.value)


def test_check_does_not_explain_when_static_rules_fail():
    t = FakeTrino(plan(1))
    rep, cost = g.check("DROP TABLE lake.raw.users", t)
    assert not rep.ok and cost is None and not hasattr(t, "q")
