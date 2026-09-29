"""SQL safety validator. Rules (all enforced before anything runs):
  1. exactly one statement and it is a SELECT (CTEs / unions allowed) - no DDL, DML, EXPLAIN, SHOW, CALL ...
  2. only allow-listed tables (lake.raw|curated|analytics.<catalogued table>), schema-qualified
  3. a LIMIT no larger than MAX_LIMIT on the outermost query
  4. a literal predicate on the partition column for every table/view flagged `partition_filter_required`
  5. no PII column referenced; `SELECT *` rejected when a referenced table has PII columns
  6. EXPLAIN check against Trino: query must plan, and estimated rows scanned must stay under MAX_SCAN_ROWS
"""
import json
import math
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

from . import metadata
from .config import MAX_LIMIT, MAX_SCAN_ROWS

FORBIDDEN = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Alter, exp.Command, exp.Merge,
             exp.Use, exp.Set, exp.Copy, exp.Grant, exp.TruncateTable, exp.Describe, exp.Show, exp.Transaction,
             exp.Commit, exp.Rollback)
CMP = (exp.EQ, exp.GT, exp.GTE, exp.LT, exp.LTE)


class GuardError(Exception):
    def __init__(self, errors):
        self.errors = errors if isinstance(errors, list) else [errors]
        super().__init__("; ".join(self.errors))


@dataclass
class GuardReport:
    sql: str
    tables: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    @property
    def ok(self):
        return not self.errors


def _is_literal(node):
    if isinstance(node, (exp.Literal, exp.Null)):
        return True
    if isinstance(node, (exp.Cast, exp.TryCast)):
        return _is_literal(node.this)
    if isinstance(node, exp.Paren):
        return _is_literal(node.this)
    if isinstance(node, exp.Neg):
        return _is_literal(node.this)
    return False


def _col_matches(col, part_col, names):
    return isinstance(col, exp.Column) and col.name.lower() == part_col and (not col.table or col.table.lower() in names)


def _has_partition_predicate(select, part_col, names):
    """True if the select's WHERE / JOIN..ON contains <partition col> <cmp|BETWEEN|IN> <literal(s)>."""
    conds = []
    if select.args.get("where"):
        conds.append(select.args["where"].this)
    for j in select.args.get("joins") or []:
        if j.args.get("on"):
            conds.append(j.args["on"])
    for cond in conds:
        # only conjunctive top-level predicates count (an OR could bypass pruning)
        stack = [cond]
        while stack:
            n = stack.pop()
            if isinstance(n, exp.Paren):
                stack.append(n.this)
            elif isinstance(n, exp.And):
                stack += [n.left, n.right]
            elif isinstance(n, CMP):
                l, r = n.left, n.right
                if (_col_matches(l, part_col, names) and _is_literal(r)) or (_col_matches(r, part_col, names) and _is_literal(l)):
                    return True
            elif isinstance(n, exp.Between):
                if _col_matches(n.this, part_col, names) and _is_literal(n.args["low"]) and _is_literal(n.args["high"]):
                    return True
            elif isinstance(n, exp.In):
                if _col_matches(n.this, part_col, names) and n.expressions and all(_is_literal(e) for e in n.expressions):
                    return True
    return False


def validate(sql, cat_tables=None, max_limit=MAX_LIMIT):
    cat_tables = cat_tables or metadata.all_tables()
    rep = GuardReport(sql=sql.strip().rstrip(";"))
    try:
        stmts = [s for s in sqlglot.parse(sql, read="trino") if s is not None]
    except sqlglot.errors.ParseError as e:
        rep.errors.append(f"SQL does not parse: {str(e)[:160]}")
        return rep
    if len(stmts) != 1:
        rep.errors.append("exactly one SQL statement is allowed")
        return rep
    stmt = stmts[0]
    if not isinstance(stmt, (exp.Select, exp.Union, exp.Subquery)) or stmt.find(*FORBIDDEN):
        rep.errors.append("only read-only SELECT statements are allowed")
        return rep

    # --- tables
    cte_names = {c.alias.lower() for c in stmt.find_all(exp.CTE)}
    refs = {}  # id(table node) -> key
    for t in stmt.find_all(exp.Table):
        if not t.db and t.name.lower() in cte_names:
            continue
        cat = (t.catalog or "lake").lower()
        key = f"{t.db.lower()}.{t.name.lower()}" if t.db else None
        if cat != "lake" or key is None:
            rep.errors.append(f"table '{t.sql()}' must be qualified as lake.<schema>.<table>")
        elif key not in cat_tables:
            rep.errors.append(f"table 'lake.{key}' is not in the allow-list (allowed: {sorted('lake.' + k for k in cat_tables)})")
        else:
            refs[id(t)] = key
    rep.tables = sorted(set(refs.values()))
    if rep.errors:
        return rep

    # --- LIMIT on the outermost query
    top = stmt.this if isinstance(stmt, exp.Subquery) else stmt
    lim = top.args.get("limit")
    if lim is None or lim.expression is None or not isinstance(lim.expression, exp.Literal) or not lim.expression.is_int:
        rep.errors.append(f"a literal LIMIT (<= {max_limit}) is required")
    elif int(lim.expression.name) > max_limit:
        rep.errors.append(f"LIMIT {lim.expression.name} exceeds the maximum of {max_limit}")

    # --- PII
    pii_cols = {c["name"].lower() for k in rep.tables for c in cat_tables[k]["columns"] if c.get("pii")}
    for col in stmt.find_all(exp.Column):
        if col.name.lower() in pii_cols:
            rep.errors.append(f"column '{col.name}' is PII and may not be selected or filtered")
    tables_with_pii = [k for k in rep.tables if any(c.get("pii") for c in cat_tables[k]["columns"])]
    for star in stmt.find_all(exp.Star):
        if isinstance(star.parent, exp.Count):
            continue
        if tables_with_pii:
            rep.errors.append(f"SELECT * is not allowed because {tables_with_pii} contain PII columns; list columns explicitly")
            break

    # --- partition filter, per SELECT scope
    for sel in stmt.find_all(exp.Select):
        scope_tables = []
        frm = sel.args.get("from_") or sel.args.get("from")
        if frm is not None and isinstance(frm.this, exp.Table):
            scope_tables.append(frm.this)
        for j in sel.args.get("joins") or []:
            if isinstance(j.this, exp.Table):
                scope_tables.append(j.this)
        for t in scope_tables:
            key = refs.get(id(t))
            spec = cat_tables.get(key) if key else None
            if spec and spec.get("partition_filter_required") and spec.get("partition_col"):
                names = {n.lower() for n in (t.alias, t.name) if n}
                if not _has_partition_predicate(sel, spec["partition_col"], names):
                    rep.errors.append(f"'lake.{key}' is partitioned: add a literal filter on {spec['partition_col']} "
                                      f"(e.g. {spec['partition_col']} = '2026-09-15' or BETWEEN two dates) in the same query block")
    return rep


def explain_check(sql, trino_conn, max_scan_rows=MAX_SCAN_ROWS):
    """Run EXPLAIN (TYPE IO) on Trino: the query must plan and its estimated scan must stay under the gate."""
    cur = trino_conn.cursor()
    try:
        cur.execute("EXPLAIN (TYPE IO, FORMAT JSON) " + sql.strip().rstrip(";"))
        plan = json.loads(cur.fetchall()[0][0])
    except Exception as e:
        raise GuardError(f"EXPLAIN failed: {str(getattr(e, 'message', e))[:300]}")
    scans, total = [], 0.0
    for t in plan.get("inputTableColumnInfos", []):
        st = t["table"]["schemaTable"]
        rows = t["estimate"].get("outputRowCount")
        if rows is None or (isinstance(rows, float) and math.isnan(rows)) or isinstance(rows, str):
            raise GuardError(f"no statistics for {st['schema']}.{st['table']}: run ANALYZE so scan cost can be estimated")
        ranges = 0
        for cc in (t.get("constraint", {}).get("columnConstraints") or []):
            if cc["columnName"] in ("dt", "cohort_week"):
                ranges = len(cc["domain"].get("ranges", []))
        scans.append({"table": f"{st['schema']}.{st['table']}", "estimated_rows": int(rows), "partition_ranges": ranges})
        total += rows
    if total > max_scan_rows:
        raise GuardError(f"estimated scan of {int(total):,} rows exceeds the limit of {max_scan_rows:,}; narrow the partition filter")
    return {"estimated_scan_rows": int(total), "scans": scans}


def check(sql, trino_conn=None, **kw):
    """Full pipeline: static rules, then EXPLAIN. Returns (GuardReport, cost|None). Never executes the query."""
    rep = validate(sql)
    cost = None
    if rep.ok and trino_conn is not None:
        try:
            cost = explain_check(sql, trino_conn, **kw)
        except GuardError as e:
            rep.errors += e.errors
    return rep, cost
