"""Which parts of a query does the federation engine push into the source system?

Reads Trino's distributed plan: a pushed operation shows up inside the connector's table handle
(`constraint on [...]`, `limit=`, `sortOrder=`, or a generated remote `Query[SELECT ...]`);
a non-pushed one shows up as a Trino operator above the scan (`filterPredicate = ...`).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from common.federation import explain, run

_SCAN = re.compile(r"(TableScan|ScanFilter\w*|ScanProject\w*)\[table = (?P<handle>pg:.*)$", re.M)


@dataclass
class PushdownReport:
    sql: str
    pushed: set[str] = field(default_factory=set)
    in_engine: set[str] = field(default_factory=set)
    rows_from_source: int = 0
    remote_sql: str | None = None


def analyze_pushdown(sql: str, **conn_kwargs) -> PushdownReport:
    plan = explain(sql, **conn_kwargs)
    report = PushdownReport(sql=sql)
    for m in _SCAN.finditer(plan):
        handle = m.group("handle")
        report.pushed.add("projection")                          # columns=[...] is always pruned
        if "constraint on [" in handle or "constraints=[" in handle:
            report.pushed.add("filter")
        if "limit=" in handle:
            report.pushed.add("topn" if "sortOrder=" in handle else "limit")
        q = re.search(r"Query\[(SELECT .*?)\] columns=", handle)
        if q:
            report.remote_sql = q.group(1)
            if "GROUP BY" in q.group(1) or "count(" in q.group(1):
                report.pushed.add("aggregation")
            if " JOIN " in q.group(1):
                report.pushed.add("join")
            if " WHERE " in q.group(1):
                report.pushed.add("filter")
        if "filterPredicate = " in handle:
            report.in_engine.add("filter")
    if re.search(r"Aggregate\[type = (PARTIAL|SINGLE|FINAL)", plan):
        report.in_engine.add("aggregation")
    if re.search(r"(InnerJoin|LeftJoin|RightJoin|CrossJoin)\[", plan):
        report.in_engine.add("join")
    report.rows_from_source = run(sql, **conn_kwargs).processed_rows
    return report
