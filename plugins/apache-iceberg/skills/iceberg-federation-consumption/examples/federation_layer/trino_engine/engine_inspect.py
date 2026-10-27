"""Inspect how Trino executes a federated query: catalogs (connector plugins), stages, splits,
join distribution and dynamic filtering. All information comes from Trino itself
(system tables, EXPLAIN, and the per-query statistics returned to the client)."""
from __future__ import annotations

import re
from dataclasses import dataclass

from common.federation import QueryResult, explain, explain_analyze, run, scan_input_rows


def catalogs() -> dict[str, str]:
    """Each data source is a catalog backed by a connector plugin (the SPI)."""
    rows = run("SELECT catalog_name, connector_name FROM system.metadata.catalogs").rows
    return dict(rows)


def stage_tree(stats: dict) -> list[dict]:
    """Flatten the stage tree the coordinator returns with every query."""
    out, todo = [], [stats["rootStage"]]
    while todo:
        s = todo.pop()
        out.append({"stage": s["stageId"], "splits": s["totalSplits"], "rows": s["processedRows"]})
        todo.extend(s.get("subStages", []))
    return sorted(out, key=lambda s: int(s["stage"]))


def join_distributions(sql: str, **kw) -> list[str]:
    return re.findall(r"Join\[.*?distribution = (\w+)", explain(sql, **kw))


def plan_fragments(sql: str, **kw) -> list[str]:
    return re.findall(r"^Fragment \d+ \[(\w+)\]", explain(sql, **kw), re.M)


@dataclass
class DynamicFilterRun:
    result: QueryResult
    iceberg_rows_read: int


def run_with_dynamic_filtering(sql: str, enabled: bool) -> DynamicFilterRun:
    props = {"enable_dynamic_filtering": str(enabled).lower()}
    res = run(sql, session_properties=props)
    scans = scan_input_rows(explain_analyze(sql, session_properties=props))
    iceberg = sum(v for k, v in scans.items() if k.startswith("lake:"))
    return DynamicFilterRun(res, iceberg)
