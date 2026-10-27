"""Measure the consumption requirements instead of assuming them.

- performance: what the engine physically reads (Trino query stats, PyIceberg scan planning)
- freshness:   age of the snapshot a dashboard reads
- concurrency: parallel dashboard sessions on the SQL endpoint
- cost:        bytes scanned per query, the unit most engines bill or size clusters by

Trino query stats come from the client protocol (`cursor.stats`, the same StatementStats the
JDBC driver exposes): https://trino.io/docs/current/develop/client-protocol.html
"""
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import quantiles

from pyiceberg.expressions import AlwaysTrue

from common.lakehouse import trino_connect


@dataclass(frozen=True)
class QueryStats:
    rows: list[tuple]
    processed_rows: int
    physical_input_bytes: int
    completed_splits: int
    elapsed_ms: int


def trino_stats(sql: str, user: str = "admin", session: dict[str, str] | None = None) -> QueryStats:
    with trino_connect(user, session_properties=session) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = [tuple(r) for r in cur.fetchall()]
        s = cur.stats
    return QueryStats(rows, s["processedRows"], s["physicalInputBytes"], s["completedSplits"], s["elapsedTimeMillis"])


@dataclass(frozen=True)
class ScanPlan:
    files: int
    file_bytes: int        # whole data files that survive pruning
    column_bytes: int      # only the column chunks of the selected columns


def plan_scan(catalog, table: str, row_filter=AlwaysTrue(), columns: tuple[str, ...] = ("*",)) -> ScanPlan:
    """What a client-side engine (PyIceberg, DuckDB via PyIceberg) would read, from metadata only."""
    t = catalog.load_table(table)
    names = [f.name for f in t.schema().fields] if columns == ("*",) else list(columns)
    ids = {t.schema().find_field(n).field_id for n in names}
    tasks = list(t.scan(row_filter=row_filter, selected_fields=tuple(names)).plan_files())
    return ScanPlan(
        files=len(tasks),
        file_bytes=sum(task.file.file_size_in_bytes for task in tasks),
        column_bytes=sum(size for task in tasks for fid, size in task.file.column_sizes.items() if fid in ids),
    )


ROLLUP_SQL = """
CREATE OR REPLACE TABLE {dst} USING iceberg AS
SELECT CAST(order_ts AS DATE) AS order_date, region, category, merchant_id,
       sum(CASE WHEN status = 'completed' THEN quantity * unit_price * (1 - discount) END) AS net_revenue,
       count(CASE WHEN status = 'completed' THEN 1 END) AS completed_orders
FROM {src} GROUP BY 1, 2, 3, 4
"""


def build_rollup(spark, src: str, dst: str) -> int:
    """Pre-aggregated gold table for dashboards (the job a materialized view or reflection would do)."""
    spark.sql(ROLLUP_SQL.format(src=src, dst=dst))
    return spark.table(dst).count()


def freshness_seconds(catalog, table: str, now: datetime | None = None) -> float:
    """Seconds between `now` and the commit time of the snapshot consumers currently read."""
    snap = catalog.load_table(table).current_snapshot()
    now = now or datetime.now(timezone.utc)
    return now.timestamp() - snap.timestamp_ms / 1000


def concurrent_sessions(sql: str, users: list[str]) -> tuple[list[float], list[list[tuple]]]:
    """Run the same dashboard query from N sessions at once; return latencies (s) and results."""
    def one(user):
        start = time.perf_counter()
        rows = trino_stats(sql, user).rows
        return time.perf_counter() - start, rows
    with ThreadPoolExecutor(max_workers=len(users)) as pool:
        out = list(pool.map(one, users))
    return [o[0] for o in out], [o[1] for o in out]


def p95(values: list[float]) -> float:
    return quantiles(values, n=20, method="inclusive")[-1] if len(values) > 1 else values[0]


@dataclass(frozen=True)
class Requirement:
    category: str          # performance, accessibility, governance, semantics, cost
    check: str
    measured: float
    threshold: float       # assumption: set it from your platform audit
    higher_is_better: bool = False

    @property
    def ok(self) -> bool:
        return self.measured >= self.threshold if self.higher_is_better else self.measured <= self.threshold


def consumption_report(reqs: list[Requirement]) -> list[str]:
    return [f"{'PASS' if r.ok else 'FAIL'} {r.category}: {r.check} = {r.measured:.3g} (limit {r.threshold:.3g})"
            for r in reqs]
