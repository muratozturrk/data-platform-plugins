"""Shared helpers: Trino DB-API connections per persona, query stats, EXPLAIN parsing, PostgreSQL access."""
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field

import psycopg
import trino

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
PG_DSN = os.getenv("PG_DSN", "postgresql://ops:ops@postgres:5432/ops")  # local dev default


def connect(user: str = "admin", source: str = "lakehouse-labs", client_tags: list[str] | None = None,
            session_properties: dict | None = None, catalog: str | None = None, schema: str | None = None):
    """One Trino connection per persona. The lab runs without authentication, so the user name is
    taken from the X-Trino-User header and authorization is enforced by file-based access control."""
    return trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user=user, source=source, client_tags=client_tags,
        session_properties=session_properties or {}, catalog=catalog, schema=schema,
        http_scheme="http",
    )


@dataclass
class QueryResult:
    rows: list[tuple]
    columns: list[str]
    query_id: str
    stats: dict = field(default_factory=dict)

    @property
    def processed_rows(self) -> int:
        """Rows read from all connectors (after pushdown and pruning at the source)."""
        return int(self.stats.get("processedRows", 0))

    @property
    def splits(self) -> int:
        return int(self.stats.get("totalSplits", 0))


def run(sql: str, user: str = "admin", **conn_kwargs) -> QueryResult:
    with connect(user=user, **conn_kwargs) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        return QueryResult(rows=rows, columns=cols, query_id=cur.query_id, stats=dict(cur.stats or {}))


def scalar(sql: str, user: str = "admin", **conn_kwargs):
    return run(sql, user=user, **conn_kwargs).rows[0][0]


def explain(sql: str, kind: str = "DISTRIBUTED", user: str = "admin", **conn_kwargs) -> str:
    return run(f"EXPLAIN (TYPE {kind}) {sql}", user=user, **conn_kwargs).rows[0][0]


def explain_analyze(sql: str, user: str = "admin", **conn_kwargs) -> str:
    return run(f"EXPLAIN ANALYZE {sql}", user=user, **conn_kwargs).rows[0][0]


_TABLESCAN_INPUT = re.compile(
    r"(?:TableScan|ScanFilter\w*|ScanProject\w*)\[table = (?P<table>[^\s,\]]+)[^\n]*\n(?:[^\n]*\n){0,12}?"
    r"[^\n]*?[\s│]Input: (?P<rows>[\d,]+) rows",
)


def scan_input_rows(analyze_text: str) -> dict[str, int]:
    """Rows each table scan actually received from its connector, from EXPLAIN ANALYZE output.
    Keys are the connector table handles' leading catalog:schema.table text."""
    out: dict[str, int] = {}
    for m in _TABLESCAN_INPUT.finditer(analyze_text):
        name = m.group("table")
        out[name] = out.get(name, 0) + int(m.group("rows").replace(",", ""))
    return out


# --- PostgreSQL (the operational source system) ---------------------------------------------

def pg_conn():
    return psycopg.connect(PG_DSN, autocommit=True)


def pg_execute(sql: str, params=None) -> None:
    with pg_conn() as conn:
        conn.execute(sql, params)


def pg_query(sql: str, params=None) -> list[tuple]:
    with pg_conn() as conn:
        return conn.execute(sql, params).fetchall()


def pg_table_reads(table: str) -> tuple[int, int]:
    """(sequential + index scans, tuples read) that PostgreSQL itself recorded for a table.
    This is the load a federated query puts on the source system."""
    with pg_conn() as conn:
        conn.execute("SELECT pg_stat_clear_snapshot()")
        row = conn.execute(
            """SELECT coalesce(seq_scan, 0) + coalesce(idx_scan, 0),
                      coalesce(seq_tup_read, 0) + coalesce(idx_tup_fetch, 0)
               FROM pg_stat_user_tables WHERE relname = %s""", (table,)).fetchone()
        return int(row[0]), int(row[1])


def wait_for_pg_stats(table: str, before: tuple[int, int], timeout_s: float = 5.0) -> tuple[int, int]:
    """PostgreSQL flushes per-backend counters asynchronously (at most about once per second)."""
    deadline = time.time() + timeout_s
    now = pg_table_reads(table)
    while now == before and time.time() < deadline:
        time.sleep(0.2)
        now = pg_table_reads(table)
    return now
