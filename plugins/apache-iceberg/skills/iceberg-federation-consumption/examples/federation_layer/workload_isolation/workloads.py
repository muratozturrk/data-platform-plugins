"""Workload isolation on a self-managed Trino cluster with resource groups (trino/resource-groups.json).

Dremio engines and Trino Gateway routing serve the same goal: keep a noisy
workload (ad hoc exploration) from starving another (dashboards, ETL). Inside one Trino cluster,
resource groups do this with selectors (user, source, client tags) and per-group limits.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from common.federation import connect, run

SLOW_SOURCE_QUERY = ("SELECT * FROM TABLE(pg.system.query(query => "
                     "'SELECT 1 AS x FROM pg_sleep({seconds})'))")


@dataclass
class Submitted:
    source: str
    thread: threading.Thread | None = None
    rows: list | None = None
    error: Exception | None = None
    stats: dict = field(default_factory=dict)
    started_at: float = 0.0
    finished_at: float = 0.0

    @property
    def elapsed_s(self) -> float:
        return self.finished_at - self.started_at


def submit(sql: str, source: str, user: str = "admin", client_tags: list[str] | None = None) -> Submitted:
    """Run a query in a background thread (like an independent client)."""
    job = Submitted(source=source)

    def _work():
        job.started_at = time.time()
        try:
            with connect(user=user, source=source, client_tags=client_tags) as conn:
                cur = conn.cursor()
                cur.execute(sql)
                job.rows = cur.fetchall()
                job.stats = dict(cur.stats or {})
        except Exception as exc:                     # noqa: BLE001 - recorded for the test
            job.error = exc
        finally:
            job.finished_at = time.time()

    job.thread = threading.Thread(target=_work, daemon=True)
    job.thread.start()
    return job


def query_info(source: str) -> list[dict]:
    """State and resource group of every query a client (identified by source) submitted."""
    res = run("SELECT query_id, state, resource_group_id, \"user\", source, error_code "
              f"FROM system.runtime.queries WHERE source = '{source}' ORDER BY created")
    return [dict(zip(res.columns, r)) for r in res.rows]


def wait_for_state(source: str, states: set[str], timeout_s: float = 20.0) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        info = query_info(source)
        if info and info[-1]["state"] in states:
            return info[-1]
        time.sleep(0.2)
    raise TimeoutError(f"{source} never reached {states}: {query_info(source)}")
