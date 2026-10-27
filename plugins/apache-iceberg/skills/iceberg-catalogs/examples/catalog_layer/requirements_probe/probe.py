"""Measure a candidate catalog against the requirement questions instead of trusting a datasheet.

- latency of the call on every query's critical path (LoadTable)
- correctness and throughput of concurrent commits (optimistic concurrency under contention)
- capabilities: auth required, credential vending, multi-table transactions, views, register
"""
from __future__ import annotations

import statistics
import threading
import time
from dataclasses import dataclass, field

import requests

from common.iceberg_http import CatalogError, IcebergRest, add_column, schema


@dataclass
class LatencyStats:
    n: int
    p50_ms: float
    p95_ms: float
    max_ms: float


def load_table_latency(client: IcebergRest, ns: str, table: str, n: int = 50) -> LatencyStats:
    client.load_table(ns, table)                       # warm-up (connection, JIT, caches)
    samples = []
    for _ in range(n):
        t0 = time.perf_counter()
        client.load_table(ns, table)
        samples.append((time.perf_counter() - t0) * 1000)
    samples.sort()
    return LatencyStats(n, round(statistics.median(samples), 1),
                        round(samples[int(0.95 * (n - 1))], 1), round(samples[-1], 1))


@dataclass
class ContentionResult:
    writers: int
    commits_per_writer: int
    committed: int
    conflicts: int
    seconds: float
    columns_added: int

    @property
    def commits_per_second(self) -> float:
        return round(self.committed / self.seconds, 1)


def contention(make_client, ns: str, table: str, writers: int = 4, commits_per_writer: int = 5,
               max_attempts: int = 50) -> ContentionResult:
    """N writers each add M columns to the same table at the same time.

    Each commit carries assert-current-schema-id, so concurrent schema changes collide (409)
    and must be retried on fresh metadata. Correctness check: exactly N*M columns end up added.
    """
    clients = [make_client() for _ in range(writers)]
    start = threading.Barrier(writers)
    lock = threading.Lock()
    stats = {"committed": 0, "conflicts": 0}
    errors: list[BaseException] = []

    def work(idx: int, client: IcebergRest):
        try:
            start.wait()
            for j in range(commits_per_writer):
                for _ in range(max_attempts):
                    meta = client.load_table(ns, table)["metadata"]
                    try:
                        client.commit(ns, table, *add_column(meta, f"w{idx}_c{j}", "string"))
                        with lock:
                            stats["committed"] += 1
                        break
                    except CatalogError as e:
                        if e.status != 409:
                            raise
                        with lock:
                            stats["conflicts"] += 1
                else:
                    raise RuntimeError(f"writer {idx} starved")
        except BaseException as e:          # surface thread failures in the caller
            errors.append(e)

    threads = [threading.Thread(target=work, args=(i, c)) for i, c in enumerate(clients)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0
    if errors:
        raise errors[0]
    final = clients[0].load_table(ns, table)["metadata"]
    current = next(s for s in final["schemas"] if s["schema-id"] == final["current-schema-id"])
    added = len([f for f in current["fields"] if f["name"].startswith("w")])
    return ContentionResult(writers, commits_per_writer, stats["committed"], stats["conflicts"],
                            round(elapsed, 2), added)


@dataclass
class Capabilities:
    rest_spec: bool = True
    auth_required: bool = False
    credential_vending: bool = False
    multi_table_transactions: bool = False
    views: bool = False
    register_table: bool = False
    endpoints: list[str] = field(default_factory=list)


def probe_capabilities(client: IcebergRest, ns: str, table: str, vending_client: IcebergRest | None = None) -> Capabilities:
    eps = client.endpoints()
    caps = Capabilities(endpoints=eps)
    caps.multi_table_transactions = "POST /v1/{prefix}/transactions/commit" in eps
    caps.views = "GET /v1/{prefix}/namespaces/{namespace}/views" in eps
    caps.register_table = "POST /v1/{prefix}/namespaces/{namespace}/register" in eps
    # Anonymous request to the same URL: does the catalog enforce authentication?
    anon = requests.get(client._url(f"/namespaces/{client.ns_path(ns)}/tables/{table}"), timeout=30)
    caps.auth_required = anon.status_code in (401, 403)
    # Ask for vended credentials and look for temporary S3 keys in the response.
    vc = vending_client or client
    try:
        res = vc._req("GET", vc._url(f"/namespaces/{vc.ns_path(ns)}/tables/{table}"),
                      headers={"X-Iceberg-Access-Delegation": "vended-credentials"})
        cfg = dict(res.get("config", {}))
        for cred in res.get("storage-credentials", []):
            cfg |= cred["config"]
        caps.credential_vending = "s3.session-token" in cfg
    except CatalogError:
        caps.credential_vending = False
    return caps


ORDERS = schema([("order_id", "long", True), ("amount", "double", False)])
