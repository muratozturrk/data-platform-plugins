"""Storage requirements as data, evaluated against measured probe results.

The order: define requirements from the audit first (performance, security, integrity,
cost/operations), then judge storage options "not based on their marketing claims but on how
well they fit". Cost and operations cannot be probed over the API; they stay in the scorecard
of storage_scorecard.
"""
from dataclasses import dataclass

from .probe import ProbeReport


@dataclass(frozen=True)
class StorageRequirements:
    name: str
    max_p99_small_get_ms: float          # performance: tail latency of a small-object GET
    min_concurrency_speedup: float       # performance: req/s at highest vs lowest concurrency
    checksum_validation: bool = True     # integrity: corrupted uploads are rejected
    strong_consistency: bool = True      # integrity: read-after-write and list-after-write
    deny_anonymous: bool = True          # security: no access without credentials
    encryption_at_rest: bool = False     # security: SSE with store-managed keys


@dataclass(frozen=True)
class Finding:
    category: str
    requirement: str
    measured: object
    ok: bool


def evaluate(r: ProbeReport, req: StorageRequirements) -> list[Finding]:
    f = [
        Finding("performance", f"p99 small GET <= {req.max_p99_small_get_ms} ms",
                r.small_get.p99_ms, r.small_get.p99_ms <= req.max_p99_small_get_ms),
        Finding("performance", f"concurrency speedup >= {req.min_concurrency_speedup}x",
                r.concurrency_speedup, r.concurrency_speedup >= req.min_concurrency_speedup),
    ]
    if req.checksum_validation:
        f.append(Finding("integrity", "rejects corrupted upload (Content-MD5)",
                         r.rejects_bad_checksum, bool(r.rejects_bad_checksum)))
    if req.strong_consistency:
        ok = bool(r.read_after_write and r.list_after_write)
        f.append(Finding("integrity", "read/list-after-write consistency", ok, ok))
    if req.deny_anonymous:
        f.append(Finding("security", "anonymous read denied", r.anonymous_denied, bool(r.anonymous_denied)))
    if req.encryption_at_rest:
        f.append(Finding("security", "SSE-S3 encryption at rest", r.sse_s3, bool(r.sse_s3)))
    return f


def gaps(findings: list[Finding]) -> list[str]:
    return [f"{x.category}: {x.requirement} (measured {x.measured})" for x in findings if not x.ok]
