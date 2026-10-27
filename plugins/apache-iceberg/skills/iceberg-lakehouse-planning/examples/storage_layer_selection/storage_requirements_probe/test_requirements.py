import uuid

import pytest

from common.s3 import client, empty_bucket, ensure_bucket

from .probe import run_probe
from .requirements import StorageRequirements, evaluate, gaps

STORES = ["seaweedfs", "rustfs"]
BUCKET = "probes"


@pytest.fixture(scope="module")
def reports():
    out = {}
    for store in STORES:
        ensure_bucket(store, BUCKET)
        prefix = f"probe-{uuid.uuid4().hex[:6]}"
        out[store] = run_probe(store, client(store), BUCKET, prefix)
        empty_bucket(store, BUCKET, prefix)
    return out


@pytest.mark.parametrize("store", STORES)
def test_tail_latency_is_measured_not_just_the_average(reports, store, measure):
    s = reports[store].small_get
    measure(f"{store}_small_get_4KiB_ms", f"mean={s.mean_ms} p50={s.p50_ms} p95={s.p95_ms} "
                                          f"p99={s.p99_ms} max={s.max_ms} (n={s.n})")
    assert s.n == 200
    assert s.p50_ms <= s.p95_ms <= s.p99_ms <= s.max_ms
    assert s.p99_ms > s.p50_ms        # the tail is always worse than the median


@pytest.mark.parametrize("store", STORES)
def test_concurrency_raises_throughput_less_than_it_raises_tail_latency(reports, store, measure):
    """Stress-testing concurrency: the store absorbs more requests, but each one waits longer."""
    r = reports[store]
    measure(f"{store}_concurrency_1_vs_16", f"req/s={r.requests_per_s} p99_ms={r.p99_ms} "
                                            f"speedup={r.concurrency_speedup}x tail_inflation={r.tail_inflation}x")
    # Throughput gain is small here: one Python client (GIL) and one local store node saturate early.
    assert r.tail_inflation > 2.0
    assert r.tail_inflation > r.concurrency_speedup


@pytest.mark.parametrize("store", STORES)
def test_integrity_checks(reports, store, measure):
    r = reports[store]
    measure(f"{store}_integrity", f"bad_md5_rejected={r.rejects_bad_checksum} etag_is_md5={r.etag_is_md5} "
                                  f"read_after_write={r.read_after_write} list_after_write={r.list_after_write}")
    assert r.rejects_bad_checksum and r.etag_is_md5
    assert r.read_after_write and r.list_after_write      # strong consistency, as on S3 since 2020


@pytest.mark.parametrize("store", STORES)
def test_anonymous_access_is_denied_by_default(reports, store):
    assert reports[store].anonymous_denied


def test_same_requirements_give_different_verdicts_per_store(reports, measure):
    """A regulated workload needs encryption at rest; the default SeaweedFS setup cannot provide it."""
    regulated = StorageRequirements("regulated analytics", max_p99_small_get_ms=250,
                                    min_concurrency_speedup=1.0, encryption_at_rest=True)
    verdicts = {s: gaps(evaluate(reports[s], regulated)) for s in STORES}
    measure("gaps", verdicts)
    assert verdicts["seaweedfs"] == ["security: SSE-S3 encryption at rest (measured False)"]
    assert verdicts["rustfs"] == []


def test_impossible_latency_requirement_is_reported_as_a_gap(reports):
    strict = StorageRequirements("sub-millisecond", max_p99_small_get_ms=0.01, min_concurrency_speedup=1.0)
    for s in STORES:
        assert [g.split(" (")[0] for g in gaps(evaluate(reports[s], strict))] == \
               ["performance: p99 small GET <= 0.01 ms"]
