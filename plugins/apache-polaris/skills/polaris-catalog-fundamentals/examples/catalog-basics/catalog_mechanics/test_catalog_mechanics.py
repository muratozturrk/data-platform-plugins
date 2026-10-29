"""The catalog as the source of truth: Polaris holds the pointer to the current metadata.json.

Claims tested on Polaris: every client resolves the same current metadata.json; a commit based
on an outdated table state fails; the writer then has to retry.
"""
import threading
import uuid

import pyarrow as pa
import pytest
from pyiceberg.exceptions import CommitFailedException

from common.polaris import ApiError, CatalogClient


def batch(start: int, n: int = 10) -> pa.Table:
    return pa.table({"id": pa.array(range(start, start + n), pa.int64()),
                     "amount": pa.array([1.0] * n)})


@pytest.fixture(scope="module")
def cat(admin, root):
    name = f"mech_{uuid.uuid4().hex[:6]}"
    admin.create_catalog(name, f"s3://warehouse/{name}")
    admin.grant(name, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    engine = root.pyiceberg(name)
    engine.create_namespace("ops")
    engine.create_table("ops.events", schema=batch(0).schema).append(batch(0))
    yield name
    admin.drop_catalog(name)


def test_all_clients_resolve_the_same_current_metadata_file(root, cat, measure):
    engine = root.pyiceberg(cat)
    rest = CatalogClient(cat, root.client_id, root.client_secret, vended=False)
    before = rest.load_table("ops", "events")["metadata-location"]
    assert engine.load_table("ops.events").metadata_location == before
    engine.load_table("ops.events").append(batch(100))          # one client commits...
    after = rest.load_table("ops", "events")["metadata-location"]
    assert after != before                                       # ...every other client sees it next load
    assert engine.load_table("ops.events").metadata_location == after
    measure("2.1_pointer_moves", {"before": before.rsplit("/", 1)[-1], "after": after.rsplit("/", 1)[-1]})


def test_polaris_rejects_a_commit_built_on_a_stale_snapshot(root, cat, measure):
    rest = CatalogClient(cat, root.client_id, root.client_secret, vended=False)
    seen = rest.load_table("ops", "events")["metadata"]["current-snapshot-id"]
    root.pyiceberg(cat).load_table("ops.events").append(batch(200))     # someone else commits
    stale_requirement = [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": seen}]
    with pytest.raises(ApiError) as e:
        rest.c("POST", "/namespaces/ops/tables/events",
               json={"requirements": stale_requirement,
                     "updates": [{"action": "set-properties", "updates": {"owner": "late-writer"}}]})
    assert e.value.status == 409 and "CommitFailedException" in e.value.text
    measure("2.1_stale_commit_error", e.value.text[:200])


def test_pyiceberg_surfaces_the_conflict_instead_of_retrying(root, cat, measure):
    """A failed commit is often assumed to be retried automatically. In PyIceberg 0.9.1 the retry is the
    caller's job: a writer holding an old table object gets CommitFailedException."""
    engine_a, engine_b = root.pyiceberg(cat), root.pyiceberg(cat)
    a, b = engine_a.load_table("ops.events"), engine_b.load_table("ops.events")
    a.append(batch(300))
    with pytest.raises(CommitFailedException) as e:
        b.append(batch(400))                                         # b still holds the old state
    b = engine_b.load_table("ops.events")                            # refresh + retry succeeds
    b.append(batch(400))
    ids = set(engine_a.load_table("ops.events").scan().to_arrow()["id"].to_pylist())
    assert {300, 400} <= ids
    measure("2.1_pyiceberg_conflict", str(e.value)[:160])


def append_with_retry(root, cat: str, start: int, attempts: int = 10) -> int:
    """Optimistic concurrency done right: reload the table and retry on conflict."""
    engine = root.pyiceberg(cat)
    for attempt in range(1, attempts + 1):
        try:
            engine.load_table("ops.events").append(batch(start))
            return attempt
        except CommitFailedException:
            continue
    raise RuntimeError("gave up")


def test_concurrent_writers_with_retry_lose_nothing(root, cat, measure):
    engine = root.pyiceberg(cat)
    rows_before = engine.load_table("ops.events").scan().to_arrow().num_rows
    attempts, threads = {}, []
    for w in range(6):
        t = threading.Thread(target=lambda w=w: attempts.__setitem__(w, append_with_retry(root, cat, 1000 + 100 * w)))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    table = engine.load_table("ops.events")
    assert table.scan().to_arrow().num_rows == rows_before + 6 * 10
    retries = sum(a - 1 for a in attempts.values())
    measure("2.1_concurrent_writers", {"writers": 6, "attempts_per_writer": dict(sorted(attempts.items())),
                                       "total_retries": retries})
