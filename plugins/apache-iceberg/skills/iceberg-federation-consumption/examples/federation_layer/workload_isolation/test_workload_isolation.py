"""Running the engine yourself means owning workload isolation (resource groups)."""
import uuid

import pytest

from common.federation import run
from federation_requirements.semantic_layer import deploy_all
from workload_isolation.workloads import SLOW_SOURCE_QUERY, query_info, submit, wait_for_state


@pytest.fixture(scope="module", autouse=True)
def semantic_layer():
    deploy_all()


def _tag(name: str) -> str:
    return f"wl-{name}-{uuid.uuid4().hex[:6]}"


@pytest.mark.parametrize("user, source, tags, expected", [
    ("etl", None, None, ["global", "etl"]),
    ("bi_dashboard", "superset-prod", None, ["global", "bi"]),
    ("admin", None, ["adhoc"], ["global", "adhoc"]),
    ("engineer", None, None, ["global", "other"]),
])
def test_selectors_route_each_workload_to_its_group(user, source, tags, expected, record):
    src = source or _tag(user)
    run("SELECT count(*) FROM lake.semantic.customer_360" if user == "bi_dashboard"
        else "SELECT count(*) FROM lake.sales.orders", user=user, source=src, client_tags=tags)
    info = query_info(src)[-1]
    assert info["resource_group_id"] == expected
    record(resource_group=".".join(info["resource_group_id"]))


def test_adhoc_limits_queue_and_reject_without_hurting_dashboards(record):
    """global.adhoc: hardConcurrencyLimit=1, maxQueued=1."""
    first, second, third = _tag("first"), _tag("second"), _tag("third")
    a = submit(SLOW_SOURCE_QUERY.format(seconds=6), source=first, client_tags=["adhoc"])
    wait_for_state(first, {"RUNNING"})

    b = submit(SLOW_SOURCE_QUERY.format(seconds=1), source=second, client_tags=["adhoc"])
    assert wait_for_state(second, {"QUEUED"})["resource_group_id"] == ["global", "adhoc"]

    c = submit(SLOW_SOURCE_QUERY.format(seconds=1), source=third, client_tags=["adhoc"])
    c.thread.join(15)
    assert c.error is not None and getattr(c.error, "error_name", "") == "QUERY_QUEUE_FULL"

    # Meanwhile a dashboard query in global.bi runs at once: no noisy-neighbour effect.
    dash = submit("SELECT region, count(*) FROM lake.semantic.customer_360 GROUP BY region",
                  source="superset-prod", user="bi_dashboard")
    dash.thread.join(30)
    assert dash.error is None and len(dash.rows) == 5
    assert a.thread.is_alive()                        # the ad hoc query is still running

    a.thread.join(30)
    b.thread.join(30)
    assert a.error is None and b.error is None
    assert b.stats["queuedTimeMillis"] > 1000         # waited for the first query to finish
    assert dash.stats["queuedTimeMillis"] < 1000
    record(adhoc_second_queued_ms=b.stats["queuedTimeMillis"],
           dashboard_queued_ms=dash.stats["queuedTimeMillis"],
           dashboard_elapsed_s=round(dash.elapsed_s, 2), third_error=c.error.error_name)
