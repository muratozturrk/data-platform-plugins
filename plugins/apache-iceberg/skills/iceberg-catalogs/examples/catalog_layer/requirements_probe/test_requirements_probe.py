"""Turn catalog evaluation questions into measurements and a scorecard."""
import pytest

from common.iceberg_http import IcebergRest
from common.lakehouse import ROOT_CLIENT
from requirements_probe.probe import (ORDERS, contention, load_table_latency,
                                              probe_capabilities)
from requirements_probe.requirements import (Requirement, choose, evaluate,
                                                     facts_from_probe)

POLARIS_CATALOG = "probe_lab"
NS = "probe"


def clients():
    return {
        "iceberg-rest-fixture": IcebergRest.reference,
        "polaris": lambda: IcebergRest.polaris(POLARIS_CATALOG, *ROOT_CLIENT),
    }


@pytest.fixture(scope="module")
def measured(polaris_admin):
    """Run every probe once per catalog; the tests below assert on the collected numbers."""
    polaris_admin.ensure_catalog(POLARIS_CATALOG)
    results = {}
    for name, make in clients().items():
        c = make()
        if NS not in c.list_namespaces():
            c.create_namespace(NS)
        for t in ("lookup", "contended"):
            if c.table_exists(NS, t):
                c.drop_table(NS, t, purge=True)
            c.create_table(NS, t, ORDERS)
        results[name] = {
            "latency": load_table_latency(c, NS, "lookup", n=50),
            "contention": contention(make, NS, "contended", writers=4, commits_per_writer=5),
            "caps": probe_capabilities(c, NS, "lookup"),
        }
    yield results
    for make in clients().values():
        c = make()
        for t in c.list_tables(NS):
            c.drop_table(NS, t, purge=True)
        c.drop_namespace(NS)
    polaris_admin.drop_catalog_cascade(POLARIS_CATALOG)


@pytest.mark.parametrize("catalog", ["iceberg-rest-fixture", "polaris"])
def test_load_table_latency_is_on_the_critical_path(measured, catalog, measure):
    lat = measured[catalog]["latency"]
    assert lat.n == 50
    assert 0 < lat.p50_ms <= lat.p95_ms <= lat.max_ms
    assert lat.p95_ms < 1000          # sanity bound for a local stack, not a benchmark claim
    measure(f"{catalog}_load_table_ms", f"p50={lat.p50_ms} p95={lat.p95_ms} max={lat.max_ms}")


@pytest.mark.parametrize("catalog", ["iceberg-rest-fixture", "polaris"])
def test_concurrent_writers_never_lose_a_commit(measured, catalog, measure):
    res = measured[catalog]["contention"]
    assert res.committed == 20
    assert res.columns_added == 20    # every commit landed exactly once
    measure(f"{catalog}_contention", f"4 writers x 5 commits: {res.conflicts} conflicts (409) retried, "
                                     f"{res.seconds}s, {res.commits_per_second} commits/s")


def test_capability_probe_separates_a_dev_catalog_from_a_governed_one(measured, measure):
    fixture, polaris = measured["iceberg-rest-fixture"]["caps"], measured["polaris"]["caps"]
    assert fixture.auth_required is False and polaris.auth_required is True
    assert fixture.credential_vending is False and polaris.credential_vending is True
    for caps in (fixture, polaris):
        assert caps.multi_table_transactions and caps.views and caps.register_table
    measure("capabilities", {"fixture": (fixture.auth_required, fixture.credential_vending),
                             "polaris": (polaris.auth_required, polaris.credential_vending)})


def test_hard_constraints_eliminate_instead_of_ranking_lower():
    candidates = {"cheap": {"rest": True, "low_ops": True}, "governed": {"rest": True, "rbac": True}}
    reqs = [Requirement("rbac", why="audit finding: shared bucket, no per-team access"),
            Requirement("low_ops", must=False, weight=5)]
    ranked = evaluate(candidates, reqs)
    assert [v.candidate for v in ranked] == ["governed", "cheap"]
    assert ranked[1].eligible is False and ranked[1].missing_musts == ["rbac"]
    assert choose(candidates, reqs).candidate == "governed"
    assert choose(candidates, reqs + [Requirement("versioning")]) is None   # nobody qualifies


def test_scorecard_from_measured_facts(measured):
    facts = {name: facts_from_probe(m["caps"], m["latency"], m["contention"])
             for name, m in measured.items()}
    production = [
        Requirement("auth_required", why="security: integrate with IAM / OAuth"),
        Requirement("credential_vending", why="governance: no long-lived storage keys in engines"),
        Requirement("load_p95_ms", max_value=1000, why="performance: interactive queries"),
        Requirement("lost_updates", max_value=0, why="correctness: transaction coordination"),
        Requirement("views", must=False, weight=1),
    ]
    winner = choose(facts, production)
    assert winner.candidate == "polaris"
    loser = next(v for v in evaluate(facts, production) if v.candidate == "iceberg-rest-fixture")
    assert loser.missing_musts == ["auth_required", "credential_vending"]
    # For a local development profile the lightweight fixture is perfectly acceptable.
    dev = [Requirement("rest"), Requirement("lost_updates", max_value=0)]
    assert choose(facts, dev).candidate == "iceberg-rest-fixture"
