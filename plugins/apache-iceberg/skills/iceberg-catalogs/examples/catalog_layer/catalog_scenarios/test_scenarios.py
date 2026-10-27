"""Replay the catalog-selection scenarios, then run the stress-test scenario for real on Nessie."""
import pytest

from common.lakehouse import list_keys
from requirements_probe.requirements import Requirement, choose, evaluate
from catalog_scenarios import nessie_refs
from catalog_scenarios.catalogs import catalog_facts
from catalog_scenarios.scenarios import SCENARIOS


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s.key for s in SCENARIOS])
def test_scorecard_reproduces_the_expected_outcome(scenario, measure):
    ranked = evaluate(catalog_facts(), list(scenario.requirements))
    winner = ranked[0]
    assert winner.eligible and winner.candidate == scenario.expected_outcome
    by_name = {v.candidate: v for v in ranked}
    for rejected in scenario.rejected:          # the alternatives that were turned down
        assert not by_name[rejected].eligible, rejected
    eligible = [v.candidate for v in ranked if v.eligible]
    measure(scenario.key, f"winner={winner.candidate} eligible={eligible} warnings={winner.warnings}")


def test_python_first_scenario_picks_a_deprecated_catalog():
    s = next(s for s in SCENARIOS if s.key == "python_first")
    winner = choose(catalog_facts(), list(s.requirements))
    assert winner.candidate == "hadoop"
    assert winner.warnings == ["hadoop is deprecated: plan the exit before adopting"]
    # Swap "no service at all" for "safe concurrent writers": the lightweight answer becomes JDBC
    # (SQLite or Postgres), which PyIceberg supports natively as SqlCatalog.
    reqs = [r for r in s.requirements if r.key != "no_service"] + [Requirement("multi_engine")]
    assert choose(catalog_facts(), reqs).candidate == "jdbc"


def test_changing_one_constraint_changes_the_decision():
    s = next(s for s in SCENARIOS if s.key == "aws_fintech")
    portable = [r for r in s.requirements if r.key not in ("aws_native", "cloud_agnostic")]
    portable.append(Requirement("cloud_agnostic", why="customers on several clouds"))
    winner = choose(catalog_facts(), portable)
    assert winner.candidate in {"dremio", "snowflake_open_catalog"}     # Polaris-based services


# ---------------------------------------------------------------------------------------------
# Stress-test scenario on a real Nessie server: daily zero-copy clone of production for stress tests.
# ---------------------------------------------------------------------------------------------
@pytest.fixture
def production(spark, uid):
    ns = f"risk_{uid}"
    spark.sql(f"CREATE NAMESPACE nessie.{ns}")
    spark.sql(f"CREATE TABLE nessie.{ns}.positions (account bigint, instrument string, qty double) USING iceberg")
    spark.sql(f"CREATE TABLE nessie.{ns}.prices (instrument string, price double) USING iceberg")
    spark.sql(f"INSERT INTO nessie.{ns}.positions SELECT id, concat('I', id % 20), 100.0 FROM range(1000)")
    spark.sql(f"INSERT INTO nessie.{ns}.prices SELECT concat('I', id), 50.0 + id FROM range(20)")
    yield ns
    for t in ("positions", "prices"):
        spark.sql(f"DROP TABLE IF EXISTS nessie.{ns}.{t}")   # NessieCatalog: gc disabled, no PURGE
    spark.sql(f"DROP NAMESPACE IF EXISTS nessie.{ns}")


def exposure(spark, ns: str, ref: str = "main") -> float:
    pos, px = f"nessie.{ns}.`positions@{ref}`", f"nessie.{ns}.`prices@{ref}`"
    return spark.sql(f"SELECT sum(p.qty * x.price) e FROM {pos} p JOIN {px} x USING (instrument)").first().e


def test_stress_test_branch_is_a_zero_copy_clone_of_production(spark, production, uid, measure):
    ns = production
    branch = f"stress_{uid}"
    prod_exposure = exposure(spark, ns)
    objects = len(list_keys("s3://warehouse/nessie/"))

    nessie_refs.create_branch(branch)
    assert len(list_keys("s3://warehouse/nessie/")) == objects                 # nothing copied
    assert nessie_refs.table_metadata_location(ns, "positions", branch) == \
        nessie_refs.table_metadata_location(ns, "positions")

    # Adverse scenario: prices drop 40% on the branch only.
    spark.sql(f"UPDATE nessie.{ns}.`prices@{branch}` SET price = price * 0.6")
    stressed = exposure(spark, ns, branch)

    assert stressed == pytest.approx(prod_exposure * 0.6)
    assert exposure(spark, ns) == prod_exposure                                 # main untouched
    assert nessie_refs.diff("main", branch) == [f"{ns}.prices"]
    nessie_refs.delete_branch(branch)                                           # discard the run
    assert branch not in nessie_refs.branches()
    assert exposure(spark, ns) == prod_exposure
    measure("stress_exposure", {"production": prod_exposure, "stressed": round(stressed, 2),
                                "objects_copied_by_branch": 0})


def test_multi_table_change_becomes_visible_in_one_merge(spark, production, uid, measure):
    ns = production
    branch = f"etl_{uid}"
    nessie_refs.create_branch(branch)
    # A two-table ETL job: new instrument I99 must appear in prices AND positions together.
    spark.sql(f"INSERT INTO nessie.{ns}.`prices@{branch}` VALUES ('I99', 1000.0)")
    spark.sql(f"INSERT INTO nessie.{ns}.`positions@{branch}` VALUES (5000, 'I99', 10.0)")

    count_i99 = lambda ref: spark.sql(  # noqa: E731
        f"SELECT (SELECT count(*) FROM nessie.{ns}.`prices@{ref}` WHERE instrument = 'I99') + "
        f"(SELECT count(*) FROM nessie.{ns}.`positions@{ref}` WHERE instrument = 'I99') c").first().c
    assert count_i99("main") == 0                    # readers on main see neither change
    main_before = nessie_refs.head("main")

    nessie_refs.merge(branch, message=f"ETL {uid}: add instrument I99")

    assert count_i99("main") == 2                    # both tables changed in one step
    history = nessie_refs.history("main", limit=5)
    assert history[0]["commitMeta"]["message"] == f"ETL {uid}: add instrument I99"
    assert history[0]["parentCommitHash"] == main_before   # a single commit on top of old main
    nessie_refs.delete_branch(branch)
    measure("merge_commit", history[0]["commitMeta"]["hash"][:12])
