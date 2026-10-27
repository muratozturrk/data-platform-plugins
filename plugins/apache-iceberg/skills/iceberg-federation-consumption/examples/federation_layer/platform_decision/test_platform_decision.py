"""Six reference federation platform scenarios, encoded as tests."""
import pytest

from platform_decision.decision import OPTIONS, Requirements, connector_coverage, recommend

SCENARIOS = {
    "fragmented multisource": (
        Requirements(table_formats=frozenset({"iceberg", "delta"}), source_systems=8,
                     location=frozenset({"on_prem", "aws"}), ops_capacity="high",
                     prefer_open_source=True),
        "trino", "Trino (open source, self-managed)"),
    "native Iceberg lakehouse": (
        Requirements(iceberg_centric=True, managed_maintenance=True, location=frozenset({"aws"}),
                     ops_capacity="low"),          # goal: simplify operations
        "dremio", "Dremio Cloud"),
    "business users and governed datasets": (
        Requirements(business_self_service=True, needs_fgac=True, location=frozenset({"azure"})),
        "dremio", None),                              # scenario names the engine, not the deployment
    "lightweight Hudi on S3": (
        Requirements(table_formats=frozenset({"hudi"}), occasional_queries=True, ops_capacity="low",
                     location=frozenset({"aws"})),
        "trino", "Amazon Athena"),
    "on-prem Cloudera modernization": (
        Requirements(table_formats=frozenset({"hive", "iceberg"}), existing_platform="cloudera",
                     location=frozenset({"on_prem"})),
        "trino", "Cloudera (managed Trino)"),
    "hybrid Iceberg, on-prem to ADLS": (
        Requirements(iceberg_centric=True, managed_maintenance=True, location=frozenset({"on_prem", "azure"})),
        "dremio", "Dremio Enterprise Edition"),
}


@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_reference_scenarios(scenario, record):
    req, engine, option = SCENARIOS[scenario]
    rec = recommend(req)
    assert rec.engine == engine
    runner_up = rec.ranking[1] if len(rec.ranking) > 1 else None
    if option is None:          # tie between editions is expected: engine choice is what matters
        assert {s.option.engine for s in rec.ranking if s.score == rec.best.score} == {engine}
    else:
        assert rec.best.option.name == option
        assert rec.best.score > runner_up.score    # decided by a requirement, not by a tie-break
    record(best=rec.best.option.name, score=rec.best.score, reasons=rec.best.reasons,
           runner_up=runner_up.option.name if runner_up else None,
           margin=(rec.best.score - runner_up.score) if runner_up else None)


def test_hudi_requirement_excludes_every_dremio_option():
    rec = recommend(SCENARIOS["lightweight Hudi on S3"][0])
    dremio = [n for n in rec.excluded if n.startswith("Dremio")]
    assert len(dremio) == 3
    assert all("hudi" in rec.excluded[n] for n in dremio)


def test_no_kubernetes_moves_hybrid_iceberg_away_from_dremio_enterprise():
    req = Requirements(iceberg_centric=True, managed_maintenance=True, needs_fgac=True,
                       location=frozenset({"on_prem"}), kubernetes_available=False)
    rec = recommend(req)
    assert rec.excluded["Dremio Enterprise Edition"] == "requires a Kubernetes cluster"
    assert "fine-grained" in rec.excluded["Dremio Community Edition"]
    assert rec.engine == "trino"


def test_impossible_requirements_are_reported_not_guessed():
    dremio_only = [o for o in OPTIONS if o.engine == "dremio"]
    with pytest.raises(ValueError, match="no option satisfies"):
        recommend(Requirements(table_formats=frozenset({"hudi"})), options=dremio_only)


def test_connector_coverage_of_the_running_federation_layer(record):
    cov = connector_coverage({"iceberg", "postgresql", "mongodb"})
    assert cov == {"configured": {"iceberg", "postgresql"}, "missing": {"mongodb"}}
    record(**{k: sorted(v) for k, v in cov.items()})
