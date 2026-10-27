from collections import Counter

import pytest

from requirements_validation.requirements import load
from acceptance_probes.probes import Context, run_acceptance
from acceptance_probes import probes


@pytest.fixture(scope="module")
def ctx(spark, ns):
    return Context(spark, ns)


def test_s3_api_probe(ctx):
    ok, ev = probes.s3_api(ctx)
    assert ok and ev == {"put_get_list_delete": True}


def test_rest_catalog_probe_sees_one_state_from_two_engines(ctx):
    ok, ev = probes.rest_catalog(ctx)
    assert ok
    assert ev["spark_snapshot"] == ev["pyiceberg_snapshot"]


def test_schema_rollback_restores_data_but_not_schema_and_lacks_attribution(ctx):
    ok, ev = probes.schema_evolution_rollback(ctx)
    assert ev["schema_versions"] == 2                       # both schemas kept in table metadata
    assert ev["rows_after_rollback"] == 1                   # data rolled back to alice's snapshot
    assert ev["columns_after_rollback"] == ["id", "amount", "risk_score"]   # schema is NOT rolled back
    assert ev["data_commits_attributed"] == ["alice", "bob"]
    assert ev["schema_change_snapshots"] == 0               # ALTER TABLE creates no snapshot ...
    assert not ok                                           # ... so CAT-02 "attribution for all changes" fails here


def test_upsert_is_deterministic_under_duplicates_late_events_and_replay(ctx):
    ok, ev = probes.idempotent_upsert(ctx)
    assert ok and ev == {"state": [(1, "FILLED"), (2, "NEW")], "replay_identical": True}


def test_identifier_fields_require_a_not_null_key(ctx):
    ok, ev = probes.identifier_fields(ctx)
    assert ok and ev == {"identifier_fields": ["client_id"], "nullable_column_rejected": True}


def test_python_client_reads_what_spark_wrote(ctx):
    ok, ev = probes.python_access(ctx)
    assert ok and ev == {"pyiceberg_rows": 250, "spark_rows": 250}


def test_acceptance_run_over_the_hamerliva_requirements(ctx):
    results = {r.requirement: r for r in run_acceptance(load(), ctx, metrics={"dashboard_query_seconds_p95": 2.4})}
    assert Counter(r.method for r in results.values()) == {"probe": 6, "metric": 3, "manual": 9}
    assert {k for k, r in results.items() if r.status == "pass"} == \
        {"STO-02", "ING-01", "ING-04", "CAT-01", "CON-04", "CON-01"}
    assert results["CAT-02"].status == "fail"
    assert results["STO-03"].status == "not-measured" and results["ING-02"].status == "not-measured"
