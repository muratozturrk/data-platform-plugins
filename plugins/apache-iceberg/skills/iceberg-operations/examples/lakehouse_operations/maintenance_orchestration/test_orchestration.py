"""A policy-driven Airflow DAG maintains Iceberg tables only where metadata says it is needed."""
import json
import os
from datetime import datetime
from pathlib import Path

import pytest
import requests
from prometheus_client.parser import text_string_to_metric_families

from common.sales import append_batches, create_sales_table, fingerprint
from maintenance_orchestration.maintenance import TablePolicy, plan

DAG_FILE = Path(__file__).resolve().parents[1] / "dags" / "iceberg_maintenance.py"
DAY = datetime(2025, 3, 1)


def pushed_metrics() -> dict[tuple[str, str], float]:
    text = requests.get(os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091") + "/metrics", timeout=10).text
    return {(s.name, s.labels.get("table", "")): s.value
            for fam in text_string_to_metric_families(text) for s in fam.samples}


def load_dag(policy_file: Path):
    from airflow.models import DagBag
    os.environ["TABLE_POLICIES"] = str(policy_file)
    bag = DagBag(dag_folder=str(DAG_FILE), include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("iceberg_table_maintenance")


def states(dag_run) -> dict[str, str]:
    return {ti.task_id: ti.state for ti in dag_run.get_task_instances()}


@pytest.fixture(scope="module")
def tables(spark, ns, tmp_path_factory):
    hot, cold = f"{ns}.orders_cdc", f"{ns}.monthly_report"
    create_sales_table(spark, hot)
    append_batches(spark, hot, batches=12, rows_per_batch=500, day=DAY, files_per_batch=4)   # 48 small files
    create_sales_table(spark, cold)
    append_batches(spark, cold, batches=1, rows_per_batch=2_000, day=DAY, files_per_batch=1)
    policies = {
        hot: {"max_small_files": 10, "retain_last": 3, "max_snapshots": 5},
        cold: {"max_small_files": 10, "retain_last": 3, "max_snapshots": 5},
        f"{ns}.does_not_exist": {},                               # broken entry: must fail alone
    }
    path = tmp_path_factory.mktemp("policies") / "table_policies.json"
    path.write_text(json.dumps(policies))
    return {"hot": hot, "cold": cold, "missing": f"{ns}.does_not_exist", "policy_file": path,
            "group": {k: v.replace(".", "__") for k, v in {"hot": hot, "cold": cold,
                                                             "missing": f"{ns}.does_not_exist"}.items()}}


@pytest.fixture(scope="module")
def first_run(spark, tables):
    before = {k: _table_state(spark, tables[k]) for k in ("hot", "cold")}
    dag = load_dag(tables["policy_file"])
    run = dag.test()
    after = {k: _table_state(spark, tables[k]) for k in ("hot", "cold")}
    return dag, run, before, after


def _table_state(spark, table):
    files = spark.sql(f"SELECT count(*) c FROM {table}.files").first().c
    snaps = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at").collect()
    return {"files": files, "snapshots": len(snaps), "current": snaps[-1].snapshot_id,
            "fingerprint": fingerprint(spark, table)}


def test_plan_follows_each_tables_policy():
    cdc = TablePolicy(max_small_files=500, retain_last=50, max_snapshot_age_hours=24, max_snapshots=500)
    report = TablePolicy(max_small_files=2000, retain_last=12, max_snapshot_age_hours=2160, max_snapshots=200)
    metrics = {"small_files": 800, "delete_files": 0, "snapshots": 120, "oldest_snapshot_age_hours": 30.0}
    assert plan(metrics, cdc) == ["compact", "expire"]
    assert plan(metrics, report) == []                                # same table state, lenient policy
    assert plan({**metrics, "small_files": 3, "delete_files": 51}, cdc) == ["compact", "expire"]
    assert plan({**metrics, "snapshots": 40}, cdc) == ["compact"]       # never expire below retain_last


def test_policy_file_generates_one_task_group_per_table(tables):
    dag = load_dag(tables["policy_file"])
    assert dag.schedule_interval == "@daily" and dag.max_active_runs == 1 and not dag.catchup
    for group in tables["group"].values():
        assert {f"{group}.{t}" for t in ("collect_metrics", "choose_actions", "compact", "expire",
                                         "nothing_to_do", "publish_metrics")} <= set(dag.task_ids)
    g = tables["group"]["hot"]
    assert dag.get_task(f"{g}.compact").downstream_task_ids == {f"{g}.expire", f"{g}.publish_metrics"}


def test_fragmented_table_is_compacted_then_expired(first_run, tables):
    _, run, before, after = first_run
    g, s = tables["group"]["hot"], states(run)
    assert [s[f"{g}.{t}"] for t in ("compact", "expire", "nothing_to_do", "publish_metrics")] == \
        ["success", "success", "skipped", "success"]
    assert before["hot"]["files"] == 48 and after["hot"]["files"] == 1
    assert before["hot"]["snapshots"] == 12 and after["hot"]["snapshots"] == 3       # retain_last=3
    assert after["hot"]["fingerprint"] == before["hot"]["fingerprint"]


def test_healthy_table_is_left_alone(first_run, tables):
    _, run, before, after = first_run
    g, s = tables["group"]["cold"], states(run)
    assert s[f"{g}.nothing_to_do"] == "success"
    assert s[f"{g}.compact"] == "skipped" and s[f"{g}.expire"] == "skipped"
    assert after["cold"] == before["cold"]                          # no new snapshot, no catalog commit


def test_metrics_reach_prometheus_pushgateway(first_run, tables):
    metrics = pushed_metrics()
    hot, cold = tables["hot"], tables["cold"]
    assert metrics[("iceberg_small_files", hot)] == 1         # the one output file is still < 20 MB (small test data)
    assert metrics[("iceberg_data_files", hot)] == 1
    assert metrics[("iceberg_files_removed_by_maintenance", hot)] == 47
    assert metrics[("iceberg_maintenance_success", hot)] == 1
    assert metrics[("iceberg_snapshots", cold)] == 1


def test_failing_table_alerts_without_blocking_other_tables(first_run, tables):
    _, run, _, _ = first_run
    g, s = tables["group"]["missing"], states(run)
    assert run.state == "failed"
    assert s[f"{g}.collect_metrics"] == "failed"
    assert s[f"{g}.publish_metrics"] == "upstream_failed"
    assert s[f"{tables['group']['hot']}.publish_metrics"] == "success"
    assert pushed_metrics()[("iceberg_maintenance_failure", tables["missing"])] == 1


def test_second_run_is_idempotent(spark, first_run, tables):
    dag, _, _, after = first_run
    run = dag.test()
    s = states(run)
    for key in ("hot", "cold"):
        assert s[f"{tables['group'][key]}.nothing_to_do"] == "success"
        assert _table_state(spark, tables[key]) == after[key]       # same files, same current snapshot


def test_python_operator_return_value_does_not_gate_downstream():
    """A bool returned from a PythonOperator gates nothing; only ShortCircuitOperator/branching gates."""
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator, ShortCircuitOperator

    with DAG("gate_check", start_date=datetime(2025, 1, 1), schedule=None) as dag:
        PythonOperator(task_id="check_plain", python_callable=lambda: False) >> EmptyOperator(task_id="compact_plain")
        ShortCircuitOperator(task_id="check_gate", python_callable=lambda: False) >> EmptyOperator(task_id="compact_gated")
    s = states(dag.test())
    assert s["compact_plain"] == "success"          # compaction would run although the check said "no"
    assert s["compact_gated"] == "skipped"
