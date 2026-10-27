"""Incident-response DAG for a bad write (data-driven).

Triggered manually or by an alert with {"table": "ns.t"}:
    find_last_good -> rollback -> purge_abandoned -> validate -> notify
Instead of a hard-coded snapshot id and an older_than cutoff, the target comes from the
latest data-quality tag and the abandoned snapshots are expired by id.
"""
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from common.lakehouse import get_spark
from maintenance_orchestration.maintenance import push_metrics
from disaster_recovery import recovery as r


def _find(params, ti):
    spark, table = get_spark(), params["table"]
    good = r.last_good_snapshot(spark, table)
    ti.xcom_push(key="abandoned", value=r.snapshots_after(spark, table, good))
    return good


def _rollback(params, ti):
    r.rollback(get_spark(), params["table"], ti.xcom_pull(task_ids="find_last_good"))


def _purge(params, ti):
    return r.purge_snapshots(get_spark(), params["table"], ti.xcom_pull(task_ids="find_last_good", key="abandoned"))


def _validate(params):
    missing = r.validate_snapshot(get_spark(), params["table"])
    if any(missing.values()):
        raise RuntimeError(f"restored snapshot is not loadable, missing files: {missing}")


def _notify(params, ti):
    # Communication layer: a metric here; Slack/Jira/Teams operators in production.
    push_metrics({"table": params["table"]}, job="iceberg_recovery",
                 extra={"abandoned_snapshots": len(ti.xcom_pull(task_ids="find_last_good", key="abandoned")),
                        "recovery_success": 1})


with DAG(
    dag_id="iceberg_recovery",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    params={"table": Param("prod.sales", type="string")},
    tags=["iceberg", "disaster-recovery"],
) as dag:
    steps = [PythonOperator(task_id=tid, python_callable=fn) for tid, fn in (
        ("find_last_good", _find), ("rollback", _rollback), ("purge_abandoned", _purge),
        ("validate", _validate), ("notify", _notify))]
    for upstream, downstream in zip(steps, steps[1:]):
        upstream >> downstream
