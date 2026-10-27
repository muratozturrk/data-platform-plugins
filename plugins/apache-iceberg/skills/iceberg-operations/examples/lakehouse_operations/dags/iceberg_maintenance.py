"""Policy-driven, metadata-aware Iceberg maintenance DAG.

One task group per table in the central policy file:
    collect_metrics -> choose_actions (branch) -> compact -> expire -> publish_metrics
                                             \\-> nothing_to_do ------------/
A failing task triggers an alert callback that pushes a failure gauge.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from common.lakehouse import get_spark
from maintenance_orchestration import maintenance as m

POLICIES = m.load_policies()


def alert_on_failure(context):
    """on_failure_callback: push a failure gauge for the table."""
    ti = context["task_instance"]
    table = ti.task_id.split(".")[0].replace("__", ".")
    m.push_metrics({"table": table}, job="iceberg_maintenance_alerts",
                   extra={"maintenance_failure": 1, "failed_at": datetime.now().timestamp()})


def _collect(table, policy):
    return m.collect_metrics(get_spark(), table, policy)


def _choose(table, policy, ti):
    group = table.replace(".", "__")
    actions = m.plan(ti.xcom_pull(task_ids=f"{group}.collect_metrics"), policy)
    return [f"{group}.{a}" for a in actions] or [f"{group}.nothing_to_do"]


def _compact(table, policy):
    return m.compact(get_spark(), table, policy)


def _expire(table, policy, ti):
    group = table.replace(".", "__")
    return m.expire(get_spark(), table, policy, ti.xcom_pull(task_ids=f"{group}.collect_metrics"))


def _publish(table, policy, ti):
    group = table.replace(".", "__")
    before = ti.xcom_pull(task_ids=f"{group}.collect_metrics")
    after = m.collect_metrics(get_spark(), table, policy)   # measure the effect, not just the run
    m.push_metrics(after, extra={"maintenance_success": 1,
                                 "files_removed_by_maintenance": before["data_files"] - after["data_files"]})
    return after


with DAG(
    dag_id="iceberg_table_maintenance",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,                       # never two maintenance runs on the same tables at once
    default_args={"retries": 0, "retry_delay": timedelta(minutes=5), "on_failure_callback": alert_on_failure},
    tags=["iceberg", "maintenance"],
) as dag:
    for table, policy in POLICIES.items():
        kwargs = {"table": table, "policy": policy}
        with TaskGroup(group_id=table.replace(".", "__")):
            collect = PythonOperator(task_id="collect_metrics", python_callable=_collect, op_kwargs=kwargs)
            choose = BranchPythonOperator(task_id="choose_actions", python_callable=_choose, op_kwargs=kwargs)
            compact = PythonOperator(task_id="compact", python_callable=_compact, op_kwargs=kwargs)
            expire = PythonOperator(task_id="expire", python_callable=_expire, op_kwargs=kwargs,
                                    trigger_rule=TriggerRule.NONE_FAILED)   # runs whether or not compact ran
            nothing = EmptyOperator(task_id="nothing_to_do")
            publish = PythonOperator(task_id="publish_metrics", python_callable=_publish, op_kwargs=kwargs,
                                     trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
            collect >> choose >> [compact, expire, nothing]
            compact >> expire
            [compact, expire, nothing] >> publish
