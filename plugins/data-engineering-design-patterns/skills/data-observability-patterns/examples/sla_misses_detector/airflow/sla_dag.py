"""SLA Misses Detector - Airflow 2.x.

Two complementary mechanisms:

1. Native `sla=` + DAG `sla_miss_callback`. Gotcha: Airflow 2.x
   measures the SLA from the DAG run's scheduled time, NOT from the task start
   (a 10s SLA on a task starting at 08:00:11 is already missed). Removed in Airflow 3.0;
   Airflow 3.1 replaces it with Deadline Alerts.
2. Explicit task-duration SLA: `on_success_callback` compares `ti.duration` with a
   per-task budget, so the measure is end - start of the task itself.

Business case: job scheduled at 06:00 must finish in 40 minutes.

Run:  copy to $AIRFLOW_HOME/dags (Airflow 2.x; set check_slas = True in [core]).
Deps: apache-airflow>=2.6,<3
"""
import datetime
import time

import pendulum
from airflow.decorators import dag, task

TASK_DURATION_SLA = {"process_visits": datetime.timedelta(minutes=40)}


def notify(message: str) -> None:
    # ponytail: print only; send to Slack/email/PagerDuty and to downstream consumers here.
    print(f"[SLA MISS] {message}")


def on_dag_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    notify(f"DAG {dag.dag_id}: tasks {task_list} missed their SLA; blocking: {blocking_task_list}")


def check_task_duration(context) -> None:
    ti = context["ti"]
    budget = TASK_DURATION_SLA.get(ti.task_id)
    if budget and ti.duration and ti.duration > budget.total_seconds():
        notify(f"{ti.task_id} took {ti.duration:.0f}s, budget {budget.total_seconds():.0f}s")


@dag(schedule="0 6 * * *", start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False,
     sla_miss_callback=on_dag_sla_miss, tags=["observability"])
def visits_daily_with_sla():

    @task
    def extract_visits(ds=None) -> str:
        return f"/data/visits/{ds}"

    # Native SLA: counted from 06:00 (DAG run start), so it must cover upstream tasks too.
    @task(sla=datetime.timedelta(minutes=40), on_success_callback=check_task_duration)
    def process_visits(path: str) -> None:
        time.sleep(5)  # ... real processing ...

    process_visits(extract_visits())


visits_daily_with_sla()
