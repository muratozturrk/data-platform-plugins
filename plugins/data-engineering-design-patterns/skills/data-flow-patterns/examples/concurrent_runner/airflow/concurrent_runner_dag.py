"""Concurrent Runner -- Apache Airflow.

Ingestion of independent datasets: run N doesn't need run N-1, so several DAG runs may
execute at the same time (catch-up after an outage, backfills, a slow delivery).
  * max_active_runs=5       -> up to 5 runs in parallel;
  * depends_on_past=False   -> no chaining between runs;
  * pool="ingestion"        -> workload management: the whole team's ingestion DAGs share a
                               bounded slot pool, so a big backfill can't starve the scheduler
                               for other tenants (create it: `airflow pools set ingestion 10 "..."`);
  * max_active_tasks        -> per-DAG task cap.
Shared-state gotcha: everything a run writes must be partitioned by its logical date
(no shared "last processed" pointer), otherwise concurrent runs race each other.

Run: `airflow pools set ingestion 10 ingestion` then `airflow dags backfill devices_loader_concurrent -s 2024-01-01 -e 2024-01-10`.
Requires: apache-airflow>=2.6.
"""
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
    dag_id="devices_loader_concurrent",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=True,
    max_active_runs=5,
    max_active_tasks=10,
    default_args={"depends_on_past": False, "pool": "ingestion", "retries": 3},
)
def devices_loader_concurrent():
    @task
    def extract() -> str:
        ctx = get_current_context()
        # Run-scoped location: concurrent runs never touch each other's files.
        return f"/tmp/dp/devices_raw/{ctx['ts_nodash']}"

    @task
    def load(location: str):
        ctx = get_current_context()
        print(f"overwrite partition hour={ctx['ts_nodash']} from {location}")

    load(extract())


devices_loader_concurrent()
