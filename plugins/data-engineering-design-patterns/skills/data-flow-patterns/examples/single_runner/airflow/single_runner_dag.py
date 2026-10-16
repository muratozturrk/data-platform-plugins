"""Single Runner -- Apache Airflow.

Incremental pipeline whose run N needs the output of run N-1 (today's trend = today vs
yesterday; sessionization state). Two settings enforce the sequential execution:
  * max_active_runs=1      -> at most one DAG run at a time (also during backfills/catchup);
  * depends_on_past=True   -> a task waits for the SAME task of the previous run to succeed,
                              so a failure halts the chain instead of computing on bad state.
`wait_for_downstream=True` additionally waits for the previous run's direct downstream tasks.

Backfill cost: runs are strictly serial -> backfill only the steps you need (e.g. re-run
the load task alone after a target change with `airflow tasks clear -t load_trends`).

Run: `airflow dags test visits_trend_generator 2024-01-02`.
Requires: apache-airflow>=2.6.
"""
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
    dag_id="visits_trend_generator",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "depends_on_past": True,
        "wait_for_downstream": True,
        "retries": 2,
    },
)
def visits_trend_generator():
    @task
    def compute_daily_visits() -> str:
        ds = get_current_context()["ds"]
        print(f"aggregating visits for {ds}")
        return f"/tmp/dp/visits_daily/{ds}"

    @task
    def compare_with_previous_day(current_path: str) -> str:
        ctx = get_current_context()
        previous = ctx["prev_ds"]  # previous logical date (schedule-based)
        print(f"diffing {current_path} with /tmp/dp/visits_daily/{previous}")
        return f"/tmp/dp/visits_trends/{ctx['ds']}"

    @task
    def load_trends(trends_path: str):
        print(f"loading {trends_path} into the serving table")

    load_trends(compare_with_previous_day(compute_daily_visits()))


visits_trend_generator()
