"""Flow Interruption Detector - batch/data-at-rest check in Airflow 2.x (TaskFlow).

Two detection strategies for a PostgreSQL table:

1. metadata layer: last commit timestamp (requires `track_commit_timestamp = on`
   in postgresql.conf).
2. data layer fallback: row count compared to the count stored by the previous run
   (Airflow Variable) - for stores without commit/modification metadata.

The task fails (-> on_failure_callback / email alert) when the flow is interrupted.

Run:  drop into $AIRFLOW_HOME/dags, create connection `dp_postgres`.
Deps: apache-airflow>=2.6, apache-airflow-providers-postgres
"""
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

TABLE = "dp.visits_flattened"
MAX_SECONDS_WITHOUT_COMMIT = 3600  # hourly ingestion -> 1h threshold
COUNT_VARIABLE = "flow_detector_visits_flattened_count"


def notify(context) -> None:
    # ponytail: log only; wire Slack/PagerDuty notifier here.
    print(f"Flow interruption detected: {context['exception']}")


@dag(schedule="@hourly", start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False,
     default_args={"on_failure_callback": notify, "retries": 0}, tags=["observability"])
def visits_flow_interruption_detector():

    @task
    def check_last_commit_time() -> int:
        hook = PostgresHook(postgres_conn_id="dp_postgres")
        seconds_since_commit = hook.get_first(f"""
            SELECT CAST(EXTRACT(EPOCH FROM NOW() - MAX(pg_xact_commit_timestamp(xmin))) AS INT)
            FROM {TABLE}""")[0]
        if seconds_since_commit is None or seconds_since_commit > MAX_SECONDS_WITHOUT_COMMIT:
            raise AirflowFailException(f"{TABLE}: no commit for {seconds_since_commit}s")
        return seconds_since_commit

    @task
    def check_row_count_progress() -> int:
        hook = PostgresHook(postgres_conn_id="dp_postgres")
        current = hook.get_first(f"SELECT COUNT(*) FROM {TABLE}")[0]
        previous = int(Variable.get(COUNT_VARIABLE, default_var=-1))
        if current <= previous:
            raise AirflowFailException(f"{TABLE}: row count unchanged ({previous} -> {current})")
        Variable.set(COUNT_VARIABLE, current)
        return current

    check_last_commit_time()
    check_row_count_progress()


visits_flow_interruption_detector()
