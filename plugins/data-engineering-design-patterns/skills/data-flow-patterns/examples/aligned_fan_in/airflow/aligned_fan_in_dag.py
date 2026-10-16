"""Aligned Fan-In -- Apache Airflow.

24 hourly branches (wait for hour partition -> load it) all merge into one child task that
computes the daily trends. The child uses the default trigger_rule=all_success: it runs only
when EVERY parent succeeded. A failed hour is fixed and replayed alone
(`airflow tasks clear -t load_hourly_visits_13 ...`), the 23 others are not recomputed.

Concurrency guard: `max_active_tasks` caps the 24-wide spike for non-elastic infrastructure.

Run: set env INPUT_DIR and the `dp_postgres` connection, then
     `airflow dags test visits_aligned_fan_in 2024-01-01`.
Requires: apache-airflow>=2.6, apache-airflow-providers-common-sql, -postgres.
"""
import os

import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor

INPUT_DIR = os.environ.get("INPUT_DIR", "/tmp/dp/visits")
CONN_ID = "dp_postgres"


@dag(
    dag_id="visits_aligned_fan_in",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_tasks=8,  # balance feedback-loop speed vs infrastructure spike
)
def visits_aligned_fan_in():
    clear_context = SQLExecuteQueryOperator(
        task_id="clear_context",
        conn_id=CONN_ID,
        sql="DELETE FROM dp.visits_raw WHERE execution_time_id = '{{ ds }}';",
    )

    generate_trends = SQLExecuteQueryOperator(
        task_id="generate_trends",
        conn_id=CONN_ID,
        # trigger_rule defaults to all_success -> the "aligned" part of the pattern
        sql="""
            DELETE FROM dp.visits_trends WHERE day = '{{ ds }}';
            INSERT INTO dp.visits_trends (day, page, visits)
            SELECT '{{ ds }}', page, COUNT(*) FROM dp.visits_raw
            WHERE execution_time_id = '{{ ds }}' GROUP BY page;
        """,
    )

    for hour_to_load in [f"{hour:02d}" for hour in range(24)]:
        file_sensor = FileSensor(
            task_id=f"wait_for_{hour_to_load}",
            filepath=INPUT_DIR + "/date={{ ds_nodash }}/hour=" + hour_to_load + "/dataset.csv",
            mode="reschedule",
            timeout=3 * 3600,
        )
        visits_loader = SQLExecuteQueryOperator(
            task_id=f"load_hourly_visits_{hour_to_load}",
            conn_id=CONN_ID,
            params={"hour": hour_to_load},
            sql="""
                DELETE FROM dp.visits_raw WHERE execution_time_id = '{{ ds }}'
                    AND execution_time_hour_id = '{{ params.hour }}';
                COPY dp.visits_raw FROM '""" + INPUT_DIR + """/date={{ ds_nodash }}/hour={{ params.hour }}/dataset.csv'
                    CSV HEADER;
            """,
        )
        clear_context >> file_sensor >> visits_loader >> generate_trends


visits_aligned_fan_in()
