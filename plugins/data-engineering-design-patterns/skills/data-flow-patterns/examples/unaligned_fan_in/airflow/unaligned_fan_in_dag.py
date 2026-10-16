"""Unaligned Fan-In -- Apache Airflow.

Same 24-branch shape as the Aligned Fan-In, but the child task runs with
trigger_rule=ALL_DONE: it starts once every parent finished, whatever the outcome, and
produces a possibly PARTIAL cube flagged with `is_approximate` + a companion completeness
row so consumers know how complete the day is.

Readability: the trigger rule is invisible in the graph view, so it is documented in the
task's doc_md. Failures are handled with `on_failure_callback` (alerting) instead of extra
"on failure" branches that would make the graph confusing.

Run: set env INPUT_DIR and the `dp_postgres` connection, then
     `airflow dags test visits_unaligned_fan_in 2024-01-01`.
Requires: apache-airflow>=2.6, apache-airflow-providers-common-sql, -postgres.
"""
import os

import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

INPUT_DIR = os.environ.get("INPUT_DIR", "/tmp/dp/visits")
CONN_ID = "dp_postgres"


def alert_on_failure(context):
    # Replace with Slack/email; keeps failure handling out of the graph.
    ti = context["task_instance"]
    print(f"[ALERT] {ti.task_id} failed for {context['ds']} -- cube will be approximate")


@dag(
    dag_id="visits_unaligned_fan_in",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_tasks=8,
)
def visits_unaligned_fan_in():
    clear_context = SQLExecuteQueryOperator(
        task_id="clear_context",
        conn_id=CONN_ID,
        sql="DELETE FROM dp.visits_raw WHERE execution_time_id = '{{ ds }}';",
    )

    generate_cube = SQLExecuteQueryOperator(
        task_id="generate_cube",
        conn_id=CONN_ID,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Runs with **ALL_DONE**: builds a partial cube when some hours failed.",
        sql="""
            DELETE FROM dp.visits_cube WHERE day = '{{ ds }}';
            INSERT INTO dp.visits_cube (day, page, device_type, visits, is_approximate)
            SELECT '{{ ds }}', page, device_type, COUNT(*),
              (SELECT CASE WHEN hours_subquery.all_hours = 24 THEN false ELSE true END FROM
                (SELECT COUNT(DISTINCT execution_time_hour_id) AS all_hours FROM dp.visits_raw
                 WHERE execution_time_id = '{{ ds }}') AS hours_subquery)
            FROM dp.visits_raw WHERE execution_time_id = '{{ ds }}'
            GROUP BY CUBE(page, device_type);

            -- companion completeness table: 12/24 hours -> 50%
            DELETE FROM dp.visits_cube_completeness WHERE day = '{{ ds }}';
            INSERT INTO dp.visits_cube_completeness (day, completeness_pct)
            SELECT '{{ ds }}', COUNT(DISTINCT execution_time_hour_id) * 100.0 / 24
            FROM dp.visits_raw WHERE execution_time_id = '{{ ds }}';
        """,
    )

    for hour_to_load in [f"{hour:02d}" for hour in range(24)]:
        file_sensor = FileSensor(
            task_id=f"wait_for_{hour_to_load}",
            filepath=INPUT_DIR + "/date={{ ds_nodash }}/hour=" + hour_to_load + "/dataset.csv",
            mode="reschedule",
            timeout=3 * 3600,
            on_failure_callback=alert_on_failure,
        )
        visits_loader = SQLExecuteQueryOperator(
            task_id=f"load_hourly_visits_{hour_to_load}",
            conn_id=CONN_ID,
            params={"hour": hour_to_load},
            on_failure_callback=alert_on_failure,
            sql="""
                -- idempotent retry: a retried hour must not append duplicates
                DELETE FROM dp.visits_raw WHERE execution_time_id = '{{ ds }}'
                    AND execution_time_hour_id = '{{ params.hour }}';
                COPY dp.visits_raw FROM '""" + INPUT_DIR + """/date={{ ds_nodash }}/hour={{ params.hour }}/dataset.csv'
                    CSV HEADER;
            """,
        )
        clear_context >> file_sensor >> visits_loader >> generate_cube


visits_unaligned_fan_in()
