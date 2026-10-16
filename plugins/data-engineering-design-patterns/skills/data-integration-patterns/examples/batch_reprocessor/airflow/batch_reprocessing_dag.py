"""Batch Reprocessor - on-demand reprocessing run in the orchestration layer (Airflow 2.x).

Combined CI/CD + orchestration variant: CI/CD (e.g. Terraform) provisions the
reprocessing compute, then triggers this DAG through the REST API (External Trigger):

    POST /api/v1/dags/visits_batch_reprocessor/dagRuns
    {"conf": {"mode": "sessions", "start": "2024-06-01 10:00:00", "end": "2024-06-01 11:00:00"}}

The run history (who reprocessed what range, when) stays in Airflow even after the
infrastructure is destroyed.

Deps: apache-airflow>=2.6, apache-airflow-providers-apache-spark
"""
import os

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_DIR = os.path.join(os.path.dirname(__file__), "..", "spark")


@dag(schedule=None, start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False, max_active_runs=1,
     params={"mode": Param("stateless", enum=["stateless", "sessions"]),
             "start": Param("0", type="string"), "end": Param("0", type="string")},
     tags=["reprocessing"])
def visits_batch_reprocessor():
    reprocess = SparkSubmitOperator(
        task_id="reprocess", conn_id="spark_default",
        application=f"{SPARK_DIR}/batch_reprocessor_job.py", py_files=f"{SPARK_DIR}/visits_logic.py",
        packages="io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        application_args=["{{ params.mode }}", "--start", "{{ params.start }}", "--end", "{{ params.end }}"])

    @task
    def announce(params=None) -> None:
        # ponytail: print only; notify consumers that origin=reprocessing records are coming.
        print(f"Reprocessed {params['mode']} range {params['start']} -> {params['end']}")

    reprocess >> announce()


visits_batch_reprocessor()
