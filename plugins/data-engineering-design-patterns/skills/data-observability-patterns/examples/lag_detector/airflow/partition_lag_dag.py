"""Lag Detector - time-partitioned store lag checked from Airflow 2.x.

Lag unit = partition timestamp. The producer creates hourly folders
`{DATA_DIR}/hour=YYYY-MM-DDTHH`; the consumer job records the last hour it processed
in the Airflow Variable `visits_consumer_last_hour`. Lag = newest available partition
- last processed partition, in hours. Runs every 15 minutes, fails above threshold.

Run:  copy to $AIRFLOW_HOME/dags; set VISITS_DATA_DIR.
Deps: apache-airflow>=2.6
"""
import os

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

DATA_DIR = os.environ.get("VISITS_DATA_DIR", "/opt/airflow/data/visits")
MAX_LAG_HOURS = 2


def newest_partition_hour() -> pendulum.DateTime:
    hours = [d.split("=", 1)[1] for d in os.listdir(DATA_DIR) if d.startswith("hour=")]
    if not hours:
        raise AirflowFailException(f"No partitions in {DATA_DIR}")
    return pendulum.parse(max(hours))  # ISO names sort chronologically


@dag(schedule="*/15 * * * *", start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False,
     tags=["observability"])
def visits_consumer_lag_detector():

    @task
    def compute_lag() -> float:
        last_available = newest_partition_hour()
        last_processed = pendulum.parse(Variable.get("visits_consumer_last_hour"))
        lag_hours = (last_available - last_processed).total_hours()
        print(f"available={last_available} processed={last_processed} lag={lag_hours}h")
        if lag_hours > MAX_LAG_HOURS:
            raise AirflowFailException(f"Consumer lag {lag_hours}h > {MAX_LAG_HOURS}h")
        return lag_hours

    compute_lag()


visits_consumer_lag_detector()
