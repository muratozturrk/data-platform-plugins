"""Dataset Tracker - OpenLineage in Airflow 2.x.

* Built-in operators with an OpenLineage extractor (SQLExecuteQueryOperator,
  PostgresOperator, ...) report inputs/outputs automatically: the provider parses
  the SQL and emits lineage events, no code needed.
* Custom tasks (@task / PythonOperator) are opaque: declare `inlets` / `outlets`
  manually, otherwise they are invisible in the lineage graph ("custom work" gotcha).

Setup:
    pip install apache-airflow-providers-openlineage apache-airflow-providers-common-sql
    export AIRFLOW__OPENLINEAGE__TRANSPORT='{"type": "http", "url": "http://localhost:5000"}'
    export AIRFLOW__OPENLINEAGE__NAMESPACE=orders_team
Deps: apache-airflow>=2.7
"""
import pendulum
from airflow.decorators import dag, task
from airflow.lineage.entities import Table
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CLUSTER = "dp_postgres"  # appears as the dataset namespace


@dag(schedule="@daily", start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False,
     tags=["lineage"])
def orders_with_users_lineage():

    # Automatic: the SQL parser extracts dp.orders + dp.users -> dp.orders_with_users.
    build_table = SQLExecuteQueryOperator(
        task_id="build_orders_with_users", conn_id="dp_postgres",
        sql="""INSERT INTO dp.orders_with_users (order_id, amount, first_name)
               SELECT o.order_id, o.amount, u.first_name
               FROM dp.orders o JOIN dp.users u ON u.id = o.user_id
               WHERE o.order_date = '{{ ds }}'""")

    # Manual: Python code is opaque for extractors, so declare its datasets.
    @task(inlets=[Table(cluster=CLUSTER, database="dp", name="orders_with_users")],
          outlets=[Table(cluster="s3://exports", database="exports", name="orders_with_users_csv")])
    def export_to_partner() -> None:
        ...  # read the table, write a CSV to the partner bucket

    build_table >> export_to_partner()


orders_with_users_lineage()
