"""Fine-Grained Tracker - column-level lineage from SQL in Airflow 2.x.

Custom column lineage: parse the SQL a task executes and resolve, for each output
column, the upstream table.column it is built from. Uses
sqlglot's lineage module, which walks the query tree like a lineage framework does.
The map is stored as a JSON document next to the table (one per DAG run), so the
history of column definitions is kept ("evolution management" gotcha).

Native alternative: the OpenLineage provider already emits a `columnLineage` facet
for SQL operators - prefer it when it covers your operators.

Run:  copy to $AIRFLOW_HOME/dags; pip install sqlglot
Deps: apache-airflow>=2.6, sqlglot>=20
"""
import json
import os

import pendulum
from airflow.decorators import dag, task

LINEAGE_DIR = os.environ.get("COLUMN_LINEAGE_DIR", "/opt/airflow/data/column_lineage")
QUERY = """
SELECT CONCAT(u.first_name, d.delivery_address) AS user_with_address, u.id AS user_id
FROM users u JOIN addresses d ON d.user_id = u.id
"""


def column_sources(sql: str) -> dict[str, list[str]]:
    import sqlglot
    from sqlglot.lineage import lineage

    output_columns = [e.alias_or_name for e in sqlglot.parse_one(sql).expressions]
    result = {}
    for column in output_columns:
        leaves = [n for n in lineage(column, sql).walk() if not n.downstream]
        # leaf names look like "u.first_name"; the leaf source is the Table, so resolve the alias
        result[column] = sorted({f"{leaf.source.name}.{leaf.name.split('.')[-1]}" for leaf in leaves})
    return result


@dag(schedule="@daily", start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), catchup=False,
     tags=["lineage"])
def users_with_address_column_lineage():

    @task
    def extract_column_lineage(ds=None) -> dict[str, list[str]]:
        lineage_map = column_sources(QUERY)
        os.makedirs(LINEAGE_DIR, exist_ok=True)
        with open(f"{LINEAGE_DIR}/users_with_address_{ds}.json", "w") as f:
            json.dump({"table": "users_with_address", "date": ds, "columns": lineage_map}, f, indent=2)
        return lineage_map  # e.g. user_with_address -> [addresses.delivery_address, users.first_name]

    extract_column_lineage()


users_with_address_column_lineage()
