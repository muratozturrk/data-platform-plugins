"""Secrets Pointer - Airflow 2.x TaskFlow DAG resolving Connections/Variables from a secrets backend.

The DAG only references `conn_id`s and Variable keys. Airflow resolves them
through the configured secrets backend (AWS Secrets Manager here) before
looking at env vars / the metadata DB, so no credential lives in the DAG,
the repo, or the Airflow DB.

airflow.cfg (or env AIRFLOW__SECRETS__BACKEND / AIRFLOW__SECRETS__BACKEND_KWARGS):
  [secrets]
  backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
  backend_kwargs = {"connections_prefix": "airflow/connections",
                    "variables_prefix": "airflow/variables"}
Secrets:
  airflow/connections/dp_postgres   -> postgresql://user:pass@db:5432/dp (or JSON)
  airflow/variables/geo_api_key       -> <api key>
Airflow masks values of sensitive fields in logs, but only those it knows about:
don't print derived strings that contain the secret.

Run: pip install apache-airflow-providers-amazon apache-airflow-providers-postgres requests;
     worker role needs secretsmanager:GetSecretValue on arn:...:secret:airflow/*.
"""
from datetime import datetime

from airflow.decorators import dag, task

GEO_API_URL = "https://geo.example.com/v1/lookup"


@dag(schedule="@hourly", start_date=datetime(2024, 1, 1), catchup=False, tags=["security", "secrets"])
def secrets_pointer_enrichment():

    @task
    def extract_ips(data_interval_start=None, data_interval_end=None) -> list:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="dp_postgres")  # pointer, resolved by the backend
        rows = hook.get_records(
            "SELECT DISTINCT ip FROM dp.visits WHERE event_time >= %s AND event_time < %s",
            parameters=(data_interval_start, data_interval_end))
        return [r[0] for r in rows]

    @task
    def enrich(ips: list) -> list:
        import requests
        from airflow.models import Variable
        api_key = Variable.get("geo_api_key")  # pointer -> airflow/variables/geo_api_key
        session = requests.Session()
        session.headers["Authorization"] = f"Bearer {api_key}"
        results = []
        for ip in ips:
            response = session.get(GEO_API_URL, params={"ip": ip}, timeout=10)
            response.raise_for_status()
            results.append((ip, response.json().get("country")))
        return results

    @task
    def load(results: list) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="dp_postgres")
        hook.insert_rows("dp.ip_geolocation", results, target_fields=["ip", "country"],
                         replace=True, replace_index="ip")

    load(enrich(extract_ips()))


secrets_pointer_enrichment()
