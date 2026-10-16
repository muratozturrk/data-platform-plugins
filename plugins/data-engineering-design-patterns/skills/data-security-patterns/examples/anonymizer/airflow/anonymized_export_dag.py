"""Anonymizer - Airflow 2.x TaskFlow DAG exporting an anonymized dataset to a partner.

  extract users (private zone) -> anonymize (drop PII, generalize, synthesize)
  -> validate no PII column/value leaks -> publish to the partner share
The validation task is the safety net: it fails the run if a forbidden column
or an original email value is present in the output.

Run: put in dags/; pip install pandas faker apache-airflow-providers-postgres;
     connection `dp_postgres`; env EXPORT_DIR (partner share mount).
"""
import os
from datetime import datetime

from airflow.decorators import dag, task

EXPORT_DIR = os.environ.get("EXPORT_DIR", "/tmp/dp/partner_share")
STAGING_DIR = os.environ.get("STAGING_DIR", "/tmp/dp/anonymizer_staging")
FORBIDDEN = {"birthday", "personal_id", "full_name", "phone"}


@dag(schedule="@weekly", start_date=datetime(2024, 1, 1), catchup=False, tags=["security", "sharing"])
def anonymized_partner_export():

    @task
    def extract(ds=None) -> str:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        df = PostgresHook(postgres_conn_id="dp_postgres").get_pandas_df(
            "SELECT user_id, email, full_name, birthday, country, city, registered_at FROM dp.users")
        os.makedirs(STAGING_DIR, exist_ok=True)
        path = f"{STAGING_DIR}/raw_{ds}.parquet"
        df.to_parquet(path)
        return path

    @task
    def anonymize(raw_path: str, ds=None) -> str:
        import pandas as pd
        from faker import Faker
        faker = Faker()
        df = pd.read_parquet(raw_path)
        df = df.drop(columns=[c for c in FORBIDDEN if c in df.columns])  # removal
        df = df.drop(columns=["user_id", "city"])  # identifiers / quasi-identifiers
        df["email"] = [faker.email() for _ in range(len(df))]  # synthetic replacement
        df["registered_at"] = pd.to_datetime(df["registered_at"]).dt.to_period("M").astype(str)  # coarsen
        path = f"{STAGING_DIR}/anonymized_{ds}.parquet"
        df.to_parquet(path)
        return path

    @task
    def validate(raw_path: str, anonymized_path: str) -> str:
        import pandas as pd
        raw, out = pd.read_parquet(raw_path), pd.read_parquet(anonymized_path)
        leaked_columns = FORBIDDEN & set(out.columns)
        if leaked_columns:
            raise ValueError(f"PII columns in export: {leaked_columns}")
        if out["email"].isin(raw["email"]).any():
            raise ValueError("original emails found in export")
        return anonymized_path

    @task
    def publish(path: str, ds=None) -> None:
        import shutil
        os.makedirs(EXPORT_DIR, exist_ok=True)
        shutil.copyfile(path, f"{EXPORT_DIR}/users_{ds}.parquet")
        shutil.rmtree(STAGING_DIR)  # don't leave raw PII behind in staging

    raw = extract()
    publish(validate(raw, anonymize(raw)))


anonymized_partner_export()
