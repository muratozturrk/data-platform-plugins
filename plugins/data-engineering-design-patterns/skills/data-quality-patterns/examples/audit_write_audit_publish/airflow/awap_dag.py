"""Audit-Write-Audit-Publish (AWAP) - batch pipeline, Airflow 2.x TaskFlow.

Flow: wait for input -> audit input (cheap: size, lines, JSON validity) -> transform
JSON lines to CSV in a staging dir -> audit output (NULLs in required columns) ->
publish to PostgreSQL with COPY (delete+insert for idempotency).

Run: put in your dags/ folder. Needs apache-airflow-providers-postgres and pandas,
a Postgres connection id `dp_postgres` and env INPUT_DIR / STAGING_DIR.
Input layout: $INPUT_DIR/<ds>/visits.json (JSON lines).
"""
import csv
import json
import os
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

INPUT_DIR = os.environ.get("INPUT_DIR", "/tmp/dp/input")
STAGING_DIR = os.environ.get("STAGING_DIR", "/tmp/dp/staging")
REQUIRED_COLUMNS = ["visit_id", "event_time", "user_id", "page", "ip", "login",
                    "browser", "browser_version", "network_type", "device_type", "device_version"]
MIN_SIZE_BYTES = 1024
MIN_LINES = 100


def audit_input_file(path: str, min_size: int = MIN_SIZE_BYTES, min_lines: int = MIN_LINES) -> None:
    """Input audit: metadata + syntactic checks. Collects ALL errors before failing."""
    errors = []
    f_size = os.path.getsize(path)
    if f_size < min_size:
        errors.append(f"File is too small. Expected at least {min_size} bytes but got {f_size}")
    lines, first_invalid = 0, None
    with open(path, encoding="utf-8") as f:
        for lines, line in enumerate(f, start=1):
            try:
                json.loads(line)
            except json.JSONDecodeError:
                first_invalid = first_invalid or lines
    if lines < min_lines:
        errors.append(f"File is too short. Expected at least {min_lines} lines but got {lines}")
    if first_invalid:
        errors.append(f"File contains invalid JSON lines. First error at line {first_invalid}")
    if errors:
        raise ValueError("Audit failed for the input file:\n-" + "\n-".join(errors))


def audit_output_file(path: str) -> None:
    """Output audit: data checks on the transformed dataset (CSV has no constraints)."""
    import pandas

    visits = pandas.read_csv(path, sep=";", header=0)
    cols_with_nulls = [c for c in REQUIRED_COLUMNS if visits[c].isnull().any()]
    if cols_with_nulls:
        raise ValueError("Found NULLs in not nullable columns: " + ",".join(cols_with_nulls))
    if visits["visit_id"].duplicated().any():
        raise ValueError("Duplicated visit_id values in transformed dataset")


@dag(schedule="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False,
     max_active_runs=1, tags=["data-quality", "awap"])
def awap_visits():
    wait_for_input = FileSensor(task_id="wait_for_input", filepath=f"{INPUT_DIR}/{{{{ ds }}}}/visits.json",
                                poke_interval=60, mode="reschedule")

    @task
    def audit_file_to_load(ds=None) -> str:
        path = f"{INPUT_DIR}/{ds}/visits.json"
        audit_input_file(path)
        return path

    @task
    def transform_file(input_path: str, ds=None) -> str:
        out = Path(STAGING_DIR) / ds / "visits.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(input_path, encoding="utf-8") as src, open(out, "w", newline="", encoding="utf-8") as dst:
            writer = csv.DictWriter(dst, fieldnames=REQUIRED_COLUMNS, delimiter=";")
            writer.writeheader()
            for line in src:
                v = json.loads(line)
                tech, user = v.get("context", {}).get("technical", {}), v.get("context", {}).get("user", {})
                writer.writerow({
                    "visit_id": v.get("visit_id"), "event_time": v.get("event_time"),
                    "user_id": v.get("user_id"), "page": v.get("page"), "ip": user.get("ip"),
                    "login": user.get("login"), "browser": tech.get("browser"),
                    "browser_version": tech.get("browser_version"), "network_type": tech.get("network_type"),
                    "device_type": tech.get("device_type"), "device_version": tech.get("device_version"),
                })
        return str(out)

    @task
    def audit_transformed_file(staged_path: str) -> str:
        audit_output_file(staged_path)
        return staged_path

    @task
    def publish(staged_path: str, ds=None) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="dp_postgres")
        conn = hook.get_conn()
        with conn, conn.cursor() as cur, open(staged_path, encoding="utf-8") as f:
            # single transaction: replace the day atomically -> rerun safe
            cur.execute("DELETE FROM dp.visits WHERE event_time::date = %s", (ds,))
            cur.copy_expert("COPY dp.visits FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER true)", f)

    audited = audit_file_to_load()
    wait_for_input >> audited
    publish(audit_transformed_file(transform_file(audited)))


awap_visits()
