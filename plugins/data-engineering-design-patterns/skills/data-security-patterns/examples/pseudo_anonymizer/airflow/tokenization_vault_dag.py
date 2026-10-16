"""Pseudo-Anonymizer (tokenization) - Airflow 2.x TaskFlow DAG with a PostgreSQL token vault.

Tokenization replaces each email with a random token and stores the mapping
in a VAULT table (`vault.email_tokens`) living in a separate, tightly
restricted schema/database. Shared datasets only contain tokens; authorized
processes can detokenize via the vault (e.g. for a support case). If the vault
is compromised, every token is reversible - protect it like a secret store.

  ensure_vault -> tokenize new emails (idempotent upsert) -> publish tokenized view

Run: put in dags/; pip install apache-airflow-providers-postgres; connections
  `dp_vault`    (only this DAG's role may read vault.email_tokens)
  `dp_postgres` (source dp.users, target dp.users_tokenized)
"""
from datetime import datetime

from airflow.decorators import dag, task


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False, tags=["security", "tokenization"])
def tokenization_vault():

    @task
    def ensure_vault() -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        PostgresHook(postgres_conn_id="dp_vault").run([
            "CREATE EXTENSION IF NOT EXISTS pgcrypto",
            "CREATE SCHEMA IF NOT EXISTS vault",
            """CREATE TABLE IF NOT EXISTS vault.email_tokens (
                 email TEXT PRIMARY KEY,
                 token TEXT NOT NULL UNIQUE DEFAULT encode(gen_random_bytes(16), 'hex'),
                 created_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
            "REVOKE ALL ON SCHEMA vault FROM PUBLIC",
        ])

    @task
    def tokenize_new_emails() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        source = PostgresHook(postgres_conn_id="dp_postgres")
        vault = PostgresHook(postgres_conn_id="dp_vault")
        emails = [row[0] for row in source.get_records("SELECT DISTINCT email FROM dp.users WHERE email IS NOT NULL")]
        if not emails:
            return 0
        # random token, NOT derived from the value -> no brute force possible without the vault
        with vault.get_conn() as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO vault.email_tokens (email) VALUES (%s) ON CONFLICT (email) DO NOTHING",
                [(e,) for e in emails])
        return len(emails)

    @task
    def publish_tokenized(_: int) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        vault = PostgresHook(postgres_conn_id="dp_vault")
        source = PostgresHook(postgres_conn_id="dp_postgres")
        mapping = dict(vault.get_records("SELECT email, token FROM vault.email_tokens"))
        # ponytail: mapping loaded into the worker; for millions of rows join inside the DB (same cluster) instead
        rows = source.get_records("SELECT user_id, email, country FROM dp.users")
        tokenized = [(user_id, mapping.get(email), country) for user_id, email, country in rows]
        source.run(["CREATE TABLE IF NOT EXISTS dp.users_tokenized (user_id TEXT, email_token TEXT, country TEXT)",
                    "TRUNCATE dp.users_tokenized"])
        source.insert_rows("dp.users_tokenized", tokenized, target_fields=["user_id", "email_token", "country"])

    publish_tokenized(tokenize_new_emails()) << ensure_vault()


tokenization_vault()
