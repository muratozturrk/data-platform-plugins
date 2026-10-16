"""Schema Compatibility Enforcer - deployment gate + DDL event trigger, Airflow 2.x.

Two enforcement modes in one DAG (trigger manually, e.g. from CI with a conf payload):
  * external service: test the candidate Avro schema against Schema Registry; fail the
    run if incompatible, register only when it passes (auto.register.schemas stays off
    in producers);
  * event-driven DDL: install a PostgreSQL event trigger that aborts DROP/RENAME COLUMN
    and column retypes on the dp schema, so table consumers are protected the same way.

Allowlist for approved breaking DDL (the Schema Migrator's contract step, run after the
grace period). The guard lets it through only when BOTH hold:
  * the transaction ran `SET LOCAL dp.schema_migration = 'contract'`, and
  * current_user is a member of role `dp_schema_migrator` (anyone can set a custom
    GUC, so the role membership is the real allowlist; grant it only to the migrator).
See examples/schema_migrator/airflow/schema_migrator_dag.py, task drop_old_column.

Run: put in dags/; trigger with conf {"subject": "visits-value", "schema": "<avro json>"}.
Needs `requests`, apache-airflow-providers-common-sql/postgres, connection `dp_postgres`,
env SCHEMA_REGISTRY_URL.
"""
import os

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
HEADERS = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

DDL_GUARD = r"""
CREATE OR REPLACE FUNCTION dp_block_breaking_ddl() RETURNS event_trigger AS $$
DECLARE cmd record;
BEGIN
  FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    IF cmd.schema_name = 'dp' AND cmd.command_tag = 'ALTER TABLE'
       AND current_query() ~* '(DROP\s+COLUMN|RENAME\s+COLUMN|ALTER\s+COLUMN\s+\S+\s+TYPE)' THEN
      -- allowlist: Schema Migrator contract step (SET LOCAL + dedicated role)
      IF COALESCE(current_setting('dp.schema_migration', true), '') = 'contract'
         AND pg_has_role(current_user, 'dp_schema_migrator', 'MEMBER') THEN
        RAISE NOTICE 'Approved schema migration on %', cmd.object_identity;
        CONTINUE;
      END IF;
      RAISE EXCEPTION 'Breaking DDL blocked on %: use the Schema Migrator procedure', cmd.object_identity;
    END IF;
  END LOOP;
END $$ LANGUAGE plpgsql;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dp_schema_migrator') THEN
    CREATE ROLE dp_schema_migrator NOLOGIN;  -- GRANT dp_schema_migrator TO <migrator login>
  END IF;
END $$;
DROP EVENT TRIGGER IF EXISTS dp_schema_guard;
CREATE EVENT TRIGGER dp_schema_guard ON ddl_command_end EXECUTE FUNCTION dp_block_breaking_ddl();
"""


@dag(schedule=None, start_date=pendulum.datetime(2024, 1, 1), catchup=False,
     tags=["data-quality", "schema"])
def schema_compatibility_gate():
    @task
    def check_compatibility(dag_run=None) -> dict:
        import requests

        conf = dag_run.conf or {}
        subject, schema = conf["subject"], conf["schema"]
        resp = requests.post(f"{REGISTRY}/compatibility/subjects/{subject}/versions/latest?verbose=true",
                             json={"schema": schema}, headers=HEADERS, timeout=30)
        if resp.status_code == 404:  # first version of the subject: nothing to break
            return {"subject": subject, "schema": schema}
        resp.raise_for_status()
        body = resp.json()
        if not body.get("is_compatible"):
            raise AirflowFailException(f"Incompatible schema for {subject}: {body.get('messages')}")
        return {"subject": subject, "schema": schema}

    @task
    def register(candidate: dict) -> int:
        import requests

        resp = requests.post(f"{REGISTRY}/subjects/{candidate['subject']}/versions",
                             json={"schema": candidate["schema"]}, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.json()["id"]

    install_ddl_guard = SQLExecuteQueryOperator(task_id="install_ddl_guard", conn_id="dp_postgres",
                                                sql=DDL_GUARD)
    install_ddl_guard >> register(check_compatibility())


schema_compatibility_gate()
