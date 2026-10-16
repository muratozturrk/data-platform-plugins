"""Schema Migrator - PostgreSQL column rename done safely, Airflow 2.x TaskFlow.

Daily DAG for the `from_page` -> `referral` migration:
  1. add_new_column     : ADD COLUMN referral (idempotent), mark from_page deprecated
  2. backfill / sync    : keep referral = from_page for rows written by old producers
  3. deadline branch    : before the deadline -> skip; after -> drop from_page
Consumers move their queries to `referral` during the grace period.

The contract step (DROP COLUMN) is breaking DDL. It passes the event-trigger guard from
examples/schema_compatibility_enforcer because it runs `SET LOCAL
dp.schema_migration = 'contract'` in the same transaction, through a dedicated
connection whose user is a member of role `dp_schema_migrator`. The expand and sync
steps are additive, so they use the regular connection and need no approval.

Run: put in dags/. Needs apache-airflow-providers-common-sql/postgres, connections
`dp_postgres` and `dp_postgres_migrator` (user granted dp_schema_migrator),
Airflow Variable `referral_migration_deadline` (YYYY-MM-DD).
"""
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CONN = "dp_postgres"
MIGRATOR_CONN = "dp_postgres_migrator"  # only this user may run approved breaking DDL


@dag(schedule="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False,
     max_active_runs=1, tags=["data-quality", "schema-migration"])
def schema_migrator_referral():
    add_new_column = SQLExecuteQueryOperator(task_id="add_new_column", conn_id=CONN, sql="""
        ALTER TABLE dp.visits ADD COLUMN IF NOT EXISTS referral VARCHAR(25);
    """)
    # guarded: once from_page is dropped this becomes a no-op instead of failing every run
    sync_new_column = SQLExecuteQueryOperator(task_id="sync_new_column", conn_id=CONN, sql="""
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'dp'
                     AND table_name = 'visits' AND column_name = 'from_page') THEN
            COMMENT ON COLUMN dp.visits.from_page IS 'DEPRECATED: use referral';
            UPDATE dp.visits SET referral = from_page
            WHERE referral IS DISTINCT FROM from_page AND from_page IS NOT NULL;
          END IF;
        END $$;
    """)

    @task.branch
    def grace_period_over(ds=None) -> str:
        deadline = Variable.get("referral_migration_deadline", default_var="2099-01-01")
        return "drop_old_column" if ds > deadline else "grace_period_continues"

    @task
    def grace_period_continues():
        print("Old and new columns both available; consumers still migrating.")

    # One execute() inside one transaction (autocommit off): SET LOCAL scopes the approval
    # to this DROP and expires at COMMIT, so nothing else in the session inherits it.
    drop_old_column = SQLExecuteQueryOperator(task_id="drop_old_column", conn_id=MIGRATOR_CONN,
                                              autocommit=False, sql="""
        SET LOCAL dp.schema_migration = 'contract';
        ALTER TABLE dp.visits DROP COLUMN IF EXISTS from_page;
    """)

    add_new_column >> sync_new_column >> grace_period_over() >> [drop_old_column, grace_period_continues()]


schema_migrator_referral()
