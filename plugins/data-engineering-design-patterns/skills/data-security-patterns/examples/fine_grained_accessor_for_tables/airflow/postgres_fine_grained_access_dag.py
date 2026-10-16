"""Fine-Grained Accessor for Tables - Airflow 2.x TaskFlow DAG applying PostgreSQL policies.

Access policies as code, re-applied idempotently on every run:
  * column-level: GRANT SELECT(<listed columns>) - `SELECT *` by user_a fails with
    "permission denied for table users"
  * row-level   : ENABLE ROW LEVEL SECURITY + CREATE POLICY ... USING (login = current_user)
  * fallback    : a guard view for engines without native row filters
Note: RLS does not apply to the table owner / superusers (use FORCE ROW LEVEL SECURITY
for owners) and policies can only use session attributes (current_user, roles, settings).

Run: put in dags/; create Airflow connection `dp_postgres_admin` (owner of dp.users);
     pip install apache-airflow-providers-postgres. Roles listed in READERS must exist.
"""
from datetime import datetime

from airflow.decorators import dag, task

CONN_ID = "dp_postgres_admin"
# role -> columns it may read; keep this in Git, review changes like code
READERS = {
    "user_a": ["id", "login", "registered_datetime"],
    "analysts": ["id", "registered_datetime"],
}


def _quote_ident(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier {name!r}")
    return name


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False, tags=["security", "access"])
def fine_grained_accessor_for_tables():

    @task
    def apply_column_grants() -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        statements = []
        for role, columns in READERS.items():
            role = _quote_ident(role)
            cols = ", ".join(_quote_ident(c) for c in columns)
            # revoke first so removed columns disappear from the grant
            statements.append(f"REVOKE ALL ON dp.users FROM {role}")
            statements.append(f"GRANT SELECT({cols}) ON dp.users TO {role}")
        PostgresHook(postgres_conn_id=CONN_ID).run(statements, autocommit=False)

    @task
    def apply_row_level_security() -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        PostgresHook(postgres_conn_id=CONN_ID).run([
            "ALTER TABLE dp.users ENABLE ROW LEVEL SECURITY",
            "DROP POLICY IF EXISTS user_row_access ON dp.users",
            "CREATE POLICY user_row_access ON dp.users USING (login = current_user)",
        ], autocommit=False)

    @task
    def create_guard_view() -> None:
        """Simulated row-level access for stores without policies."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        PostgresHook(postgres_conn_id=CONN_ID).run([
            """CREATE OR REPLACE VIEW dp.users_blogs AS
               SELECT b.* FROM dp.blogs b WHERE b.blog_author = current_user""",
            "GRANT SELECT ON dp.users_blogs TO analysts",
        ], autocommit=False)

    @task
    def verify() -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        rows = PostgresHook(postgres_conn_id=CONN_ID).get_records(
            """SELECT grantee, column_name FROM information_schema.column_privileges
               WHERE table_schema = 'dp' AND table_name = 'users' AND privilege_type = 'SELECT'""")
        granted = {}
        for grantee, column in rows:
            granted.setdefault(grantee, set()).add(column)
        for role, columns in READERS.items():
            if granted.get(role, set()) != set(columns):
                raise ValueError(f"{role}: expected {sorted(columns)}, got {sorted(granted.get(role, []))}")

    apply_column_grants() >> apply_row_level_security() >> create_guard_view() >> verify()


fine_grained_accessor_for_tables()
