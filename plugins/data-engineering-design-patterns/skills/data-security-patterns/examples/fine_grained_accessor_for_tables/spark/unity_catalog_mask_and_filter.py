"""Fine-Grained Accessor for Tables - Databricks Unity Catalog column mask + row filter via spark.sql.

* Column mask: everyone sees the `ip` column, but only members of `engineers`
  see its content (masking flavour, as in Databricks and Snowflake).
* Row filter : a SQL UDF that the engine injects as a WHERE clause on every query.
Both are evaluated per query -> some overhead; if it hurts, materialize a
per-group table/view (Dataset Materializer) at the cost of duplication.
Nested STRUCT fields can't be masked individually: unnest them first.

Run on a Databricks cluster with Unity Catalog (DBR 12.2+):
  export UC_CATALOG=main UC_SCHEMA=dp
  python unity_catalog_mask_and_filter.py      (or paste into a notebook)
"""
import os

from pyspark.sql import SparkSession

CATALOG = os.environ.get("UC_CATALOG", "main")
SCHEMA = os.environ.get("UC_SCHEMA", "dp")
FQ = f"{CATALOG}.{SCHEMA}"

spark = SparkSession.builder.getOrCreate()

STATEMENTS = [
    f"CREATE SCHEMA IF NOT EXISTS {FQ}",
    # --- column-level: masking function
    f"""CREATE OR REPLACE FUNCTION {FQ}.ip_mask(ip STRING)
        RETURN CASE WHEN is_account_group_member('engineers') THEN ip ELSE '***' END""",
    # --- row-level: admins see everything, others only their own country
    f"""CREATE OR REPLACE FUNCTION {FQ}.country_filter(country STRING)
        RETURN is_account_group_member('admins')
            OR country = (SELECT u.country FROM {FQ}.user_countries u WHERE u.login = current_user())""",
    f"""CREATE TABLE IF NOT EXISTS {FQ}.user_countries (login STRING, country STRING)""",
    f"""CREATE TABLE IF NOT EXISTS {FQ}.visits (
          visit_id STRING,
          ip STRING MASK {FQ}.ip_mask,
          country STRING,
          page STRING)
        WITH ROW FILTER {FQ}.country_filter ON (country)""",
    # For an existing table use ALTER instead of CREATE:
    f"ALTER TABLE {FQ}.visits ALTER COLUMN ip SET MASK {FQ}.ip_mask",
    f"ALTER TABLE {FQ}.visits SET ROW FILTER {FQ}.country_filter ON (country)",
    # table-level grant is still required: fine-grained rules narrow it, they don't replace it
    f"GRANT SELECT ON TABLE {FQ}.visits TO `analysts`",
]


def apply_policies() -> None:
    for statement in STATEMENTS:
        spark.sql(statement)


def show_effective_view() -> None:
    # Run as a non-engineer: ip comes back as '***' and only own-country rows are visible.
    spark.sql(f"SELECT visit_id, ip, country FROM {FQ}.visits LIMIT 10").show(truncate=False)


if __name__ == "__main__":
    apply_policies()
    show_effective_view()
