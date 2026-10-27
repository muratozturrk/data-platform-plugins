"""Dremio reads the table Spark wrote, through Nessie, with no copy."""
import pytest

from common.dremio import DremioError
from common.lakehouse import list_keys
from spark_ingestion.ingest import full_reload
from dremio_queries.dremio_lab import SOURCE, connect_dremio, count_at_snapshot, data_files_scanned, snapshots


@pytest.fixture(scope="module")
def dremio():
    return connect_dremio()


@pytest.fixture(scope="module")
def table(spark, ns):
    t = full_reload(spark, ns)
    return t, f"{SOURCE}.{t}"            # Spark name, Dremio name (source.namespace.table)


def test_dremio_discovers_the_spark_table_through_nessie(dremio, table):
    spark_name, dremio_name = table
    ns = spark_name.split(".")[0]
    children = dremio.session.get(f"{dremio.url}/api/v3/catalog/by-path/{SOURCE}/{ns}").json()["children"]
    assert [c["path"][-1] for c in children] == ["fashion_sales"]


def test_same_answer_in_dremio_and_spark_and_no_new_files(spark, dremio, table):
    spark_name, dremio_name = table
    objects_before = len(list_keys("s3://warehouse/"))
    sql = "SELECT category, count(*) AS n, sum(sales_amount) AS revenue FROM {t} GROUP BY category ORDER BY category"
    d = [(r["category"], r["n"], float(r["revenue"])) for r in dremio.sql(sql.format(t=dremio_name))]
    s = [(r.category, r.n, float(r.revenue)) for r in spark.sql(sql.format(t=spark_name)).collect()]
    assert d == s
    assert sum(n for _, n, _ in d) == 3000
    assert len(list_keys("s3://warehouse/")) == objects_before       # reading creates no copy


def test_table_snapshot_and_time_travel_follow_spark_commits(spark, dremio, table):
    spark_name, dremio_name = table
    first = snapshots(dremio, dremio_name)
    spark.sql(f"""INSERT INTO {spark_name} VALUES
                  (90001, 'Wool Coat', 'Outerwear', 199.00, DATE '2024-06-02', 'Chicago', '45-54', 'Summer Kickoff')""")
    after = snapshots(dremio, dremio_name)
    spark_ids = [r.snapshot_id for r in spark.sql(
        f"SELECT snapshot_id FROM {spark_name}.snapshots ORDER BY committed_at").collect()]

    assert [int(r["snapshot_id"]) for r in after] == spark_ids        # one history, seen by both engines
    assert len(after) == len(first) + 1
    assert dremio.sql(f"SELECT count(*) AS n FROM {dremio_name}")[0]["n"] == 3001
    assert count_at_snapshot(dremio, dremio_name, first[-1]["snapshot_id"]) == 3000
    with pytest.raises(DremioError, match="Encountered"):          # unquoted snapshot id is a parse error
        dremio.sql(f"SELECT count(*) AS n FROM {dremio_name} AT SNAPSHOT {first[-1]['snapshot_id']}")


def test_metadata_pruning_skips_files_by_column_range(spark, dremio, ns):
    """A filter on sales_amount = 35 skips files whose min/max range excludes 35."""
    source = spark.table(f"{ns}.fashion_sales")
    sorted_t, random_t = f"{ns}.sales_by_amount_range", f"{ns}.sales_random_layout"
    source.repartitionByRange(6, "sales_amount").sortWithinPartitions("sales_amount").writeTo(sorted_t).create()
    source.repartition(6).writeTo(random_t).create()

    sql = "SELECT * FROM {source}.{t} WHERE sales_amount = 35"
    assert data_files_scanned(dremio, f"SELECT * FROM {SOURCE}.{sorted_t}") == 6     # no filter: every file
    assert data_files_scanned(dremio, sql.format(source=SOURCE, t=sorted_t)) == 1    # ranges are disjoint
    assert data_files_scanned(dremio, sql.format(source=SOURCE, t=random_t)) == 6    # every range covers 35

    # The same decision computed from Iceberg metadata in Spark: one candidate file of six.
    candidates = spark.sql(f"""SELECT count_if(readable_metrics.sales_amount.lower_bound <= 35
                                               AND readable_metrics.sales_amount.upper_bound >= 35) AS c
                               FROM {sorted_t}.data_files""").first().c
    assert candidates == 1
