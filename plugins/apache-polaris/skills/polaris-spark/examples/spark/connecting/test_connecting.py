"""Connecting Spark to Polaris: the minimal REST catalog settings, then the same REST client against
S3 storage with and without credential vending."""
import pytest

from common.admin import LATEST_SERVER, ROOT, create_s3_catalog
from common.spark_session import add_vended_s3_catalog
from conftest import jerr, record


def test_connection_snippet(spark, checks, measure):
    namespaces_before = [r[0] for r in spark.sql("SHOW NAMESPACES IN polaris").collect()]
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.db").show()
    tables = spark.sql("SHOW TABLES IN polaris.db").collect()
    namespaces_after = [r[0] for r in spark.sql("SHOW NAMESPACES IN polaris").collect()]
    assert namespaces_before == [] and namespaces_after == ["db"] and tables == []
    for example in ("SparkConf", "SHOW NAMESPACES IN polaris", "CREATE NAMESPACE IF NOT EXISTS polaris.db",
                    "SHOW TABLES IN polaris.db"):
        record(checks, example, "ok", "as-is")
    measure("connection", {"namespaces_before": namespaces_before, "namespaces_after": namespaces_after,
                               "spark": spark.version, "iceberg_runtime": "1.5.2"})


def test_file_catalog_write_needs_table_setup(spark, measure):
    """bootstrap.py creates a FILE catalog under /data; Polaris (uid 10000) and Spark (uid 185) both
    write there, so a table whose directories were not prepared with table_setup.py fails."""
    with pytest.raises(Exception) as err:
        spark.sql("CREATE TABLE polaris.db.not_prepared (id BIGINT) USING iceberg")
    assert "Failed to create file" in str(err.value)
    measure("unprepared_table_error", jerr(err.value))


def test_old_image_drops_s3_endpoint_settings(measure):
    """The 1.1.0-incubating-SNAPSHOT image stores an S3 catalog without endpoint / stsEndpoint / pathStyleAccess, so
    vending goes to AWS STS and cannot work with a local S3 service."""
    stored = create_s3_catalog("s3cat_old")
    assert {"endpoint", "stsEndpoint", "pathStyleAccess"}.isdisjoint(stored)
    stored_latest = create_s3_catalog("s3cat_probe", LATEST_SERVER, principal_role="service_admin")
    assert {"endpoint", "stsEndpoint", "pathStyleAccess"} <= set(stored_latest)
    measure("stored_storage_keys", {"1.1.0_image": sorted(stored), "1.8.0": sorted(stored_latest)})


@pytest.mark.parametrize("vended", [False, True])
def test_s3_catalog_needs_the_vending_header(spark, vended, measure):
    """Polaris 1.8.0, S3 storage, the same REST settings plus S3FileIO. The Spark container has
    no AWS keys: only credentials vended by Polaris can reach the bucket."""
    name = f"s3cat_{'vended' if vended else 'plain'}"
    create_s3_catalog(name, LATEST_SERVER, principal_role="service_admin")
    add_vended_s3_catalog(spark, name, name, *ROOT, vended=vended, server=LATEST_SERVER)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name}.db")
    try:
        spark.sql(f"CREATE TABLE {name}.db.t (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {name}.db.t VALUES (1), (2)")
        result = spark.sql(f"SELECT count(*) FROM {name}.db.t").collect()[0][0]
    except Exception as e:                      # noqa: BLE001 - the error text is the result
        result = jerr(e)
    measure(f"s3_{'vended' if vended else 'no_header'}", result)
    if vended:
        assert result == 2
    else:
        assert result != 2
