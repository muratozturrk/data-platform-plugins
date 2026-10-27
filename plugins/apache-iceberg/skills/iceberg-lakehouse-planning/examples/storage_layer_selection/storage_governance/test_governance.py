import base64
import os
from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError
from py4j.protocol import Py4JJavaError

from common.lakehouse import SSE_C_KEY, events_df
from common.s3 import client, ensure_bucket, list_keys, split_uri

from .governance import (ACCEPTED, BREAKS, ENFORCED, UNSUPPORTED, create_locked_bucket,
                         feature_matrix, restore_deleted, retained_versions)

DDL = "CREATE TABLE {t} (order_id BIGINT, amount DOUBLE) USING iceberg {loc}"


@pytest.fixture(scope="module")
def matrix(uid_module):
    return {store: feature_matrix(client(store), f"gov-{uid_module}") for store in ("seaweedfs", "rustfs")}


@pytest.fixture(scope="module")
def uid_module():
    import uuid
    return uuid.uuid4().hex[:6]


def _root_cause(err) -> str:
    """Innermost 'Caused by' line of a Java exception, shortened."""
    lines = [ln for ln in str(err.value).splitlines() if ln.startswith("Caused by:")]
    return (lines[-1] if lines else str(err.value).splitlines()[1])[:180]


def _data_files(spark, table):
    return [r.file_path for r in spark.sql(f"SELECT file_path FROM {table}.files").collect()]


# ---------- feature matrix: what the endpoint actually does ----------

def test_feature_matrix_of_both_s3_implementations(matrix, measure):
    for store, m in matrix.items():
        measure(store, m)
    common = {"bucket_policy": ENFORCED, "versioning": ENFORCED, "object_lock": ENFORCED,
              "lifecycle": ACCEPTED, "sse_c": ENFORCED, "tagging": ENFORCED}
    for store in matrix:
        assert {k: matrix[store][k] for k in common} == common
    # Encryption with store-managed keys needs a key source (KMS or master key) in the store.
    assert matrix["rustfs"]["sse_s3"] == ENFORCED and matrix["rustfs"]["default_encryption"] == ENFORCED
    assert matrix["seaweedfs"]["sse_s3"] == UNSUPPORTED
    assert matrix["seaweedfs"]["default_encryption"] == BREAKS


# ---------- access control: storage enforces it even when the catalog does not ----------

def test_read_only_identity_can_query_but_not_write_an_iceberg_table(spark, ns, measure):
    t = f"{ns}.orders_ro"
    spark.sql(DDL.format(t=f"lake.{t}", loc=""))
    events_df(spark, 1000).select("order_id", "amount").writeTo(f"lake.{t}").append()
    assert spark.table(f"analyst.{t}").count() == 1000            # Read + List on bucket warehouse
    with pytest.raises(Py4JJavaError) as err:
        spark.sql(f"INSERT INTO analyst.{t} VALUES (1, 1.0)")
    measure("analyst_insert_error", _root_cause(err))
    assert "403" in str(err.value) or "Access Denied" in str(err.value)
    spark.sql(f"REFRESH TABLE lake.{t}")
    assert spark.table(f"lake.{t}").count() == 1000                # nothing was committed


def test_bucket_scoped_identity_cannot_read_another_teams_files(spark, ns):
    t = f"lake.{ns}.orders_ro"
    bucket, key = split_uri(_data_files(spark, t)[0])
    rnd = client("seaweedfs", os.getenv("RND_ACCESS_KEY", "rnd"), os.getenv("RND_SECRET_KEY", "rnd-secret"))  # local dev default
    with pytest.raises(ClientError) as err:
        rnd.get_object(Bucket=bucket, Key=key)
    assert err.value.response["Error"]["Code"] == "AccessDenied"
    rnd.put_object(Bucket="rnd", Key="features/x.parquet", Body=b"ok")   # own bucket works


# ---------- integrity: versioning and object lock under an Iceberg table ----------

def test_versioned_bucket_recovers_an_accidentally_deleted_data_file(spark, ns, uid_module, measure):
    bucket = f"versioned-{uid_module}"
    s3 = client()
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    t = f"lake.{ns}.orders_versioned"
    spark.sql(DDL.format(t=t, loc=f"LOCATION 's3://{bucket}/orders'"))
    events_df(spark, 2000).select("order_id", "amount").repartition(2).writeTo(t).append()
    before = spark.sql(f"SELECT count(*), round(sum(amount), 2) FROM {t}").first()
    victim = _data_files(spark, t)[0]
    s3.delete_object(Bucket=bucket, Key=split_uri(victim)[1])          # someone cleans up "old files"
    with pytest.raises(Py4JJavaError) as err:
        spark.sql(f"SELECT sum(amount) FROM {t}").collect()
    measure("query_after_delete", _root_cause(err))
    restore_deleted(s3, bucket, split_uri(victim)[1])
    assert spark.sql(f"SELECT count(*), round(sum(amount), 2) FROM {t}").first() == before


def test_object_lock_keeps_every_iceberg_file_after_drop_table_purge(spark, ns, uid_module, measure):
    bucket = f"locked-{uid_module}"
    s3 = client()
    create_locked_bucket(s3, bucket, days=1)
    t = f"lake.{ns}.ledger"
    spark.sql(DDL.format(t=t, loc=f"LOCATION 's3://{bucket}/ledger'"))
    events_df(spark, 500).select("order_id", "amount").writeTo(t).append()
    files_before = len(list_keys("seaweedfs", bucket, "ledger/"))
    spark.sql(f"DROP TABLE {t} PURGE")                                  # succeeds: only delete markers
    versions = retained_versions(s3, bucket, "ledger/")
    measure("after_purge", f"visible objects={len(list_keys('seaweedfs', bucket, 'ledger/'))}, "
                           f"retained versions={len(versions)} (objects before purge={files_before})")
    assert list_keys("seaweedfs", bucket, "ledger/") == []
    assert len(versions) == files_before
    with pytest.raises(ClientError) as err:                             # retention blocks real deletion
        s3.delete_object(Bucket=bucket, Key=versions[0]["Key"], VersionId=versions[0]["VersionId"])
    assert err.value.response["Error"]["Code"] == "AccessDenied"


# ---------- Iceberg S3FileIO governance properties ----------

def test_s3fileio_tags_files_on_write_and_soft_deletes_on_expiration(spark, ns, measure):
    """`s3.write.tags.*`, `s3.delete-enabled=false` and `s3.delete.tags.*`: hand deletion to lifecycle rules."""
    t = f"lake_softdelete.{ns}.orders_soft"
    spark.sql(DDL.format(t=t, loc=""))
    events_df(spark, 1000).select("order_id", "amount").writeTo(t).append()
    first = _data_files(spark, t)
    events_df(spark, 1000, seed=7).select("order_id", "amount").writeTo(t).overwritePartitions()
    now = (datetime.now(timezone.utc) + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
    expired = spark.sql(f"CALL lake_softdelete.system.expire_snapshots(table => '{ns}.orders_soft', "
                        f"older_than => TIMESTAMP '{now}', retain_last => 1)").first()
    assert expired.deleted_data_files_count == len(first)              # Iceberg "deleted" them ...
    s3 = client()
    tags = {}
    for path in first:
        b, k = split_uri(path)
        s3.head_object(Bucket=b, Key=k)                                 # ... but they are still on storage
        tags[k.rsplit("/", 1)[-1]] = {x["Key"]: x["Value"] for x in s3.get_object_tagging(Bucket=b, Key=k)["TagSet"]}
    measure("expired_file_tags", tags)
    assert all(v == {"team": "finance", "iceberg-deleted": "true"} for v in tags.values())
    current = _data_files(spark, t)
    b, k = split_uri(current[0])
    assert {x["Key"]: x["Value"] for x in s3.get_object_tagging(Bucket=b, Key=k)["TagSet"]} == {"team": "finance"}
    spark.sql(f"DROP TABLE {t}")


def test_s3fileio_sse_c_encrypts_every_file_the_engine_writes(spark, ns, measure):
    """`s3.sse.type=custom`: S3FileIO sends the customer key with every PUT and GET."""
    t = f"lake_ssec.{ns}.orders_enc"
    spark.sql(DDL.format(t=t, loc=""))
    events_df(spark, 1000).select("order_id", "amount").writeTo(t).append()
    assert spark.table(t).count() == 1000                               # reads send the key too
    bucket, prefix = split_uri(spark.sql(f"DESCRIBE TABLE EXTENDED {t}")
                               .where("col_name = 'Location'").first().data_type)
    s3, key = client(), base64.b64decode(SSE_C_KEY)
    readable_without_key = {}
    for k in list_keys("seaweedfs", bucket, prefix + "/"):
        kind = "metadata.json" if k.endswith(".metadata.json") else k.rsplit(".", 1)[-1]
        try:
            s3.get_object(Bucket=bucket, Key=k)
            readable_without_key.setdefault(kind, set()).add(True)
        except ClientError:
            s3.get_object(Bucket=bucket, Key=k, SSECustomerAlgorithm="AES256", SSECustomerKey=key)
            readable_without_key.setdefault(kind, set()).add(False)
    measure("readable_without_key", {k: sorted(v) for k, v in readable_without_key.items()})
    assert readable_without_key["parquet"] == {False}                   # data files
    assert readable_without_key["avro"] == {False}                      # manifests, manifest lists
    assert readable_without_key["metadata.json"] == {True}              # written by the REST server
    spark.sql(f"DROP TABLE {t}")


def test_s3fileio_sse_s3_on_rustfs_fails_the_sdk_integrity_check(spark, measure, uid_module):
    """RustFS returns a non-MD5 ETag for SSE-S3 objects; AWS SDK v2 rejects the upload."""
    import hashlib
    s3 = client("rustfs")
    ensure_bucket("rustfs", "etag-check")
    body = b"x" * 1000
    plain = s3.put_object(Bucket="etag-check", Key="plain", Body=body)["ETag"].strip('"')
    sse = s3.put_object(Bucket="etag-check", Key="sse", Body=body, ServerSideEncryption="AES256")["ETag"].strip('"')
    measure("etags", f"md5={hashlib.md5(body).hexdigest()} plain={plain} sse_s3={sse}")
    assert plain == hashlib.md5(body).hexdigest() != sse

    ns = f"sse_{uid_module}"
    t = f"rust_sse.{ns}.orders"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS rust_sse.{ns}")
    spark.sql(DDL.format(t=t, loc=""))
    with pytest.raises(Py4JJavaError) as err:
        events_df(spark, 1000).select("order_id", "amount").writeTo(t).append()
    assert "Data read has a different checksum than expected" in str(err.value)
    spark.sql(f"DROP TABLE {t} PURGE")
    spark.sql(f"DROP NAMESPACE rust_sse.{ns}")
