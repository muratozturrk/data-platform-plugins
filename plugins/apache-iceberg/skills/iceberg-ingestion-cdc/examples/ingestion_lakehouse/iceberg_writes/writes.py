"""How Iceberg manages writes.

Part 1 uses Spark SQL / DataFrame writes and reads the resulting snapshots.
Part 2 drives Iceberg's Java commit API (the same API Spark and Flink use underneath) through
two independent REST catalog clients, which act as two writer processes. That makes the
optimistic-concurrency race deterministic. API: https://iceberg.apache.org/docs/latest/java-api-quickstart/
and https://iceberg.apache.org/docs/latest/reliability/ (concurrent write operations).
"""
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

from py4j.protocol import Py4JJavaError

from common.lakehouse import catalog_props


# --- Part 1: write semantics ----------------------------------------------------------------
def create_accounts(spark, table, merge_mode="copy-on-write") -> None:
    spark.sql(f"""CREATE TABLE {table} (id BIGINT, region STRING, balance DOUBLE)
                  USING iceberg PARTITIONED BY (region)
                  TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='{merge_mode}',
                                 'write.update.mode'='{merge_mode}', 'write.delete.mode'='{merge_mode}')""")


def accounts_df(spark, ids, region, balance=100.0):
    return spark.createDataFrame([(i, region, balance) for i in ids], "id BIGINT, region STRING, balance DOUBLE")


def last_snapshot(spark, table):
    return spark.sql(f"SELECT snapshot_id, operation, summary FROM {table}.snapshots "
                     f"ORDER BY committed_at DESC LIMIT 1").first()


def upsert(spark, table, staging_view):
    """Plain upsert MERGE: update matches, insert the rest."""
    spark.sql(f"""MERGE INTO {table} t USING {staging_view} s ON t.id = s.id
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")


def concurrent_appends(spark, table, writers: int, rows_per_writer: int) -> None:
    """N jobs append to the same table at the same time; conflicts are retried by Iceberg."""
    def job(w):
        spark.sql(f"INSERT INTO {table} SELECT id, 'eu', {w}.0 FROM range({w * rows_per_writer}, "
                  f"{(w + 1) * rows_per_writer})")
    with ThreadPoolExecutor(writers) as pool:
        list(pool.map(job, range(writers)))


# --- Part 2: commit protocol with two writer processes --------------------------------------
class Writer:
    """An independent Iceberg client: own REST catalog connection, own cached table metadata."""

    def __init__(self, spark, name, table):
        jvm = spark._jvm
        self.jvm = jvm
        props = jvm.java.util.HashMap()
        for k, v in catalog_props().items():
            props.put(k, v)
        self.catalog = jvm.org.apache.iceberg.rest.RESTCatalog()
        self.catalog.initialize(name, props)
        self.table = self.catalog.loadTable(jvm.org.apache.iceberg.catalog.TableIdentifier.parse(table))

    def base_snapshot_id(self):
        return self.table.currentSnapshot().snapshotId()

    def data_file(self, region, name, rows=10):
        # Metadata-only file entry: the commit protocol never opens data files, so a
        # registered path is enough to exercise it (the tests never scan these tables).
        return (self.jvm.org.apache.iceberg.DataFiles.builder(self.table.spec())
                .withPath(f"{self.table.location()}/data/region={region}/{name}.parquet")
                .withFileSizeInBytes(1024).withRecordCount(rows)
                .withPartitionPath(f"region={region}").withFormat("PARQUET").build())

    def append(self, region, name):
        self.table.newAppend().appendFile(self.data_file(region, name)).commit()

    def overwrite_region(self, region, name, validate_from=None):
        """Replace a partition. With validate_from, fail if someone added matching data since."""
        filt = self.jvm.org.apache.iceberg.expressions.Expressions.equal("region", region)
        op = self.table.newOverwrite().overwriteByRowFilter(filt).addFile(self.data_file(region, name))
        if validate_from is not None:
            op = op.validateFromSnapshot(validate_from).conflictDetectionFilter(filt).validateNoConflictingData()
        op.commit()

    def close(self):
        self.catalog.close()


def java_error(fn) -> str | None:
    """Run fn; return the simple class name of the Java exception it raised, or None."""
    try:
        fn()
    except Py4JJavaError as e:
        return e.java_exception.getClass().getSimpleName()
    return None


def race_appends(spark, table, writers: int, region: str = "eu") -> list[str | None]:
    """`writers` independent clients append at the same instant (barrier).

    Returns one entry per writer: None if it committed, else the Java exception class.
    Each client refreshes before committing, so only commits racing inside the same
    refresh-to-swap window collide; whether they surface depends on commit.retry.num-retries.
    """
    clients = [Writer(spark, f"racer{i}", table) for i in range(writers)]
    barrier = threading.Barrier(writers)
    tag = uuid.uuid4().hex[:6]

    def go(i):
        barrier.wait()
        return java_error(lambda: clients[i].append(region, f"race_{tag}_{i}"))
    try:
        with ThreadPoolExecutor(writers) as pool:
            return list(pool.map(go, range(writers)))
    finally:
        for c in clients:
            c.close()
