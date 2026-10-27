# Catalog Options and Selection

Scope: the catalog ecosystem, where each keeps the pointer, configuration snippets, a requirements
scorecard, eight selection scenarios, Nessie branches.

## Ecosystem

| Catalog | OSS | REST | Distinguishing features |
|---|---|---|---|
| Hadoop | Yes | No | No service; `version-hint.text`; **deprecated** |
| Hive | Yes | No | Reuses an existing HMS; legacy / migration bridge |
| JDBC | Yes | No | Any JDBC database; SQL transaction for the pointer swap; runs inside each engine |
| Apache Polaris | Yes | Yes | Principals, principal roles, catalog roles; credential vending; internal + read-only external catalogs |
| Project Nessie | Yes | Yes | Git-like branches/tags, multi-table commits; DynamoDB/PostgreSQL/Cassandra backends |
| Apache Gravitino | Yes | Yes | Federated, geo-distributed metadata lake: metalake -> catalogs -> schemas -> tables, filesets, models, topics |
| Lakekeeper | Yes | Yes | Rust; OpenFGA authz; vending; soft delete; change-approval hooks; REST under `/catalog`, management under `/management`; PostgreSQL only |
| AWS Glue Data Catalog | No | Yes | Managed; IAM, Lake Formation, CloudTrail; one catalog per account and region |
| Dremio catalog | Polaris-based | Yes | Managed; row filters, column masking, lineage, automated maintenance; billed on maintenance compute |
| Snowflake Open Catalog | Polaris-based | Yes | Hosted Polaris; billed per request; overlapping storage paths prohibited in internal catalogs |
| Databricks Unity Catalog | No (OSS is a separate codebase) | Yes | Delta + Iceberg governance |

## Where the pointer lives

| Catalog | Pointer mechanism |
|---|---|
| Hadoop | `metadata/version-hint.text` holds N of `vN.metadata.json`; commit = "create vN+1 if absent", needs atomic rename |
| JDBC | Row in `iceberg_tables` (`metadata_location`) |
| REST (fixture, Polaris, Glue, Lakekeeper...) | Server store, returned as `metadata-location` by LoadTable |
| Nessie | `metadataLocation` of content key `ns.table` in a commit on a branch |

The same Spark SQL (create, 2 inserts, 200 rows) runs unchanged on Hadoop, REST, Polaris and
Nessie catalogs, and in each case the catalog's pointer names exactly the file Spark resolved
(latest row of `<table>.metadata_log_entries`). Three inserts into a Hadoop table leave
`version-hint.text` = 4 and `v1`..`v4.metadata.json`.

## Configuration snippets

```properties
# JDBC (PostgreSQL)
spark.sql.catalog.jdbc=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.jdbc.type=jdbc
spark.sql.catalog.jdbc.uri=jdbc:postgresql://db:5432/iceberg
spark.sql.catalog.jdbc.jdbc.user=<db-user>
spark.sql.catalog.jdbc.jdbc.password=<db-password-from-secret-store>
spark.sql.catalog.jdbc.warehouse=s3://warehouse/jdbc
# Hive Metastore
spark.sql.catalog.hms.type=hive
spark.sql.catalog.hms.uri=thrift://metastore:9083
# AWS Glue (native client)
spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.glue.warehouse=s3://bucket/prefix
# Nessie (Iceberg NessieCatalog over Nessie API v2)
spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog
spark.sql.catalog.nessie.uri=http://nessie:19120/api/v2
spark.sql.catalog.nessie.ref=main
spark.sql.catalog.nessie.warehouse=s3://warehouse/nessie/
# Hadoop (dev only, deprecated)
spark.sql.catalog.hadoop.type=hadoop
spark.sql.catalog.hadoop.warehouse=file:///tmp/hadoop-warehouse
```

Glue jobs enable Iceberg with `--datalake-formats iceberg`; Glue also offers an Iceberg REST endpoint.

```python
from pyiceberg.catalog.sql import SqlCatalog       # Python twin of JdbcCatalog
cat = SqlCatalog("jdbc", uri="sqlite:///catalog.db", warehouse="s3://warehouse/jdbc")
# PyIceberg has no Hadoop catalog: load_catalog(type="hadoop") raises ValueError.
from pyiceberg.table import StaticTable             # read-only by metadata path
StaticTable.from_metadata("file:///tmp/hadoop-warehouse/db/t/metadata/v4.metadata.json")
```

## Requirements scorecard
Translate audit findings into six areas: performance/availability/scale, governance and lineage,
security and compliance, deployment and ecosystem, cost and operations, federation. 99.9%
availability is ~8.8 h downtime per year; mission-critical targets 99.99% (< 1 h).

```python
@dataclass(frozen=True)
class Requirement:
    key: str; must: bool = True; weight: int = 1
    max_value: float | None = None; min_value: float | None = None; why: str = ""

production = [
    Requirement("auth_required", why="security: IAM / OAuth"),
    Requirement("credential_vending", why="governance: no long-lived keys in engines"),
    Requirement("load_p95_ms", max_value=1000, why="interactive queries"),
    Requirement("lost_updates", max_value=0, why="transaction coordination"),
    Requirement("views", must=False, weight=1),
]
# evaluate(): candidates missing any must are ineligible (with reasons); the rest rank by weight.
```

- Measured probe (plain REST): REST fixture misses `auth_required` and `credential_vending`, so
  Polaris wins the production profile; a dev profile (`rest`, `lost_updates <= 0`) picks the fixture.
- A cheap candidate without `rbac` stays ineligible even with a higher preference score; if nobody
  meets all musts there is no winner.
- Latency: fixture p50 36.1 / p95 57.6 ms, Polaris p50 73.1 / p95 107.9 ms; tens of ms rarely change
  end-to-end query time. Contention: 4 writers x 5 schema commits on one table -> 20/20 on both,
  21 vs 42 retries.

## Scenarios

| Scenario | Dominant musts | Outcome | Rejected |
|---|---|---|---|
| Mid-sized retail on HMS, 12-18 month migration | HMS compat, multi-engine, self-managed | Hive (revisit Nessie/Polaris) | Hadoop |
| Fintech all-in on AWS, AI workloads | managed, AWS-native, RBAC, audit, REST | Glue (revisit for portability) | Dremio, Snowflake Open Catalog |
| Healthcare multinational, residency | geo-distributed, federation, multi-asset, RBAC, audit | Gravitino | Lakekeeper, Nessie |
| Small SaaS on AWS, batch + Athena | managed, AWS-native, low ops | Glue | Lakekeeper, JDBC |
| Global multicloud firm | federation, geo-distributed, OSS, REST | Gravitino | Nessie, Polaris |
| Daily zero-copy stress-test clones | versioning, multi-table transactions | Nessie | Glue, Polaris, Gravitino, Lakekeeper |
| Phased migration federating Hive/Teradata/Oracle | query federation, RBAC, audit, low ops | Dremio catalog | Hadoop, JDBC, Polaris, Gravitino, Glue |
| Python-first startup, no infrastructure | no service, Python, OSS, low cost | Hadoop (deprecated) | Polaris, Glue |

Each profile leaves exactly one eligible option. Replacing "AWS-native" with "cloud-agnostic" in the
fintech case makes a Polaris-based managed service win. In the Python-first case, replacing "no
service" with "safe concurrent writers" makes the answer JDBC (`SqlCatalog`) - prefer that.

## Nessie branches

```python
API = "http://nessie:19120/api/v2"
head = requests.get(f"{API}/trees/main").json()["reference"]["hash"]
requests.post(f"{API}/trees", params={"name": "stress_x", "type": "BRANCH"},
              json={"type": "BRANCH", "name": "main", "hash": head})          # zero-copy
requests.post(f"{API}/trees/main@{head}/history/merge",
              json={"fromRefName": "etl_x", "fromHash": etl_head})            # one commit on main
requests.get(f"{API}/trees/main/diff/stress_x")                               # changed keys
```

```sql
UPDATE nessie.risk.`prices@stress_x` SET price = price * 0.6;   -- branch only
SELECT sum(p.qty * x.price) FROM nessie.risk.`positions@stress_x` p
  JOIN nessie.risk.`prices@stress_x` x USING (instrument);
```

Branch creation copies 0 objects; exposure 5,950,000 on `main`, 3,570,000 on the branch; diff lists
only `prices`; deleting the branch leaves `main` unchanged. A two-table ETL branch shows 0 of 2 new
rows on `main` before the merge, 2 of 2 after; the newest `main` commit is the merge with the old
head as parent. Every table change (create, append, schema change) is one Nessie commit.

## Gotchas

| Symptom | Cause | Fix |
|---|---|---|
| `DROP TABLE ... PURGE` fails "GC is disabled" on Nessie | `NessieCatalog` sets `gc.enabled=false` (files shared by branches) | Drop without PURGE; use Nessie GC |
| Polaris 403 "not in the list of allowed locations" on register | Table outside the catalog/namespace location | Add to `allowedLocations` + `polaris.config.allow.unstructured.table.location=true` |
| Concurrent writers corrupt a Hadoop table on S3 | No atomic rename on object stores | Use JDBC or REST |
| Glue tables invisible in another region | Glue is regional | Replicate metadata or use a cross-region catalog |
| Old Nessie image lacks Iceberg REST | Docker Hub tags stop at 0.76.6 | Use `NessieCatalog` or newer images from ghcr.io / quay.io |
