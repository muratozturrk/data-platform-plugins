# Connecting Spark to Polaris

## Concept

Polaris is an Iceberg REST catalog. Spark uses Iceberg's `SparkCatalog` plugin with the REST
client (`RESTCatalog`), and each Polaris catalog becomes one Spark catalog. Polaris authenticates
the client with OAuth2 client credentials (a principal's client id and secret). It then authorizes
each call with the principal's roles, and can return storage credentials scoped to one table
(credential vending).

```
Spark (RESTCatalog)                         Polaris
  POST /api/catalog/v1/oauth/tokens  ---->  client_credentials, scope PRINCIPAL_ROLE:ALL
                                     <----  access token (refreshed if token-refresh-enabled)
  GET  /api/catalog/v1/config?warehouse=<catalog>  ---->  prefix and catalog defaults
  load / create / commit table (+ header X-Iceberg-Access-Delegation)
                                     <----  metadata location (+ vended storage config)
  read / write data and manifest files directly on storage
```

## Property reference

All keys are under `spark.sql.catalog.<name>`, where `<name>` is the identifier used in SQL.

| Property | Value | Notes |
|---|---|---|
| (catalog key itself) | `org.apache.iceberg.spark.SparkCatalog` | Required |
| `.catalog-impl` | `org.apache.iceberg.rest.RESTCatalog` | Required |
| `.uri` | `http(s)://<host>:8181/api/catalog` | Polaris Iceberg REST base, without `/v1` |
| `.warehouse` | Polaris catalog name, e.g. `polariscatalog` | Not a storage path |
| `.credential` | `<clientId>:<clientSecret>` | Read it from the environment or a secret store |
| `.scope` | `PRINCIPAL_ROLE:ALL` or `PRINCIPAL_ROLE:<role>` | No spaces. A narrow role must be assigned to the principal |
| `.token-refresh-enabled` | `true` | Keeps long jobs authenticated |
| `.header.X-Iceberg-Access-Delegation` | `vended-credentials` | Asks Polaris for table-scoped storage keys |
| `.io-impl` | `org.apache.iceberg.aws.s3.S3FileIO` | For S3 storage |
| `.s3.endpoint` / `.s3.path-style-access` | endpoint URL / `true` | Only for non-AWS S3 (MinIO, SeaweedFS ...) |
| `.client.region` | e.g. `us-east-1` | The region the AWS SDK uses |

Session-wide settings: `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`
(needed for MERGE/UPDATE/DELETE and `CALL`), and `spark.jars.packages` with
`org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.4.0`
in the tested setup.

## Storage modes

| Catalog storage | Extra Spark settings | Effect |
|---|---|---|
| FILE | none | Polaris and Spark must share the directory, and both need write access |
| S3 + vending | header + S3FileIO (+ endpoint for non-AWS) | Spark holds no keys. Polaris scopes them per table and per read or read-write |
| S3 without vending | static keys in Spark | Spark can reach everything the keys allow, so catalog RBAC does not protect the files |

On the Polaris side, vending needs a storage config with a role Polaris can assume (`roleArn`).
For non-AWS S3 it also needs `endpoint`, `stsEndpoint` and `pathStyleAccess`. Polaris 1.8.0 keeps
these fields. The 1.1.0-incubating-SNAPSHOT image silently dropped them and kept only
`allowedLocations`, `region`, `roleArn` and `storageType`. Its STS calls then went to AWS and
failed with `The security token included in the request is invalid` (403).

## Worked example: a vended S3 catalog added at runtime

```python
def add_vended_s3_catalog(spark, name, warehouse, client_id, client_secret, vended,
                          server="http://polaris-latest:8181"):
    c = f"spark.sql.catalog.{name}"
    for k, v in {
        c: "org.apache.iceberg.spark.SparkCatalog",
        f"{c}.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
        f"{c}.uri": f"{server}/api/catalog",
        f"{c}.warehouse": warehouse,
        f"{c}.credential": f"{client_id}:{client_secret}",
        f"{c}.scope": "PRINCIPAL_ROLE:ALL",
        f"{c}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{c}.s3.endpoint": "http://seaweedfs:8333",
        f"{c}.s3.path-style-access": "true",
        f"{c}.client.region": "us-east-1",
    }.items():
        spark.conf.set(k, v)
    if vended:
        spark.conf.set(f"{c}.header.X-Iceberg-Access-Delegation", "vended-credentials")

add_vended_s3_catalog(spark, "s3cat_vended", "s3cat_vended", cid, secret, vended=True)
spark.sql("CREATE NAMESPACE IF NOT EXISTS s3cat_vended.db")
spark.sql("CREATE TABLE s3cat_vended.db.t (id BIGINT) USING iceberg")
spark.sql("INSERT INTO s3cat_vended.db.t VALUES (1), (2)")   # works with no keys in Spark
```

Measured: with `vended=True` the table was written and 2 rows were read back. With `vended=False`
(and no keys in the container), the write failed in the AWS SDK with `SdkClientException`.

The Polaris setup behind it, as management API calls made by root:

```
POST /api/management/v1/catalogs   {"catalog": {"name": ..., "type": "INTERNAL",
      "properties": {"default-base-location": "s3://warehouse/<name>"},
      "storageConfigInfo": {"storageType": "S3", "allowedLocations": [...], "region": "us-east-1",
        "endpoint": "http://seaweedfs:8333", "pathStyleAccess": true,
        "stsEndpoint": "http://seaweedfs:8333", "roleArn": "arn:aws:iam::000000000000:role/lake-vendor"}}}
POST /catalogs/<name>/catalog-roles                         {"catalogRole": {"name": "writer"}}
PUT  /catalogs/<name>/catalog-roles/writer/grants           {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}
PUT  /principal-roles/<principal-role>/catalog-roles/<name> {"catalogRole": {"name": "writer"}}
```

## Multi-catalog sessions

- Use one Spark catalog per Polaris catalog. The Spark name and the Polaris name can differ, and
  `warehouse` is what maps one to the other.
- You can add catalogs after the session starts with `spark.conf.set`. No restart is needed.
- Different catalogs may use different principals (different `credential` values), which is how
  one job reads with a reader principal and writes with a writer principal.
- `SHOW NAMESPACES IN <catalog>` and `SHOW TABLES IN <catalog>.<ns>` are quick connectivity
  checks. On a fresh catalog the first returned nothing, and `db` after `CREATE NAMESPACE`.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| `SdkClientException` on the first write to S3 | No vending header and no keys in Spark | Add `header.X-Iceberg-Access-Delegation=vended-credentials` |
| STS 403 `security token ... invalid` from Polaris | Storage config lost `endpoint`/`stsEndpoint` (1.1.0 image), so Polaris called AWS | Polaris 1.8.0+, and check `GET /catalogs/<name>` `storageConfigInfo` |
| `ServiceUnavailableException: Failed to create file: file:/data/db/<t>/metadata/...` | FILE catalog directory not writable by Polaris and Spark | Pre-create `<ns>/<table>/{metadata,data}` with shared permissions, or use object storage |
| 400 `invalid_scope` at session start | Malformed scope, e.g. `PRINCIPAL_ROLE: ALL` | `PRINCIPAL_ROLE:ALL` with no space |
| 401 right after a token was issued | Narrow scope names a role the principal does not hold | Assign the principal role, or use `ALL` |
| 403 on list or load | No catalog role with the privilege reaches the principal role | Grant `TABLE_LIST` / `TABLE_READ_DATA` / `CATALOG_MANAGE_CONTENT` and assign the roles |
| Upload fails after a successful load | Read-only vended keys (the principal has read privileges only) | Grant write privileges to a catalog role of that principal |
| Large first start | hadoop-aws 3.4.0 pulls in the AWS SDK v2 bundle (530 of 571 MB) | Bake the jars into the image |

## Related

- [dataframe-and-sql.md](dataframe-and-sql.md), [streaming-and-maintenance.md](streaming-and-maintenance.md)
- The `iceberg-catalogs` skill of the apache-iceberg plugin covers REST catalogs in general
- The `polaris-catalog-fundamentals` (credential vending) and `polaris-access-control` skills of this plugin cover storage configs and privileges
