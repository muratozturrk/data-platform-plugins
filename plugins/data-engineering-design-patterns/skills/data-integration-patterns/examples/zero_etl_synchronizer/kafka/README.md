# Zero-ETL Synchronizer - Kafka side (configuration, no code)

The pattern removes the ingestion job. The broker (or vendor service) writes topic data
into lakehouse tables itself, so this "example" is configuration only.

**Aiven Iceberg topics (open source plug-in on tiered storage)**, broker properties:

```properties
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=io.aiven.kafka.tieredstorage.RemoteStorageManager
rsm.config.segment.format=iceberg
rsm.config.structure.provider.class=io.aiven.kafka.tieredstorage.iceberg.AvroSchemaRegistryStructureProvider
rsm.config.iceberg.namespace=dp
rsm.config.iceberg.catalog.class=org.apache.iceberg.rest.RESTCatalog
rsm.config.iceberg.catalog.uri=http://localhost:8181
rsm.config.iceberg.catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
rsm.config.iceberg.catalog.warehouse=s3://dp/iceberg
```
Closed segments are converted to Parquet in the `dp` namespace. The topic still needs
`remote.storage.enable=true`, and a schema registry is required for the structure.

**Bufstream**: catalog in `bufstream.yaml`, then per-topic configs:

```yaml
iceberg:
  catalogs:
    - name: local-rest-catalog
      rest:
        url: http://iceberg-rest:8181
```
```properties
bufstream.archive.kind=ICEBERG
bufstream.archive.iceberg.catalog=local-rest-catalog
bufstream.archive.iceberg.table=dp.visits
```
(In local dev, trigger the sync with `bufstream admin clean topics`. The managed service syncs on schedule.)

**Confluent Tableflow (Delta/Iceberg)**: configure it with Terraform (repeatable). Set the topic,
the S3 bucket/external location, the catalog type (`databricks`), and credentials from a secret store,
never inline.

What remains your job: permissions on the output location, catalog configuration, sync tuning
(file size, throughput), compaction and monitoring. See `../spark` and `../airflow`.
