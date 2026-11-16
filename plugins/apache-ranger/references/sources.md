# Web sources (v1)

## Apache Ranger project
- Release news and notes: 2.6.0 (Feb 2025), 2.7.0 (Jul 30 2025: audit module refactor; plugin packages bundle only Solr and HDFS audit-destination libraries; `ranger-audit-dest-*` modules; usersync `ldap.referral=follow` default), 2.8.0 (Mar 1 2026: `authz-api` and `authz-embedded` modules, service-managed ACLs/inline policy, Kerberos docker setup, hadoop-common removed from plugins, JWT authentication between plugin and Admin; Kafka 3.9.1). https://cwiki.apache.org/confluence/display/RANGER/News
- Wiki: architecture, installation (`install.properties`, `setup.sh`), usersync, tag-based policies and Tag Synchronizer, security zones, row-level filtering and column masking, policy conditions, deny/exceptions and evaluation flow, audit configuration, REST APIs. https://cwiki.apache.org/confluence/display/RANGER
- Source repo: `dev-support/ranger-docker` (compose-based build of admin/usersync/tagsync/kms/db/solr), plugin modules (`plugin-kafka`, `plugin-trino` legacy, `ranger-audit-dest-*`). https://github.com/apache/ranger
- RANGER-4810: move Trino authorizer from Ranger repo to Trino repo (SPI drift, JDK requirements).
- Python client `apache-ranger` (PyPI); REST API Swagger in Ranger Admin.

## Engines
- Trino docs — Ranger access control (`access-control.name=ranger`, `ranger.service.name`, `ranger.plugin.config.resource`, `ranger.hadoop.config.resource`; `ranger-trino-security.xml`, `ranger-trino-audit.xml`, `ranger-policymgr-ssl.xml`; column masking, row filtering, audit; Ranger ≥ 2.5 service-def; `queryid` policy for `{USER}`); Trino PR #22675 (Ranger authorizer added to Trino). https://trino.io/docs/current/security/ranger-access-control.html
- Apache Kafka authorizer interface (KIP-504); Ranger Kafka plugin (`RangerKafkaAuthorizer`); Strimzi custom authorization (`authorization.type: custom`, `authorizerClass`, `superUsers`). https://strimzi.io/docs
- Apache Kyuubi — Spark AuthZ plugin (`RangerSparkExtension`, row filter, masking, Iceberg). https://kyuubi.readthedocs.io
- Starburst docs (Ranger in Kubernetes, usersync env settings) as a reference for containerised config.

## Kubernetes deployment references
- Canonical `ranger-k8s-operator` (Juju charm: PostgreSQL relation, usersync as separate single unit, plugins poll every ~3 min, stateless Admin).
- Community walkthrough "Big data on Kubernetes — Apache Ranger" (building images, install.properties, Solr/Elasticsearch audit settings, plugins inside engine containers).

## Reports, comparisons, use cases
- GigaOm, "ABAC vs RBAC" benchmark and "Cloud Data Security: Immuta and Apache Ranger" (policy-change counts as cost metric); The Daily Swig coverage of the Ranger maintainers' rebuttal (lookup tables/tag policies overlooked) and Privacera's "What's ABAC got to do with it?".
- Privacera white paper "Apache Ranger and Privacera" (Ranger as the engine behind a commercial platform; cloud gaps Ranger OSS does not cover, e.g. object storage).
- Cloudera SDX (Ranger + Atlas as shared governance across on-prem/hybrid) as the largest deployed pattern; AWS EMR Ranger integration (Trino/Spark/Hive plugins, S3 plugin proprietary).
- Research: "ABAC in big-data platforms with Apache Ranger" (NIST SP 800-162 trust chain applied to Hadoop; ProQuest 2767177654).
- Huawei MRS Ranger principles page (concise component description).

## Platform context
- HKM technology list (`HKM_kullaniacak_tum_teknolojiler.xlsx`): Ranger for access policy and dynamic masking; OPA for retention; Keycloak; OpenBao; OpenMetadata; Nessie; OpenSearch/immudb audit.
- Related plugins in this marketplace: `trino` (Ranger config examples), `project-nessie` (CEL + Ranger two-layer model), `qa-test-strategy` (T-ranger-*, T-trino-00x tests), `platform-engineering` (Policy → Ranger compilation).
