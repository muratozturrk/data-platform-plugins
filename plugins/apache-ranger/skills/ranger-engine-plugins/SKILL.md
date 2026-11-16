---
name: ranger-engine-plugins
description: Integrating Ranger with the HKM stack engine by engine: Trino (built-in ranger access control, config files, resources and access types, masking/row filters, impersonation, Gateway), Kafka on Strimzi (RangerKafkaAuthorizer as custom authorizer, image layer, KRaft notes, topic-prefix policies), Spark SQL via Apache Kyuubi AuthZ (Ranger-backed extension, Iceberg support, limits), and the engines without a Ranger plugin (ClickHouse, Superset, Nessie, Flink, OpenMetadata, CKAN, WSO2, object storage) where the control plane compiles equivalent native rules. Use when onboarding an engine or reasoning about enforcement coverage and gaps.
---

# Engine Plugins and Coverage

## Coverage map (HKM stack)
| Engine | Enforcement | Row filter / mask | Audit to Ranger |
|---|---|---|---|
| Trino (+ Gateway) | **Ranger plugin (in Trino)** | yes / yes | yes |
| Kafka (Strimzi) | **Ranger Kafka plugin** as custom authorizer | n/a | yes |
| Spark SQL (Spark on K8s) | **Kyuubi Spark AuthZ** (Ranger client, hive-style service) | yes / yes (SQL path only) | yes |
| Flink | none — protected by Kafka plugin (topics), Nessie CEL (catalog), object-store credentials | — | via Kafka/Nessie |
| ClickHouse | native RBAC, row policies, column grants — compiled from Policy | native | ClickHouse query log → audit pipeline |
| Superset | feature permissions only (SQL Lab, charts, export) — compiled; no RLS; data path goes through Trino (Ranger) with impersonation | via Trino | via Trino |
| Nessie | CEL metadata rules — compiled | — | Nessie events |
| OpenMetadata | own RBAC; **source of tags** pushed to Ranger | — | own audit |
| CKAN / WSO2 / FastAPI | visibility classes, scopes — compiled | — | own logs |
| Object storage (ObjectScale/Ceph/AIStor) | bucket policies + scoped/vended credentials; no OSS Ranger S3 plugin | — | storage access logs |
Rule: any path to data that bypasses a Ranger-enforcing engine must be closed by credentials (no direct lake access for humans) or covered by compiled native rules with tests proving equivalence.

## Trino
- `etc/access-control.properties`: `access-control.name=ranger`, `ranger.service.name=<service>`, `ranger.plugin.config.resource=/etc/trino/ranger-trino-security.xml,/etc/trino/ranger-trino-audit.xml[,ranger-policymgr-ssl.xml]` (plus `ranger.hadoop.config.resource` only if Hadoop/Kerberos configs are needed). The authorizer ships with Trino (moved from the Ranger repo in 2024); Ranger ≥ 2.5 has the service-def.
- `ranger-trino-security.xml`: `ranger.plugin.trino.policy.rest.url`, `…policy.cache.dir`, `…policy.pollIntervalMs`, `…access.cluster.name`, `…super.users/groups`, optional Kerberos principal/keytab; SSL file for 2-way TLS.
- Resources/access types (service-def): catalog, schema, table, column (select, insert, delete, update/alter, create, drop, show, grant/revoke…), `trinouser` (impersonate), `systemproperty`/`sessionproperty` (alter), `function`/`procedure` (execute), `queryid` (execute to view/kill). Default-deny surprises: users need a `queryid` policy for `{USER}` to see/kill own queries and access to `system`/`information_schema` for tools (Superset, DBeaver, dbt) — include in baseline.
- Row filters and masks are applied by Trino's engine to every client; expressions must be valid Trino SQL; views with definer/invoker semantics need explicit policy on the view and underlying tables as appropriate.
- Multi-cluster: one Ranger service per cluster or one shared service when policies must be identical (interactive + ETL clusters) — set the same `ranger.service.name`; Gateway is transparent.
- Combine with `file` access control only for bootstrap (`access-control.config-files` chaining) and remove afterwards.

## Kafka on Strimzi
- Broker config: `authorizer.class.name=org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer`; in Strimzi `spec.kafka.authorization: { type: custom, authorizerClass: …, superUsers: [CN=…, svc-control-plane] }` and a **custom Kafka image** adding the Ranger Kafka plugin jars and `ranger-kafka-security.xml`, `ranger-kafka-audit.xml` on the classpath; the plugin's policy cache and audit spool need writable volumes.
- Resources: `topic`, `consumergroup`, `transactionalid`, `cluster`, `delegationtoken`; access types publish, consume, create, delete, configure/alter, describe, describe_configs, alter_configs, idempotent_write, cluster_action.
- Institution isolation = policies on topic/group prefixes `t.{institution}.*` in the institution zone; quotas remain Kafka-native (Strimzi `KafkaUser` quotas) because Ranger does not manage quotas.
- Principals: with mTLS the principal is the certificate DN — map to short names with `ssl.principal.mapping.rules`; with OAUTHBEARER (Keycloak) the principal is the token subject/`preferred_username`. Inter-broker and controller principals must be super users; verify the plugin against the Kafka/KRaft version in use (Ranger 2.8 builds against Kafka 3.9.x) before upgrading brokers.
- Strimzi's User Operator ACL management is disabled when a custom authorizer is used — access is granted only via Ranger.

## Spark SQL via Kyuubi AuthZ
- Apache Kyuubi provides `kyuubi-spark-authz` (`spark.sql.extensions=org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension`) using Ranger client libraries with a hive-type (or dedicated spark-sql) Ranger service: `ranger-spark-security.xml`, `ranger-spark-audit.xml` on the driver classpath; supports table/column access checks, row filters and masking for Spark SQL and Iceberg commands.
- Limits: enforcement is in the SQL optimizer of the **driver** — RDD/Dataset APIs, direct file reads (`spark.read.parquet("s3://…")`) and arbitrary code bypass it. Therefore: platform Spark jobs are generated SQL/DataFrame-on-catalog only, run with per-institution object-store credentials, and ad-hoc Spark (notebooks) is restricted to institution-scoped credentials + AuthZ; treat Spark AuthZ as defence in depth, credentials as the hard boundary.
- The hive-type Spark service has no catalog level: the Spark database is `<catalog>_<schema>` (`t_acme_lake_gold_sales`, provisional until a lab test), so one manifest compiles to both services; tag items carry `hive:*` access types; cross-institution shares are not served on Spark (`references/trino-security.md` › Spark path).

## Tags from OpenMetadata
No tagsync connector for OpenMetadata ships with Ranger; the control plane maps OpenMetadata classifications (`PII.*`, `Tier`, `purpose`) to Ranger tag definitions and resource-tag associations through the tag REST API on a schedule and on change events; a nightly reconciliation compares both sides.

## Rollout pattern
Staging dry run with decision table → enable plugin with permissive baseline + full audit → compare audits to expected decisions → switch to deny-by-default → remove bootstrap access control.

## Anti-patterns
- Assuming Spark AuthZ stops file-level access
- Leaving Kafka native ACL fallback or Strimzi `simple` authorization half-configured
- Different catalog names per engine for the same data
- Tags maintained by hand in Ranger

Sources: `references/sources.md` (Trino Ranger access-control docs and PR #22675, RANGER-4810, Ranger Kafka plugin docs, Strimzi custom authorization docs, Apache Kyuubi AuthZ docs, Ranger tag REST API).

Details and book-derived rules: `references/kafka-security.md`, `references/trino-security.md`.
