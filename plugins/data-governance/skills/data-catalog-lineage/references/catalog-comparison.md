# Catalog Comparison: OpenMetadata vs DataHub vs Apache Atlas

Measured with the same frozen rubric ([catalog-capabilities.md](catalog-capabilities.md)) on the
published release of each product, read from source code:

| Product | Release | Commit | Inspected |
|---|---|---|---|
| OpenMetadata | `1.13.3-release` | `255f6694913b` | 2026-08-11 |
| DataHub | `v1.7.0` | `7f81ccbfe27b` | 2026-08-11 |
| Apache Atlas | `release-2.5.0` | `234608315f29` | 2026-08-11 |

Every score below is evidence type **K** (code) unless marked **D** (official docs) or **İ**
(issue tracker). Vendor-claim (B) share is 0 % for all three. Paths are relative to each repo
root. The open-source editions were scored; paid cloud editions were not.

## Score matrix (D1, D2, D3, D11, D12)

| ID | Capability | OM | DH | Atlas | What decides it |
|---|---|---|---|---|---|
| D1.1 | Entity model | 3 | **4** | **4** | DH and Atlas let users define new entity types; OM cannot (`TypeResource.java:78-85`) |
| D1.2 | Custom properties | 2 | **4** | **4** | OM: typed, but not on users/teams/services/policies and **no required flag**; DH structured properties with `allowedValues`/`cardinality`/`version`; Atlas Business Metadata with compatibility-enforcing typedef updates |
| D1.3 | Schema openness | 3 | 3 | 2 | OM JSON Schema + ODCS/ODPS/DCAT/PROV/SKOS mappings; DH PDL → Avro/OpenAPI; Atlas product-specific JSON only |
| D1.4 | Versioning | 3 | 3 | 2 | OM/DH full history + diff; Atlas full snapshots in audit but no diff/rollback |
| D1.5 | API/SDK | 3 | 3 | 3 | All have official SDK(s) and bulk; none documents an API compatibility guarantee |
| D1.6 | Change events | 3 | 3 | 3 | OM webhook/chat/email subscriptions with offsets, failed-event log, replay; DH MCL/PlatformEvent on Kafka + Actions filters (no outbound webhook); Atlas `ATLAS_ENTITIES` Kafka topic, no per-subscription filter |
| D1.7 | Export / portability | 2 | 3 | 3 (D) | OM CSV for a subset of types (no dashboards/topics/pipelines), RDF if enabled; DH file sink/source round-trip documented; Atlas ZIP Import/Export API |
| D2.1 | Relevance | **4** | 3 | 2 | OM boosts by usage and votes, admin-tunable with preview |
| D2.2 | Facets | 3 | **4** | 2 | DH nested facets + shareable saved views |
| D2.3 | Natural language | 2 | 2 | 1 | Both OM and DH ship vector search **off by default**; NL-to-query not in OSS |
| D2.4 | Recommendations | 2 | 3 | 1 | DH personalised recent-views/edits/searches |
| D2.5 | Personalisation | 3 | 3 | 1 | OM personas; DH page templates |
| D2.6 | Entity page | 4 | 4 | 2 | Atlas has no quality/usage data to show |
| D3.1 | Table lineage | 3 | 3 | 3 | All automatic and queryable; **none detects broken chains** |
| D3.2 | Column lineage | 3 | **4** | 3 | DH stores the transform text and chains to BI fields |
| D3.3 | Extraction method | 3 | 3 | 3* | OM 25 dialects + runtime, edge `source`; DH sqlglot + agents, `confidenceScore`; *Atlas runtime hooks only, no SQL parser |
| D3.4 | Impact analysis | 3 | 3 | 2 | OM/DH filter + export; Atlas direction/depth only; none has what-if |
| D3.5 | OpenLineage | 2 | 2 | 0 (İ) | OM/DH official consumers; Atlas none (ATLAS-4550 open); no producer anywhere |
| D11.1 | Connector breadth | 3 | **4** | 1 | DH 116 registered sources with central support status; OM ~95; Atlas 9 |
| D11.2 | Custom connectors | **3** | 2 | 2 | OM scaffold + custom connector on the same scheduler; DH UI ingestion does not run custom sources |
| D11.3 | Event bus | 2 | **3** | 2 | DH consumes and produces on Kafka (or pgQueue); OM consumes only; Atlas Kafka only |
| D11.4 | Alerts / webhooks | **4** | 2 | 0 | OM routing as entities, retries, dead-letter, delivery status; Atlas logs only |
| D11.5 | Plugin/app framework | 3 | 2 | 2 | OM apps installable at runtime; DH plugin types fragmented; Atlas `libext` jars |
| D11.6 | Governance as code | 2 | **3** | 2 | DH YAML for datasets/products/glossary/properties/forms with `diff`/`sync` drift view |
| D12.1 | Footprint | 3 | 3 | 1 | 2 / 2 / 4 mandatory components |
| D12.2 | HA / scale | 3 | 3 (D) | 2 | OM clustered Quartz, active-active test setup; DH stateless GMS; Atlas ZooKeeper active-passive |
| D12.3 | Multi-tenancy | 1 | 1 (D) | 1 | No tenant concept anywhere |
| D12.4 | SSO | **3** | 1 | 2 | OM OIDC/SAML/LDAP + SCIM; DH OSS OIDC only (SAML "not yet supported"); Atlas Kerberos/LDAP/Keycloak/Knox |
| D12.5 | i18n | 1 | 1 | 0 | UI translations only (OM 19 languages, DH 10) |
| D12.6 | Community | 2 | 2 | 2 (D) | See community section — same score, different risks |
| D12.7 | Upgrades | 2 | 2 (D) | 2 | Migration tooling everywhere; no compatibility guarantee |
| D12.8 | Backup / DR | 0 | **2** | 1 (D) | DH quickstart `--backup/--restore` + index rebuild; OM has no backup tool (`restore.py` not registered in the CLI) |

\* Atlas D3.3 is recorded as 3 in the research, but the rubric's level 2 requires general SQL
parsing (≥3 dialects) and level 3 builds on it; Atlas captures lineage only through runtime
hooks for 7 systems. Read the cell as "runtime-only, automatic" rather than as parity with the
other two.

Domain averages: D1 2.7 / 3.3 / 3.0 · D2 3.0 / 3.2 / 1.5 · D3 2.8 / 3.0 / 2.2 ·
D11 2.8 / 2.7 / 1.5 · D12 1.9 / 1.9 / 1.4 (OM / DH / Atlas). Related: AI-asset lineage (D13.2)
is DH 4 (training job → model → downstream in one graph, `MLModelLineageInfo.pdl:93-119`),
OM 2, Atlas 2.

### Key evidence (`path:line`)

- **OM** lineage store/extract `openmetadata-spec/.../type/entityLineage.json:24-102`,
  `openmetadata-service/.../jdbi3/LineageRepository.java:135-182,433-452`; lineage API and export
  `resources/lineage/LineageResource.java:190-259,402-486`; OpenLineage
  `resources/lineage/OpenLineageResource.java:54,104-184`; events/alerts
  `openmetadata-spec/.../events/eventSubscription.json:98-200`,
  `apps/bundles/changeEvent/AbstractEventConsumer.java:144-200`; connectors
  `ingestion/src/metadata/ingestion/source/` (97 `service_spec.py`); custom connectors
  `ingestion/src/metadata/cli/scaffold.py:11-60`; search
  `openmetadata-service/src/main/resources/json/data/settings/searchSettings.json:89-126`.
- **DH** column lineage `metadata-models/.../dataset/FineGrainedLineage.pdl:44-49`; table lineage
  `UpstreamLineage.pdl:6-27`; SQL aggregator `metadata-ingestion/src/datahub/sql_parsing/sql_parsing_aggregator.py:973-1360`;
  impact UI `datahub-web-react/src/app/entityV2/shared/tabs/Lineage/ImpactAnalysis.tsx:55-87`;
  OpenLineage `metadata-service/openapi-servlet/.../openlineage/controller/LineageApiImpl.java:30-102`;
  connectors `metadata-ingestion/setup.py:1128-1245`; custom properties
  `metadata-models/.../structured/StructuredPropertyDefinition.pdl:104-150`; as-code
  `metadata-ingestion/src/datahub/cli/specific/dataproduct_cli.py:151-186`.
- **Atlas** lineage service `repository/.../discovery/EntityLineageService.java:255-350`, REST
  `webapp/.../rest/LineageREST.java:70-114`; hooks `notification/.../hook/AtlasHook.java:58-126`;
  Hive column lineage `addons/hive-bridge/.../hook/events/CreateHiveProcess.java:232-240`; change
  events `intg/.../model/notification/EntityNotification.java:100-213`; new types
  `webapp/.../rest/TypesREST.java:358-412`; footprint `distro/pom.xml:37-76`.

## Architecture

| Item | OpenMetadata 1.13.3 | DataHub 1.7.0 | Apache Atlas 2.5.0 |
|---|---|---|---|
| Backend | Java 21, Dropwizard 5.0.0 (`pom.xml:80,1067-1071`) | Java 21, Spring 7.0.8; Play 3.0.10 frontend tier (`build.gradle:6,38,58`) | Java **1.8** target, Spring 5.3.39, JanusGraph 1.1.0 (`pom.xml:136,191,1107-1108`) |
| Ingestion | Python framework | Python `acryl-datahub` | None central — per-system Java hooks/bridges installed next to each source |
| **Mandatory infra** | **2**: MySQL or PostgreSQL; Elasticsearch or OpenSearch (search enum has no "none", `elasticSearchConfiguration.json:97-100`) | **2**: MySQL/PostgreSQL/Cassandra; OpenSearch/Elasticsearch (also serves graph and timeseries) | **4**: HBase 2, SolrCloud, Kafka, ZooKeeper (+ HDFS under HBase in practice) |
| On by default but removable | Airflow as pipeline client (`conf/openmetadata.yaml:544`; `NoopClient` or K8s Job operator) | Kafka (`pgqueue` in `quickstart-postgres`, `application.yaml:171-173`) | — |
| Optional | Redis, RDF/Fuseki, external secrets manager | Neo4j, Cassandra, separate MAE/MCE consumers | PostgreSQL backend for JanusGraph (replaces HBase, count stays 4), Ranger |
| Deployment | Docker Compose variants, tarball, Helm (separate repo), K8s operator | Docker Compose (10 profiles), Helm, cloud guides | Tarball + `atlas_start.py`; **no official Docker image or Helm chart** |
| Stated minimum | 4+ cores, 8+ GB RAM, 20+ GB disk (Helm dev README) | 2 CPU, 8 GB RAM, 2 GB swap, 13 GB disk (`docs/quickstart.md:30`) | No statement; example production heap 15 GB for the server alone (`distro/src/conf/atlas-env.sh:41`) |
| Licence | Apache-2.0 server/UI; **`ingestion/` under Collate Community License v1.0** (1 053 of 1 404 Python files) | Apache-2.0 | Apache-2.0 |
| Governance | Single vendor | Single-vendor-led | **ASF top-level project** (PMC, voted releases) |
| Paid edition split | Vendor cloud/enterprise edition exists | **DataHub Cloud** holds change proposals, notifications/subscriptions, assertion evaluation, freshness/volume monitors, access workflows, contract evaluation (`docs/managed-datahub/managed-datahub-overview.md:10-70`) | None |

**"DataHub needs Neo4j" is outdated for 1.7.0.** The graph service defaults to Elasticsearch
(`metadata-service/configuration/src/main/resources/application.yaml:377`,
`GRAPH_SERVICE_IMPL:elasticsearch`; `:1172` "Only required if GraphService type is neo4j");
only 2 of 10 Docker profiles include Neo4j. The catch: the Helm **prerequisites** chart still
enables Neo4j by default (`docs/deploy/kubernetes.md:46-55`) — set `neo4j.enabled: false` and
override the graph implementation.

## Connector coverage for a typical open stack

Classification as in [catalog-capabilities.md](catalog-capabilities.md#connector-coverage-questions-to-ask).

| Component | OpenMetadata | DataHub | Apache Atlas |
|---|---|---|---|
| Kafka | built-in | built-in | separate module (bulk bridge, no runtime hook) |
| Flink | built-in | built-in (incubating) | type model only |
| Iceberg | absent as a catalog connector (Trino marks Iceberg tables, `source/database/trino/metadata.py:283-302`) | built-in (`source/iceberg/iceberg.py:149`) | separate module, partial (via Trino extractor) |
| S3 / MinIO | built-in, MinIO named in code | built-in (S3); MinIO via endpoint override | type model only |
| ClickHouse | built-in | built-in | absent |
| Trino | built-in | built-in | separate module (new in 2.5.0; no column lineage) |
| dbt | built-in | built-in | absent |
| Spark | type model only in this repo (producer agent lives elsewhere) | separate module (Java agent on OpenLineage) | type model only (+ external connector, not verified) |
| Airflow | built-in (+ managed API module) | separate module (plugin) | absent |
| NiFi | built-in | built-in (certified) | absent in repo (NiFi-side reporting task, not verified) |
| Great Expectations | built-in | built-in + separate module | absent |
| PostgreSQL | built-in | built-in | type model only (generic RDBMS) |
| Schema Registry | built-in (inside Kafka connector) | built-in (standalone) | separate module (inside Kafka bridge) |
| **Working coverage** | **11 / 13** | **13 / 13** | **4 / 13** |

Counts: OpenMetadata ~95 connectors (97 `service_spec.py` minus a duplicate and a template;
README says "84+"); DataHub 116 source plugins (53 certified, 49 incubating, 14 testing, 1
unknown) plus 39 transformers and 7 sinks; Atlas 9 source systems (7 hooks + 4 bridges, three
overlap, + Trino extractor).

## Community and sustainability (snapshot 2026-08-11)

| Indicator | OpenMetadata | DataHub | Apache Atlas |
|---|---|---|---|
| Commits, last 12 months (default branch) | 4 414 | 3 140 | **178** |
| Stable releases, last 12 months | 59 (two maintenance lines in parallel) | 15 | **1** |
| Release gap | ~6 days | ~24 days (majors ~2.5 months) | 25 and 16 months between the last three |
| Contributors (all time, GitHub) | 424 | 411 | 145 (ASF accounts not counted) |
| Open / closed issues | 724 / 9 788 | 518 / 2 283 | 1 053 / 4 266 (**JIRA**, not GitHub) |
| Governance | single vendor | single-vendor-led | foundation (ASF) |
| D12.6 | 2 | 2 | 2 (D) |

Atlas's GitHub repo is an ASF mirror created in 2017; it has no GitHub issues or releases, so
stars (2 132 vs 14 848 and 12 516) are not comparable. Its JIRA is alive (283 opened, 251
resolved in 12 months) but code flow is an order of magnitude lower: maintained, not actively
developed. Foundation governance is the one structural advantage — it is the only option
without single-vendor dependency. OpenMetadata's release rate also means **upgrade load**.

## Which catalog for which situation

| Situation | Leaning | Because | Watch out for |
|---|---|---|---|
| Hadoop/Hive-centred estate already running HBase, Solr, Kafka, ZooKeeper, with Ranger | **Atlas** | Runtime Hive lineage with column expressions; Ranger/external authorisation (D6.7 = 3, highest of the three); foundation governance | 9 connectors, no OpenLineage, no alerts, Java 1.8, slow releases, no container distribution |
| Broad, modern stack (lakehouse, streaming, many SQL engines, Iceberg) where connector coverage and lineage depth rule | **DataHub** | 116 sources, 13/13 on the example stack, column transforms, Kafka in/out, metadata-as-code with drift view, backup tooling | OSS lacks proposals/approvals, collaboration, notifications and assertion evaluation (cloud-only); OSS SSO is OIDC only |
| Governance team that needs workflows, alerting, quality and stewardship in the free edition | **OpenMetadata** | Alerts 4, custom connectors on the same scheduler, search ranking 4, SAML/LDAP/SCIM, quality and approvals in OSS | No new entity types, custom properties have no required flag, no Iceberg connector, Spark lineage producer not in repo, no backup tool, `ingestion/` licence |
| Smallest footprint | OpenMetadata or DataHub | Both run on relational DB + search | DataHub Helm prerequisites still bring Neo4j unless disabled |
| Must model new entity types (e.g. regulatory objects) | DataHub or Atlas | D1.1 = 4 | DataHub new types get no UI automatically |
| Lineage must flow in from many engines without lock-in | Any OpenLineage consumer (OM, DH) | Emit OpenLineage from pipelines; swap catalogs later | No catalog exports lineage back as OpenLineage |

These leanings follow from the measured cells; re-score with your own requirement weights
before deciding, and re-check versions — all three products release often enough (Atlas
excepted) that cells move between releases.
