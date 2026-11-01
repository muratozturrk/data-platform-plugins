# Components, Deployment, Operations (OpenMetadata 1.13.3)

Paths are relative to the OpenMetadata repo at tag `1.13.3-release`. `SVC` =
`openmetadata-service/src/main/java/org/openmetadata/service/`, `SPEC` =
`openmetadata-spec/src/main/resources/json/schema/`. Evidence: K = source code, D = docs,
K+ = verified while writing this skill. D-scores (0–4) are from the governance rubric
(`openmetadata-governance-assessment`).

## Runtime Topology

```
 Browser ──> openmetadata-server (Dropwizard/Jetty, :8585, UI bundled)
                │  JDBC                    │ REST
                ▼                          ▼
        MySQL | PostgreSQL        Elasticsearch | OpenSearch
                ▲
                │ REST + bot JWT
 ingestion workers (Python "metadata" workflows)
   started by: AirflowRESTClient → Airflow (+ openmetadata-airflow-apis plugin)
            or K8sPipelineClient → K8s Jobs / OMJob operator
            or nothing (NoopClient) → you run `metadata ingest -c x.yaml` yourself
 optional: Redis cache · Fuseki (RDF/SPARQL) · external secrets manager · MCP server app
```

The server is the only writer of metadata; ingestion pushes everything through the REST API.
Config is one file, `conf/openmetadata.yaml`, where nearly every key is `${ENV_VAR:-default}`.
Entry point: `SVC/OpenMetadataApplication.java`.

## Components

| Component | 1.13.3 facts | Evidence |
|---|---|---|
| Server | Java 21 (`maven-compiler-plugin` source/target 21), Dropwizard 5.0.0, embedded Jetty; project version 1.13.3 | `pom.xml:29,80,261-263,303-319,1067-1071` (K) |
| UI | React 18, TypeScript 5, Ant Design 4.24.16, Vite 7.3.5, Node ≥ 22; extra component lib `openmetadata-ui-core-components/` (Tailwind v4) | `openmetadata-ui/src/main/resources/ui/package.json:5-6,104,137,145,260,15-16,262` (K) |
| Relational DB | Default driver MySQL; PostgreSQL alternative (`assumeMinServerVersion:12`). No stated minimum DB version found (searched README, CLAUDE.md, DEVELOPER.md, conf); integration tests use MySQL 8.3.0 / PostgreSQL 15 | `conf/openmetadata.yaml:255-262,286-300`; `openmetadata-integration-tests/README.md:22-25` (K) |
| Search | `searchType` enum has only `elasticsearch`, `opensearch` — search is mandatory. Repo docs: ES 7.17+ or OpenSearch 2.6+; quickstart ships ES 9.3.0 | `conf/openmetadata.yaml:460-466`; `SPEC/configuration/elasticSearchConfiguration.json:96-99`; `CLAUDE.md:17`; `DEVELOPER.md:394`; `docker/docker-compose-quickstart/docker-compose.yml:42` (K/D) |
| Semantic search | kNN + BM25 hybrid (RRF), local embeddings via DJL possible; off by default, **OpenSearch only** (D2.3 = 2). `embeddingProvider` options: openai, bedrock (default), google, djl | `SVC/search/vector/OpenSearchVectorService.java:37,88,149-156`; `conf/openmetadata.yaml:494-499` (K) |
| Ingestion | Python package `openmetadata-ingestion` 1.13.3.0, CLI `metadata`; `requires-python >=3.9` (repo guide says 3.10–3.11 — inconsistency) | `ingestion/pyproject.toml:8-9,16,36-37`; `CLAUDE.md:15,23` (K) |
| Pipeline service client | `enabled` default `true`, class `AirflowRESTClient`; alternatives `K8sPipelineClient` (`useOMJobOperator: true`) and `NoopClient`; class loaded by reflection | `conf/openmetadata.yaml:543-547,588-615`; `SVC/clients/pipeline/PipelineServiceClientFactory.java:91`; `SVC/clients/pipeline/noop/NoopClient.java` (K) |
| Cache | Redis optional, `provider: none` default | `conf/openmetadata.yaml:732-737,744-775` (K) |
| RDF / knowledge graph | Apache Jena Fuseki, `rdf.enabled: false`; exposes JSON-LD/Turtle of the graph when on | `conf/openmetadata.yaml:716-730`; `docker/docker-compose-quickstart/docker-compose-rdf.yml` (K) |
| Metrics | `eventMonitor: prometheus` (or cloudwatch) — embedded `/metrics`, no Prometheus server needed to run | `conf/openmetadata.yaml:526-527` (K) |
| Messaging | **No Kafka, no ZooKeeper** anywhere in quickstart compose or `conf/openmetadata.yaml`; Kafka is only an ingestion source | T3 footnote (K) |
| MCP server | `openmetadata-mcp` module; started by reflection (`org.openmetadata.mcp.McpServer`) and shipped as bundled `McpApplication` | `SVC/OpenMetadataApplication.java:442`; `openmetadata-service/src/main/resources/json/data/app/McpApplication.json` (K+) |

### Pipeline client choice

| Option | Use when | Notes |
|---|---|---|
| `AirflowRESTClient` (default) | You already run Airflow or accept it | Needs the OM Airflow plugin (`openmetadata-airflow-apis/`); quickstart runs it as the `ingestion` container (`docker-compose.yml:505-547`) |
| `K8sPipelineClient` | Kubernetes, no Airflow | Each run is a K8s Job; OMJob CRD guarantees exit handlers (status reporting) on OOM/eviction (`openmetadata-k8s-operator/README.md:1-16`). Default image `docker.getcollate.io/openmetadata/ingestion-base:latest` (`conf/openmetadata.yaml:590`) — mirror and pin it when air-gapped |
| `NoopClient` | You schedule ingestion yourself (existing Airflow, cron, CI) | UI can no longer deploy/trigger pipelines; run `metadata ingest -c workflow.yaml` from your scheduler |

## Authentication and Authorization

| Item | 1.13.3 | Evidence |
|---|---|---|
| Providers | `basic`, `azure`, `google`, `okta`, `auth0`, `aws-cognito`, `custom-oidc`, `ldap`, `saml`, `openmetadata` (bot JWT) | `SPEC/entity/services/connections/metadata/openMetadataConnection.json:20-40` (K+) |
| One provider at a time | `authenticationConfiguration.provider` is single-valued (default `basic`) | `conf/openmetadata.yaml:344-350` (K) |
| SCIM 2.0 | User/group provisioning, DELETE deactivates | `SVC/resources/scim/ScimResource.java:37,92-333` (K) |
| Roles from IdP / LDAP | `useRolesFromProvider`, LDAP group→role mapping | `conf/openmetadata.yaml:340,418` (K) |
| Authorizer | `DefaultAuthorizer` + `JwtFilter`, both class names configurable | `conf/openmetadata.yaml:331-332`; `SVC/OpenMetadataApplication.java:888-932` (K+) |
| Multi-tenancy | None; only Team/Domain naming + `DomainOnlyAccess` policy (D12.3 = 1) | `openmetadata-service/src/main/resources/json/data/policy/DomainAccessPolicy.json:1-27` (K) |

D12.4 (SSO/identity) = 3. Policy and RBAC detail belongs to the governance skills.

## Deployment Options

| Option | Artefact | Note |
|---|---|---|
| Docker Compose | `docker/docker-compose-quickstart/` (MySQL+ES default; `-postgres`, `-opensearch-standalone`, `-rdf` + Fuseki variants); `docker/docker-compose-openmetadata/` (server only, external DB/search); `docker/docker-compose-ingestion/` | Migration container `execute-migrate-all` runs `openmetadata-ops.sh migrate` before the server starts (`docker-compose.yml:60-63`) |
| Tarball / bare metal | `openmetadata-dist/src/main/assembly/binary.xml:18-59` | `bin/openmetadata-server-start.sh` builds the classpath from `libs/*.jar` + `EXT_CLASSPATH` (`:42-49`) |
| Kubernetes / Helm | **Not in this repo** (`charts/` absent) — separate `open-metadata/openmetadata-helm-charts` repo (`docker/development/helm/README.md:3`) | Extra version-coupling dependency to track at upgrade time |
| K8s operator | `openmetadata-k8s-operator/` — only for ingestion jobs (`OMJob` CRD), not the server | D11.6 notes no Terraform provider |

## Footprint

The only explicit sizing in the repo: **4+ cores, 8 GB+ RAM, 20 GB+ disk** —
`docker/development/helm/README.md:17-20`, in a local Helm-test context; general prerequisites live
on docs.open-metadata.org (no `openmetadata-docs/` dir in the repo). Indirect data: server heap
`-Xmx1G -Xms1G` (`docker-compose.yml:242`), ES heap 1 GB (`:45`), OpenSearch standalone 2–4 GB
(`docker-compose-opensearch-standalone.yml:19`). For comparison: DataHub v1.7.0 quickstart
2 CPU / 8 GB / 13 GB disk; Atlas 2.5.0 example server heap alone 15 GB plus HBase, Solr, Kafka,
ZooKeeper. Size the DB and search cluster on your entity and column count, not on these numbers.

## High Availability and Scaling (D12.2 = 3)

- Server instances are stateless towards each other; all share one DB and one search cluster.
- App and event-subscription schedulers use **clustered Quartz** (`isClustered=true`,
  `JobStoreTX`, `QRTZ_` tables in the shared DB): `SVC/apps/scheduler/AppScheduler.java:60-73`,
  `SVC/events/scheduled/EventSubscriptionScheduler.java:104-131` (K).
- The repo ships a three-server active-active test bed sharing one DB and search engine:
  `docker/development/distributed-test/README.md:1-33` (K).
- Not found in 1.13.3: metric-driven autoscaling, multi-region deployment.
- HA of the catalog = HA of the DB (replication/failover) + HA of the search cluster + ≥2 servers
  behind a load balancer. Note the PostgreSQL JDBC block exposes `targetServerType` and
  `loadBalanceHosts` (`conf/openmetadata.yaml:298-300`).

## Upgrades and Migrations (D12.7 = 2)

- `./bootstrap/openmetadata-ops.sh migrate` applies schema, search-index and secret migrations;
  applied versions are tracked in `SERVER_CHANGE_LOG` (`SVC/util/OpenMetadataOperations.java:1177-1206`,
  `bootstrap/MIGRATION_SYSTEM.md:34-65,147-153`). Flyway + native SQL under `bootstrap/sql/migrations/`;
  `migrationConfiguration.extensionPath` allows extra migration scripts (`conf/openmetadata.yaml:324-327`).
- 82 native migration dirs from 1.1.0 to 1.13.3 show a regular release cadence (D12.6).
- Not found: written deprecation policy, backward-compatibility promise for minor versions,
  **rollback path**. `SECURITY.md:10-11` still lists 1.11.x/1.10.x as supported.
- `recover` exists for data lost by an earlier Flyway `--force` issue (`OpenMetadataOperations.java:1207-1215`) —
  a hint that forced migrations have bitten users.

## Backup and DR (D12.8 = 0)

Code side: no `backup.py`; `ingestion/src/metadata/cli/restore.py` exists but is **not registered**
in the CLI (`ingestion/src/metadata/cmd.py:46-67`) — dead code; no backup docs in the repo.
Responsibility is entirely the database's. Mitigations:

1. Only two stateful components, and the search index is derived: rebuild it with
   `openmetadata-ops.sh reindex` (`OpenMetadataOperations.java:1431`) or the `SearchIndexingApplication`.
   **Back up only the relational DB.**
2. Use native DB tooling: base backup + WAL archiving / binlog for point-in-time recovery; replicate
   to the DR site asynchronously if you have an RPO/RTO target.
3. Because migrations have no rollback, take a DB snapshot before every upgrade and test-restore
   each backup **on the same OM version** that produced it; restorability of an old backup into a
   newer version is undetermined.
4. Also back up `conf/openmetadata.yaml`, the `FERNET_KEY` (stored connector secrets are unreadable
   without it) and JWT signing keys.

The practice above is a recommendation built on the evidence, not an OM feature.

## Operations CLI (`bootstrap/openmetadata-ops.sh`)

Subcommands verified in `SVC/util/OpenMetadataOperations.java` (K+): `info`, `validate`, `repair`,
`migrate`, `recover`, `changelog`, `reindex`, `reindex-rdf`, `reindexdi`, `reembed`,
`create-indexes`, `drop-indexes`, `deploy-pipelines`, `migrate-secrets`, `syncAlertOffset`,
`install-app`, `delete-app`, `create-user`, `reset-password`, `regenerate-bot-tokens`,
`check-connection`, `search-fitness`, `analyze-tables`, `db-tune`, `get-/update-/remove-security-config`,
`dbServiceCleanup`, `relationshipCleanup`, `tagUsageCleanup`, `cleanup-flowable-history`,
`setOpenMetadataUrl`, `configureEmailSettings`, and the destructive **`drop-create`** (drops all
tables, recreates indexes — `:1094-1099`).

## Air-Gapped Checklist

- Mirror images (`docker.getcollate.io/openmetadata/server|ingestion:1.13.3`, ingestion-base for K8s).
- Keep `naturalLanguageSearch`/semantic search off, or use the `djl` embedding provider (local) —
  the other providers are external APIs.
- Set `FERNET_KEY`, JWT keys, admin principals; replace default DB/RDF credentials in `conf/openmetadata.yaml`.
- Pin the Helm chart version from the separate chart repo to the server version.
- UI ships 19 locales including `tr-tr`; user-entered content (descriptions, glossary) is
  single-language and the language choice lives only in a browser cookie (D12.5 = 1).

## Why OM over the Alternatives (Architecture Only)

| | OpenMetadata 1.13.3 | DataHub v1.7.0 | Apache Atlas 2.5.0 |
|---|---|---|---|
| Mandatory deps | 2 (DB + search) | 2 (DB + OpenSearch); Kafka replaceable by `pgqueue`; Neo4j **not** required by code/Docker defaults, but the Helm `prerequisites` chart still enables it | 4 (HBase, SolrCloud, Kafka, ZooKeeper) |
| Licence | Apache-2.0 core + CCL `ingestion/` | Apache-2.0, but key governance features are DataHub Cloud only | Apache-2.0, ASF top-level project |
| Governance model | Single vendor (Collate) | Single vendor (DataHub/Acryl) | ASF PMC |

An older argument that "DataHub requires Neo4j" is stale for v1.7.0; dependency count alone does
not separate OM from DataHub — the open-source vs commercial feature split does.
