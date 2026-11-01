# Missing Connectors and Workarounds (OpenMetadata 1.13.3)

These gaps do not show up in a generic capability score — OpenMetadata scores 3/4 on connector
breadth (D11.1) — but appear as soon as OpenMetadata is placed next to a concrete on-prem stack.
Impact and ease scores (1–5, priority = impact × ease) come from the research gap guide; the full
catalogue is in `openmetadata-gap-closing`. Paths are relative to the OpenMetadata source at
`1.13.3-release`.

## Summary

| Gap | Evidence | Impact · Ease · Priority | Do now | Durable fix |
|---|---|---|---|---|
| No Iceberg connector | No `*iceberg*` in `ingestion/src`, `openmetadata-spec/src`; not in `databaseService.json` enum | 5 · 4 · 20 | Catalogue via Trino; snapshot facts via API | Iceberg connector (REST/HMS/JDBC catalog) |
| No Spark producer in repo | `sparkConnection.json` has no fields; no `source/pipeline/spark/` | high (not scored separately) | OpenLineage → OM; SQL-at-deploy parsing | Same; agent outside repo |
| Flink without lineage | `pipeline/flink/metadata.py:125-128` empty | — | OpenLineage or API edges | Same |
| NiFi without dataset lineage | `pipeline/nifi/metadata.py:237-245`, issue #6950 | — | API edges / orchestrator outlets | Upstream fix |
| No catalog↔Ranger policy link | `securityService` scaffolding, `children=[]` (`security/security_service.py:68`) | 5 · 4 · 20 | Drift report, no code | Sync job; or fill the extension point |
| Vault not a secrets provider | `secretsManagerProvider.json:8` | — | Env injection + `${VAR}` | Own provider |
| No change events to Kafka | `eventSubscription.json:98-108` | D11.3 = 2 | Custom consumer `className`, or webhook → bridge → Kafka | Same |

## 1. Iceberg on MinIO

**Why it is the most critical one.** In a lakehouse where Bronze, Silver and Gold are all Iceberg
tables on MinIO, the storage backbone is the one component OpenMetadata cannot catalogue natively.
Consequences: (a) most data assets are represented only second-hand; (b) lineage breaks in the
middle ("Kafka → Bronze files → ? → ClickHouse") wherever Spark/dbt transform Iceberg tables;
(c) when erasure requests are executed as Iceberg `DELETE`/`UPDATE` (delete files, new snapshots),
the catalogue cannot show **on which asset, when and for which request** a deletion happened —
the evidence chain for erasure duties (e.g. KVKK — Law No. 6698 on the Protection of Personal
Data — Art. 7 and Art. 11(1)(e); see `openmetadata-kvkk-compliance`) breaks here. Not legal advice.

**What already works (K):**
- Trino connector marks tables in an Iceberg-backed catalog as `TableType.Iceberg`
  (`database/trino/metadata.py:283-302`), so schema, profiling, usage and query lineage are
  available through Trino.
- Athena (only) maps Iceberg table properties to custom properties via `includeCustomProperties`
  (`athena/metadata.py:390`) — a pattern to copy, not something Trino does.
- Kafka Connect lineage understands Iceberg sink `iceberg.tables` (`kafkaconnect/constants.py:31`).

**What is missing:** snapshot history, partition spec, delete files, table properties for Trino-
or Spark-written tables; anything for Iceberg tables no Trino catalog exposes.

**Route (from the gap guide, ease 4):**
1. *One week — measure.* Ingest the Trino Iceberg catalog and list what is and is not visible. The
   gap may be smaller than expected; measure before building.
2. *Two weeks — compare.* List what DataHub's built-in `IcebergSource`
   (`datahub/metadata-ingestion/src/datahub/ingestion/source/iceberg/iceberg.py:149`, plus
   `iceberg_profiler.py`) extracts and check it against your erasure-evidence needs.
3. *Then — prototype one Silver table end to end.* A connector is a Python module implementing
   the database source base classes (`ingestion/src/metadata/ingestion/source/database/`), talking
   to the Iceberg catalog you run (REST, Hive Metastore or JDBC — an architecture decision that goes
   into the connection schema). Mapping: Iceberg table → `Table`; schema → columns; partition spec →
   table partition fields. No new entity type is needed.
4. *Decision point — snapshots.* Existing table fields may not hold snapshot history; options are
   custom properties (`openmetadata-spec/.../type/customProperty.json:1-71`, see
   `openmetadata-architecture`) or a profile-style time series. This is where erasure evidence will
   come from, so design it first.

**No-fork packaging:** ship the connector as your own package and run it through a
`CustomDatabase` connection with `sourcePythonClass` (see [ingestion-workflows.md](ingestion-workflows.md) §7).
Code inside `ingestion/` is Collate Community License v1.0; publishing a derived connector as open
source is subject to that licence — decide consciously.

**Interim snapshot evidence without a connector:** a scheduled job (e.g. an Airflow task) reads
snapshot/delete metadata from the engine — Trino's Iceberg connector exposes metadata tables such
as `"<table>$snapshots"` (Trino documentation, not OpenMetadata) — and writes the facts onto the
Trino-catalogued table as custom-property values through the OM API.

**Avoid:** pointing the `datalake` or `s3` connector at the warehouse prefix. They read data and
metadata files as independent tables/containers (`readers/dataframe/reader_factory.py:38-54`), see
deleted rows in un-compacted files, and know nothing about snapshots. The datalake JSON reader can
extract columns from one Iceberg metadata JSON (`utils/datalake/datalake_utils.py:748-772`) — useful
for a schema check, not for cataloguing.

## 2. Spark

Category-wide weakness: DataHub ships a separate Java agent, Atlas only a type model; in
OpenMetadata 1.13.3 the repository has the `SparkLineage` edge type (`entityLineage.json:70`) and an
empty `sparkConnection.json`, but no producer code. Routes, best first:

| Route | Works for MinIO/Iceberg writes? | Notes |
|---|---|---|
| OpenLineage Spark integration (external project) → `POST /api/v1/openlineage/lineage` or Kafka topic → `openlineage` connector | Yes, if events carry the catalog table identifier as a symlink and the schema exists in OM | See [lineage.md](lineage.md) §6 resolution rules; capture one event and test |
| Parse job SQL at deploy time with `metadata lineage` against the Trino service | Yes for Spark SQL whose table names match Trino FQNs | Parsed in the service's dialect; DataFrame code is invisible |
| Spline agent + Spline server → `spline` connector | **No** — only `jdbc:` and `dbfs:` datasources resolve (`spline/metadata.py:135-160`) | Fine for JDBC targets only |
| Explicit edges from the job (`PUT /api/v1/lineage`, SDK `add_lineage`) | Yes | Most control, most code |
| OpenMetadata's own Spark agent | Not verifiable — not in this repository | Check its source before relying on it |

## 3. Flink

The connector gives jobs, tasks and run status (useful for failure alerts via
`matchPipelineState`, D10.4 = 3) but no lineage. Options: OpenLineage events if your Flink version
and setup emit them (external; verify), or register topic→table edges from the deployment pipeline
with `PUT /api/v1/lineage`, attaching the Flink `Pipeline` entity in `lineageDetails.pipeline` so
the job sits on the edge. Topic entities come from the Kafka connector; sink tables from the
engine connectors.

## 4. NiFi

Process groups become pipelines with status and pipeline→pipeline edges; processor-level
dataset lineage is an open issue (#6950). If an orchestrator (Airflow) triggers the flows, declare
the datasets as task `outlets`; otherwise post explicit edges with the NiFi pipeline on the edge.

## 5. Ranger (policy link)

**State (K):** OpenMetadata knows which columns are personal data (PII auto-classification, 41
types, D6.1 = 3); Ranger applies masking and row filters; nothing connects them. A designed but
empty extension point exists — `securityService` with the single enum value `Ranger`
(`openmetadata-spec/.../entity/services/securityService.json:5,21`), `connections/security/rangerConnection.json`
(`hostPort`, `authType`), `metadataIngestion/securityServiceMetadataPipeline.json`,
`openmetadata-service/src/main/resources/json/data/testConnections/security/ranger.json`, base class
`SecurityServiceSource` whose topology has `children=[]`. Missing: a `ranger` plugin in `setup.py`
and a `service_spec.py`. D6.7 (delegating OM's own authorisation decisions) stays 1; Atlas scores 3
with a native Ranger authorizer (`AtlasAuthorizerFactory.java:32-86`, Apache-2.0).

**Silent risk:** a column tagged `PII.Sensitive` looks protected in the catalogue while Ranger has
no masking policy for it — or Ranger masks a column the catalogue does not tag, and someone later
removes the "unexplained" policy.

**Right expectation:**

| Wrong expectation | Right expectation |
|---|---|
| The catalogue masks data | The catalogue tells Ranger which columns must be masked |
| The catalogue applies row filters | The catalogue verifies filters are consistent with tags |
| The catalogue makes access decisions | The catalogue keeps the reason and the evidence |

**Route:**
1. *Start with a drift report, no code:* export the columns tagged `PII.Sensitive` (or your own
   special-category tag) from OM, export the columns covered by Ranger masking policies, compare.
   A day's work; the size of the difference decides the priority.
2. *Direction 1 — catalogue → Ranger (easy, do first):* a job reads tagged columns from the OM API
   and creates or proposes masking policies through Ranger's documented policy REST API; trigger it
   from an OM webhook on tag changes (D11.4 = 4: rule filters, retries, dead-letter, delivery status).
3. *Direction 2 — Ranger → catalogue (drift):* scheduled comparison reporting "tagged but no
   policy" and "policy but untagged" — a natural agentic task (see `openmetadata-capability-design`).
4. *Direction 3 — full delegation (optional, hardest):* a custom authorizer via
   `authorizerConfiguration.className` (`conf/openmetadata.yaml:330-331`) moves OM's own decisions
   to Ranger; raises D6.7 but adds little over 1–2.

Run the bridge as an Airflow task outside the OM codebase: it stays your code and avoids the
ingestion licence question. Filling the `securityService` extension point (a real Ranger connector
that ingests policies) is the in-product alternative.

## 6. HashiCorp Vault

Server `secretsManager` defaults to `db` (connection secrets Fernet-encrypted in the OM database,
`DBSecretsManager.java:50`); named external providers are AWS, Azure, GCP, Kubernetes. Choices:
- Accept `db` and protect the OM database and its Fernet key.
- Keep secrets out of stored connections: `storeServiceConnection: false`, run ingestion from your
  own scheduler, inject credentials from Vault into the runner's environment (e.g. Vault Agent) and
  reference them as `${VAR}` (CLI expansion, `config/common.py:109`) or `{{ env('VAR') }}` in the GE
  config.
- Write a provider: Java `SecretsManager` implementation + factory entry on the server
  (`openmetadata-service/.../secrets/`), Python counterpart in `ingestion/src/metadata/utils/secrets/`.
- A Vault-to-Kubernetes secret sync feeding the `kubernetes` provider is conceivable but was not
  evaluated in the research.

## 7. Change events to Kafka

OpenMetadata reads from Kafka but has no broker publisher; subscription types are Webhook, Slack,
MsTeams, GChat, Email, ActivityFeed, GovernanceWorkflowChangeEvent (`eventSubscription.json:98-108`).
Streaming jobs therefore cannot react natively to catalogue events (new PII tag, retention change,
revoked certification). Two routes, neither needs a fork:

- **Custom consumer class (in-process).** A subscription's `className` (`eventSubscription.json:248-251`)
  is loaded by name and must extend `AbstractEventConsumer`
  (`EventSubscriptionScheduler.java:184-191`); its jar goes on `EXT_CLASSPATH`. The class can produce
  to Kafka directly and reuses OM's offset and retry machinery. Verified from the loading code only:
  no such consumer has been built or run, and it will not show up as a typed destination in the UI.
  Details in `openmetadata-architecture` (extension points).
- **Webhook bridge (out-of-process).** A webhook subscription filtered with SpEL rules → a small
  bridge service that writes each event to a Kafka topic keyed by entity id → Flink/consumers.
  Webhooks retry and dead-letter (D11.4 = 4) but lack Kafka's replay; make the bridge idempotent
  and alert on the subscription's delivery status.

Pick the custom consumer when you can ship and upgrade a jar with the server; pick the bridge when
the server image must stay untouched.
