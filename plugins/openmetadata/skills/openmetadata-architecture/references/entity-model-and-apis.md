# Entity Model, Custom Properties, Versioning, APIs (OpenMetadata 1.13.3)

Paths relative to the repo at `1.13.3-release`. `SVC` = `openmetadata-service/src/main/java/org/openmetadata/service/`,
`SPEC` = `openmetadata-spec/src/main/resources/json/schema/`. K = source code, K+ = verified while
writing this skill. D-scores are rubric scores (0–4).

## The Spec Is the Model (D1.3 = 3)

- All entities, API requests, configs and events are **JSON Schema draft-07** files under `SPEC`,
  packaged as the separately versioned `openmetadata-spec` artefact.
- Java classes are generated with `jsonschema2pojo-maven-plugin` (`openmetadata-spec/pom.xml:171-197`);
  the Python ingestion pydantic models (`make generate`, `Makefile:53`) and UI TypeScript types
  (`generate_ts.sh`) are generated from the same schemas. One schema change therefore ripples into server, UI and ingestion.
- Standards mappings present: ODCS v3.1.0 data contracts (`SPEC/entity/datacontract/odcs/odcsDataContract.json:1-12`),
  ODPS v4.1, and an RDF ontology mapped to DCAT/PROV/SKOS (`openmetadata-spec/src/main/resources/rdf/ontology/openmetadata.ttl:26-33`).
- Not found: a published compatibility test suite for schema versions.

## Entity Types (D1.1 = 3)

The server registers a `Type` record for every schema in `SPEC/entity/` that carries the
`@om-entity-type` annotation (`openmetadata-spec/.../utils/JsonUtils.java:94,520-550`) — **37 in 1.13.3** (K+):

| Area | Entity types |
|---|---|
| Data assets | table, tableColumn, database, databaseSchema, storedProcedure, topic, container, searchIndex, apiCollection, apiEndpoint, dashboard, dashboardDataModel, dashboardDataModelColumn, chart, pipeline, mlmodel, metric, directory, file, spreadsheet, worksheet |
| Governance | glossary, glossaryTerm, classification, tag, domain, dataProduct, dataContract |
| AI | aiApplication, llmModel, promptTemplate, agentExecution, aiGovernancePolicy, mcpServer, mcpExecution |
| Other | notificationTemplate, learningResource |

**You cannot add an entity type at runtime.** `SVC/resources/types/TypeResource.java:78-85` says so
explicitly: "This does not support creating new custom data types — only custom properties using
existing OpenMetadata data types." A new entity type means editing the spec, repository, resource,
search index and UI — a fork.

Common anatomy of an entity: `id`, `name`, `fullyQualifiedName`, `displayName`, `description`,
`version`, `updatedAt/By`, `owners`, `tags`, `domains`, `changeDescription`, `deleted`, and
`extension` (custom property values, `SPEC/entity/data/table.json:1266-1269`).

## Custom Properties (D1.2 = 2)

Field types are the definitions annotated `@om-field-type` in `SPEC/type/basic.json` and
`SPEC/type/customProperties/complexTypes.json` — **17 types** (K+):

| Group | Types |
|---|---|
| Scalars | `string`, `integer`, `number`, `markdown`, `email`, `sqlQuery` |
| Time | `timestamp`, `timeInterval`, `duration`, `date-cp`, `dateTime-cp`, `time-cp` (date/time formats configurable) |
| Choice | `enum` (configured values, optional multi-select — `SPEC/type/customProperties/enumConfig.json:9-19`) |
| References | `entityReference`, `entityReferenceList` (allowed entity types configurable) |
| Structured | `table-cp` (columns defined in `tableConfig.json`), `hyperlink-cp` |

How it works:
- Definition: `PUT /v1/metadata/types/{id}` (`addProperty`, `TypeResource.java:503-518`) or UI;
  list with `GET /v1/metadata/types/name/{entityType}/customProperties`.
- Validation: values in `extension` are validated against the property's JSON Schema
  (`SVC/jdbi3/TypeRepository.java:164-192`).
- Search: typed values are indexed (`customPropertiesTyped`) and usable as facets
  (`SVC/search/CustomPropertySearchFields.java:35-60`).
- CSV: export/import carries them in the `extension` column.
- Column level: `tableColumn` and `dashboardDataModelColumn` are entity types, so columns can carry custom properties.

Limits (K):
- Schema: `required` is only `name`, `description`, `propertyType` (`SPEC/type/customProperty.json:69`) —
  **no mandatory/required flag**: enforcement needs governance workflows or rules.
- Only entity-category types; **not** users, teams, services, policies, roles (`TypeRepository.java:164-167`).
- Field-category types cannot be extended; **no new property types** without a spec change.
- Property names must be unique per entity; `/` and `~` are reserved (JSON Pointer).

## Versioning, Deletion, Rollback (D1.4 = 3)

- Every change bumps `version` as **Major.Minor**, starting at 0.1; backward-compatible change →
  Minor, incompatible → Major (`SPEC/type/entityHistory.json:9-104`). `changeDescription` lists
  fields added/updated/deleted with old and new values.
- History API: `GET /{id}/versions` and `GET /{id}/versions/{version}` on every entity resource
  (e.g. `SVC/resources/databases/TableResource.java:385,408`). UI shows a version timeline.
- Deletion: soft delete by default; `hardDelete=true` and `recursive=true` query params;
  `PUT /restore` undoes a soft delete (`TableResource.java:716-743,785`).
- **Rollback** to an earlier version is not a generic API: it exists only as the governance-workflow
  node `RollbackEntityTask`, which reverts to the last *Approved* version
  (`SVC/governance/workflows/elements/nodes/automatedTask/RollbackEntityTask.java`; `SVC/jdbi3/EntityRepository.java:2408-2507`).
- Updates: `PUT` (create-or-update) and `PATCH` with `application/json-patch+json` (RFC 6902) (`TableResource.java:518-528`).

## APIs, SDKs, CLI (D1.5 = 3)

| Surface | 1.13.3 | Evidence |
|---|---|---|
| REST | JAX-RS resources under `/api/v1/...`; OpenAPI (JSON + YAML) generated at build by `swagger-maven-plugin-jakarta` | `openmetadata-service/pom.xml:1180-1215` (K) |
| Auth for machines | Bot users with JWT tokens (`regenerate-bot-tokens` ops command); OAuth/JWT for users | `conf/openmetadata.yaml:454` (K) |
| Java SDK | `openmetadata-sdk` ("Official Java SDK … Stripe-style"), Apache-2.0; plus Feign-based `openmetadata-clients/openmetadata-java-client` | `openmetadata-sdk/pom.xml:12-16` (K) |
| Python SDK | `OpenMetadata` client in `ingestion/src/metadata/ingestion/ometa/ometa_api.py`, shipped in `openmetadata-ingestion` — **CCL-licensed tree** | `ingestion/LICENSE:1-2` (K) |
| CLI `metadata` | `ingest`, `ingest-dbt`, `usage`, `profile`, `test`, `lineage`, `app`, `classify`, `webhook`, `scaffold-connector` — workflows are declarative YAML | `ingestion/src/metadata/cmd.py:46-67` (K) |
| Ops CLI | `bootstrap/openmetadata-ops.sh` (server-side admin; see components reference) | `SVC/util/OpenMetadataOperations.java` (K+) |
| MCP | `openmetadata-mcp` module, `McpServerResource`, `McpApplication` bundled app | K+ |

Not found: a written API backward-compatibility commitment; a Terraform provider (D11.6 = 2).

## CSV Import / Export (D1.7 = 2)

- Endpoints on supporting resources: `GET /name/{name}/export`, `/exportAsync`,
  `PUT /name/{name}/import`, `/importAsync`, with `dryRun` (`TableResource.java:587-670`;
  generic logic `SVC/resources/EntityResource.java:788-1060`).
- Covered: database service → database → schema → table (recursive), glossary, team, user,
  test cases — with tags, owners, domains and `extension` (column docs:
  `openmetadata-service/src/main/resources/json/data/entity/entityCsvDocumentation.json:5-183`).
- Not covered: dashboards, topics, ML models, containers, pipelines.
- With RDF enabled the whole graph can be pulled as JSON-LD/Turtle.
- CSV import is a one-way push; no drift detection between a Git copy and the live catalog (D11.6).

## Change Events and Subscriptions (D1.6 = 3, D11.4 = 4)

**ChangeEvent** (`SPEC/type/changeEvent.json`): `eventType` from `SPEC/type/changeEventType.json`
(`entityCreated`, `entityUpdated`, `entityFieldsChanged`, `entitySoftDeleted`, `entityDeleted`,
`entityRestored`, thread/post/task, suggestion, `userLogin`, …), entity type/id/FQN, change
description, user, timestamp. Not CloudEvents-formatted.

**EventSubscription** (`SPEC/events/eventSubscription.json`), an entity managed over the API:

| Field | Values |
|---|---|
| `alertType` | Notification, Observability, ActivityFeed, GovernanceWorkflowChangeEvent, Custom (`:81`) |
| `subscriptionType` (destination) | Webhook, Slack, MsTeams, GChat, Email, ActivityFeed, GovernanceWorkflowChangeEvent (`:101-109`) — **no Kafka/Kinesis** |
| `subscriptionCategory` | Users, Teams, Admins, Assignees, Owners, Mentions, Followers, External |
| `filteringRules` | SpEL rule functions: event type, entity, owner, domain, tag, field change, pipeline state… (`SVC/events/subscription/AlertsRuleEvaluator.java`) |
| `retries` / `pollInterval` | default 3 / 60 s (`:333-342`) |
| `className` | Java consumer class; default `AlertPublisher` (`:248`) — see extension-points |

Delivery (`SVC/apps/bundles/changeEvent/AbstractEventConsumer.java:57-453`): events are polled from
the DB by offset (`eventSubscription.Offset`), delivered **at least once**, failures recorded as
failed events (dead-letter style), destination status tracked (`active`, `awaitingRetry`,
`retryLimitReached`, `failed`, `disabled`), metrics kept; `openmetadata-ops.sh syncAlertOffset`
re-syncs offsets. Schema-change alerts come from `ENTITY_UPDATED` + `matchAnyFieldChange`, with
downstream notification via lineage (`notifyDownstream`, D10.3 = 3).

**Publishing to Kafka (D11.3 = 2):** OM reads Kafka (topics, schema registry, OpenLineage over
Kafka) but no subscription destination or broker publisher exists in 1.13.3. Options are in
[extension-points.md](extension-points.md).
