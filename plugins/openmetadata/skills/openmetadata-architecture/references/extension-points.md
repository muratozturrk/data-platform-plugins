# Extension Points, Empty Interfaces, Secrets, Licence (OpenMetadata 1.13.3)

Paths relative to the repo at `1.13.3-release`. `SVC` = `openmetadata-service/src/main/java/org/openmetadata/service/`,
`SPEC` = `openmetadata-spec/src/main/resources/json/schema/`. K = source code, D = docs, İ = issue/PR,
K+ = verified while writing this skill. Loading mechanisms marked K+ were read in source; none
of the custom implementations sketched here was built or run in the research.

## Decide Where New Code Goes

```
Can it be expressed as data (custom property, tag, workflow, subscription, CSV)?  -> no code
Does it run on a schedule / on demand, talk to the API, show up in the UI apps page?
      -> App: internal (Java jar) or external (Python, run by the pipeline client)
Does it read a source system into the catalog?            -> ingestion connector (Python)
Does it replace a server behaviour the config names by class? -> plug-in jar on EXT_CLASSPATH
Does it touch a closed enum / switch (new destination, new secrets provider, new entity type)?
      -> fork, or wrap from outside (webhook bridge, sidecar)
```

Prefer the higher rung: every fork turns each upgrade into a merge, and migrations have no
rollback path (D12.7 = 2).

## Plug-in Mechanism: Your Jar + a Class Name

`bin/openmetadata-server-start.sh:42-49` builds the server classpath from `libs/*.jar` and then
appends `$EXT_CLASSPATH` (K+). The server instantiates the following classes by reflection, so a
jar on `EXT_CLASSPATH` plus a config value replaces them without forking:

| Hook | Config / field | Contract | Loaded at (K+) |
|---|---|---|---|
| Natural-language query | `elasticsearch.naturalLanguageSearch.providerClass` (+ `enabled: true`) | `NLQService`: `transformNaturalLanguageQuery(SearchRequest, String) → ES/OS query JSON`, `cacheQuery`; ctor `(ElasticSearchConfiguration)` | `SVC/search/nlq/NLQServiceFactory.java:15-43`; `conf/openmetadata.yaml:494-499` |
| Subscription consumer | `className` on an EventSubscription (settable in the create request, `SPEC/events/api/createEventSubscription.json:14`) | subclass `AbstractEventConsumer`; ctor `(DIContainer)`; inherits offset polling, retries, failed-event store | `SVC/events/scheduled/EventSubscriptionScheduler.java:184-191` |
| Request event handler | `eventHandlerConfiguration.eventHandlerClassNames` | `EventHandler`: `init(config)`, `process(req, resp)`, `close()`; runs async after each successful write request | `SVC/events/EventFilter.java:61-81`; `conf/openmetadata.yaml:538-541` |
| Authorizer / request filter | `authorizerConfiguration.className`, `.containerRequestFilter` | `Authorizer`, JAX-RS filter | `SVC/OpenMetadataApplication.java:888-932` |
| Pipeline service client | `pipelineServiceClientConfiguration.className` | `PipelineServiceClient` (Airflow, K8s, Noop shipped) | `SVC/clients/pipeline/PipelineServiceClientFactory.java:91` |
| Limits | `limits.className` (default `DefaultLimits`) | `Limits` | `SVC/OpenMetadataApplication.java:966-969` |
| Internal app | `className` in an app marketplace definition | subclass `AbstractNativeApplication` | `SVC/apps/ApplicationHandler.java:335` |

Gotchas: `EventFilter` swallows handler init exceptions at INFO level (`EventFilter.java:74-76`)
— a broken handler fails silently; handlers are fire-and-forget with no offset or retry, so use a
subscription consumer when delivery matters. A misconfigured `NLQService` silently falls back to
`NoOpNLQService`. `bootstrap/openmetadata-ops.sh:40-50` puts `collate-*` jars first on the classpath
"for cases where we override classes" — the commercial edition itself extends OSS by class
shadowing; do not rely on that for your own code.

## Apps Framework (D11.5 = 3)

- An app = marketplace definition (`SPEC/entity/applications/marketplace/appMarketPlaceDefinition.json:9-190`)
  + installed `App` entity (`SPEC/entity/applications/app.json:77-127`). Register with
  `POST /v1/apps/marketplace` (`SVC/resources/apps/AppMarketPlaceResource.java:54,281`), install,
  trigger and uninstall over REST (`SVC/resources/apps/AppResource.java:108,876,1054,1284`) or
  `openmetadata-ops.sh install-app|delete-app`.
- `appType`: **internal** = Java `className` running in the server (Quartz-scheduled, clustered);
  **external** = Python `sourcePythonClass` run as an ingestion pipeline by the pipeline client.
  Example: `openmetadata-service/src/main/resources/json/data/appMarketPlaceDefinition/HelloPipelines.json`.
- `scheduleType`: Live, Scheduled, ScheduledOrManual, NoSchedule, OnlyManual.
- Config: `appConfig` is a `oneOf` of the bundled configs **plus a free-form object**
  (`additionalProperties: true`), so a custom app needs no spec change for its settings
  (`SPEC/entity/applications/configuration/applicationConfig.json:8-46`, K+).
- Bundled (10 definitions): AutoPilot, CacheWarmup, DataContractValidation, DataInsights,
  DataInsightsReport, DataRetention, HelloPipelines (example), Mcp, RdfIndex, SearchIndexing.
- Limits: no UI extension points (no custom pages/panels), `permission` enum has a single value
  `All` (`app.json:123-125`), app versions are not managed independently from the core.

## Ingestion Extensions (Python)

Source/Processor/Stage/Sink/BulkSink steps (`ingestion/src/metadata/ingestion/api/steps.py:34-97`),
connectors discovered by convention through `service_spec.py`
(`ingestion/src/metadata/utils/service_spec/service_spec.py:37-66`), `Custom<Type>Connection`
entry points for out-of-tree connectors, and `metadata scaffold-connector` to generate a skeleton
(`ingestion/src/metadata/cli/scaffold.py:11-60`) (D11.2 = 3). No community connector registry.
Connector-level detail: `openmetadata-platform-integration`.

## Empty Extension Points — and Why They Are Empty

Defined but unfilled in OSS 1.13.3. The research's cross-cutting finding: they are mostly **not
forgotten but reserved for the commercial edition (Collate)**.

| Extension point | State in 1.13.3 | Commercial counterpart |
|---|---|---|
| `NoOpNLQService` | Default and only NLQ provider; returns `null` (`SVC/search/nlq/NoOpNLQService.java:22-30`); still the default in 2.0 (D) | AI Analytics / AskCollate |
| Ranger / `securityService` | Enum, `rangerConnection.json`, test schemas and Python base `ingestion/src/metadata/ingestion/source/security/security_service.py` exist; no Ranger source implementation; `SPEC/metadataIngestion/securityServiceMetadataPipeline.json` has one field (`type`) | Collate docs: "Reverse Metadata (Collate Only)" (D) |
| `slaValidation.json` | `refreshFrequencyMet`, `latencyMet`, `availabilityMet`, `actualLatency` defined; no Java/Python reference to the type — result schema, no evaluator | — |
| `aiPlatformConfiguration.json` | gRPC client config for an external "AI Platform" (`enabled: false`); no `.proto`, no gRPC dependency, no block in `conf/openmetadata.yaml` | Collate AI platform |
| `agentType` enum in `app.json:98-103` | `CollateAI`, `CollateAITierAgent`, `CollateAIQualityAgent`, `Metadata`; only `Metadata` runs (AutoPilot); configs for the three others exist under `applications/configuration/external/` | Collate AI agents |
| AI entities (`llmModel`, `agentExecution`, `promptTemplate`, `aiApplication`) | Record layer, not an engine; 2.0 docs: "AgentExecution stores runtime observations; it isn't the agent definition" (D) | AI SDK under CCL + Collate AI Studio |

Related moves (İ): #28922 moved Data Access Request button logic from OSS to Collate; PR #29734
removed the AI Governance Studio UI (~15 000 lines) from OSS, leaving only schema types; Ontology
Studio epic #32041 tracks AskCollate context features in a separate AI-platform repo.

Reading: an empty slot is a ready-made interface for an in-house, Apache-2.0, no-external-API
implementation (e.g. `NLQService` backed by an open-weight LLM) — and also a sign you are building
in the same space as the vendor. Design candidates: `openmetadata-capability-design`.

## Secrets Managers (gap: Vault)

`secretsManagerConfiguration.secretsManager` (`conf/openmetadata.yaml:623-624`), enum in
`SPEC/security/secrets/secretsManagerProvider.json:8`: `db` (default), `managed-aws`, `aws`,
`managed-aws-ssm`, `aws-ssm`, `managed-azure-kv`, `azure-kv`, `in-memory`, `gcp`, `kubernetes`.

- **HashiCorp Vault is not a provider** — no match for `hashicorp` in Java, Python or JSON sources (K+).
- The factory is a `switch` over that closed enum (`SVC/secrets/SecretsManagerFactory.java:38-57`)
  and `ExternalSecretsManager.storeSecret/updateSecret` are package-private abstract methods
  (`SVC/secrets/ExternalSecretsManager.java:198-202`): adding Vault means a fork of the spec enum,
  the Java factory and the Python side (`ingestion/src/metadata/utils/secrets/`).
- `db`, `aws`, `aws-ssm`, `azure-kv` are handled by `DBSecretsManager` on the server (non-managed
  variants are read on the ingestion side only — comment at `SecretsManagerFactory.java:40-49`).
- No-fork options: keep `db` (connector secrets encrypted with `FERNET_KEY` in the OM database) and
  source `FERNET_KEY`, DB and JWT credentials from Vault via environment injection at deploy time;
  or use the `kubernetes` provider and sync Vault → Kubernetes Secrets with an external operator.
  Closing options are ranked in `openmetadata-gap-closing`.

## What Needs a Fork

| Change | Why | Evidence |
|---|---|---|
| New entity type | Spec + generated classes + repository/resource/index/UI | `TypeResource.java:78-85` (K) |
| New custom-property type | Field types come from `@om-field-type` schema definitions | `JsonUtils.java:520-590` (K+) |
| New subscription destination type (e.g. Kafka as a UI choice) | Closed enum + `AlertFactory` switch | `SPEC/events/eventSubscription.json:101-109`; `SVC/apps/bundles/changeEvent/AlertFactory.java:18-21` (K+) |
| New secrets provider | Closed enum + switch | above |
| New search engine / "no search" | `searchType` enum: elasticsearch, opensearch | `SPEC/configuration/elasticSearchConfiguration.json:96-99` (K) |
| Several identity providers at once | `provider` single-valued | `conf/openmetadata.yaml:348` (K) |

Kafka without a fork: (a) a custom `AbstractEventConsumer` set as the subscription `className`,
producing to Kafka with OM's offset/retry machinery (K+ mechanism); (b) a webhook subscription to a
small bridge service that produces to Kafka. Either way, the destination will not appear as a
typed choice in the UI.

## Licence Boundary (Strategic Risk, Not a Capability Gap)

| Tree | Licence | Measurement |
|---|---|---|
| `openmetadata-service`, `-spec`, `-ui`, `-sdk`, `common` | Apache-2.0 (`LICENSE:1-2`, `NOTICE:1-2`) | 0 of 1 626 `openmetadata-service/src/main/java` files carry a CCL header (K) |
| `ingestion/` | Collate Community License v1.0 (`ingestion/LICENSE:1-2`) | 1 053 / 1 404 `.py` in `ingestion/src` (~73 %); 1 574 / 2 181 across `ingestion/`; core connectors included, e.g. `.../source/database/mysql/metadata.py:1-2` (K) |
| `openmetadata-airflow-apis/` | Folder `LICENSE` is Apache-2.0, yet 53 of 68 source files carry a CCL header | K+ — inconsistent; treat as CCL until clarified |

- The CCL restricts offering a competing SaaS/PaaS/IaaS; internal on-prem use is allowed.
- It decides **where** contributions you intend to publish should live: server plug-in jars, apps
  and separate packages that only *depend on* the SDK stay under your own licence; code written
  into `ingestion/` is a derivative of a CCL tree.
- The Python SDK (`ometa`) itself sits in the CCL tree; the Java SDK is Apache-2.0.
- Governance: single vendor (Collate), no foundation; `SECURITY.md` support table is stale (D12.6 = 2).
  DataHub has the analogous risk via its OSS/Cloud feature split.
- Not legal advice; have counsel read the CCL text for your distribution model.
