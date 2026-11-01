# Extending OpenMetadata 1.13.3: Choosing the Mechanism

Every gap route in this skill sits somewhere on one ladder. Go up a rung only when the rung
below cannot do the job. Higher rungs cost more at every upgrade. Paths are relative to the
OpenMetadata repo at `1.13.3-release`. Items marked **[verified here]** were confirmed in
source while writing this skill and are not in the original research.

## The escalation ladder

| Rung | Mechanism | Typical gaps | Evidence | Upgrade risk | Licence of your code |
|---|---|---|---|---|---|
| 1. Configure | Custom properties, classifications/tags, glossaries + `conceptMappings`, policies (`matchAnyTag`), platform `entitySemantics` rules, governance workflows, event subscriptions | D1.2 (interim), D8.1, D8.2, D14.2, D14.8 tier 1, D14.11 | `customProperty.json`, `entityRulesSettings.json`, `workflowDefinition.json`, `eventSubscription.json:98-200` | Lowest; settings migrate with the server | n/a (data, not code) |
| 2. Automate outside | REST API / Python SDK client (`ingestion/.../ometa/ometa_api.py:265`) driven by an external scheduler (Airflow DAG) | 4.3 (Ranger bridge), D9.6 expiry, D14.1 VERBİS converter, D14.3 destruction job | D11.6 note: YAML workflows via `metadata` CLI/API | Low; depends on API stability | Your own code; the imported SDK itself is under the Collate Community License (plain REST avoids it) |
| 3. Plug in a class | Settings that name a class: `authorizerConfiguration.className`, `naturalLanguageSearch.providerClass`, custom connector `sourcePythonClass` | D2.3, D6.7 (full Ranger delegation) | `conf/openmetadata.yaml:330-332,494-499`, `customDatabaseConnection.json:24-27` | Medium; interface may change between versions | Your own jar or module |
| 4. App / connector | Native (Java, Internal) or external (Python, External) Apps; ingestion connectors; filling the `securityService` scaffold | 4.1 Iceberg, 4.3 (in-product), D8.3 | `app.json:77-90`, `AbstractNativeApplication.java:52-125`, D11.5 = 3 | Medium; follows framework changes | Apps/connectors under `ingestion/` → Collate Community License |
| 5. Change spec/service (fork) | Edit JSON Schema in `openmetadata-spec`, Java in `openmetadata-service`, UI, DB migrations | D1.2 durable, D14.5 `dueDate`, D14.10 doc link, D6.3, D14.8 tier 2 | `TypeResource.java:78-85` (no user-defined entity types) | Highest; you rebase every release unless upstreamed | Apache-2.0 trees (spec/service/ui) |

Rule of thumb: when a gap's route reaches rung 5, first ask whether rungs 1+2 together
(a field plus an outside job) give 80% of the value. They usually do.

## When a custom property is enough

Use a custom property when **all** of these hold:

- The fact belongs to one existing entity: a table, column-bearing asset, glossary term, data
  product, and so on. Check that the target schema has an `extension` field. **[verified
  here]** `table.json`, `glossaryTerm.json` and `dataProduct.json` have it.
  `team.json`, `user.json`, `policy.json`, service schemas and `docStore/document.json` do not.
- The value fits a supported type: string/markdown, number, date/time/interval, enum
  (multi-select optional, `enumConfig.json:9-19`), entity reference or list (configurable
  `entityTypes`, `complexTypes.json:6,51`), or table (`tableConfig.json`).
- "Required" can be expressed as a rule. Use a platform semantics rule (below) or a
  nonconformity listing rather than a schema flag.
- Nothing has to *happen* automatically when the value changes or expires. If something does,
  add rung 1 workflows or rung 2 jobs.

Move off custom properties when:

| Signal | Why | Go to |
|---|---|---|
| The thing has its own lifecycle, several parties, or many-to-many links (transfer record, request, breach, agreement) | One asset field cannot hold one-to-many | Borrow an existing type (glossary term, task/thread, data contract) or fork |
| You need per-record deadlines or escalation | `thread.json` and `userApprovalTask` have no `dueDate` or `timeout` (research, K; re-checked: no `dueDate` in `thread.json`) | Rung 5 (`dueDate` field), or rung 2 job computing deadlines from a date property |
| You need to attach a property to Team/User/Policy/Service | No `extension` on those schemas | Model the concept as a glossary term and reference it |
| The control must act on data (mask, filter, delete) | The catalog is not in the data path | Enforce in Ranger/Trino/storage; keep the catalog as the record |

**The borrowing patterns, ranked by the research:** glossary term (versioned, approval workflow,
SKOS export, has `extension`) > data contract (`dataContract.json:57-127`, though semantically a
contract is not a transfer) > per-asset fields (cannot express one asset going to several
parties).

## Platform semantics rules (rung 1) [verified here]

- **Where they live:** `entityRulesSettings.entitySemantics[]`, entries of type `semanticsRule`
  (`type/basic.json:242+`) with `name`, `description`, `rule` (JsonLogic), `enabled`,
  `entityType`, `ignoredEntities` and `provider`.
- **Defaults:** five ship in `json/data/settings/entityRulesSettings.json`, for example
  single-domain, single-glossary-term per table, and data product/domain match.
- **When they run:** on create, through `EntityRepository.copy()` (`:1357`) and
  `EntityMapper.java:39`; on patch, through `EntityRepository.java:3898`.
- **What the rule sees:** the full entity map (`RuleEngine.java:184-192`), including
  `extension` and `tags`.
- **Updates are lenient:** an update is rejected only if it does not reduce the number of
  failing rules (`RuleEngine.java:63-71`).
- **API:** `PUT /v1/system/settings` (`SystemResource.java:555`); read per type with
  `GET /v1/system/settings/entityRulesSettings/{entityType}` (`:397`).
- **Data contracts** can carry their own semantics rules, evaluated with
  `enforceContract` (`RuleEngine.java:59-60,118-125`).

Use them for mandatoriness and consistency checks. The research referenced `entitySemantics`
only as a way to *list* nonconforming entities (D14.2). Write-time enforcement is an additional
finding. Test against ingestion before enabling a rule broadly.

## Schedulers <a id="schedulers"></a>

**Research statement:** "no scheduled rule engine; schedulers only run ingestion jobs" (applied
to the D14 deadline gaps).

**[verified here] This is too strong for 1.13.3.** Governance workflows have a
`periodicBatchEntityTrigger` (`governance/workflows/elements/triggers/periodicBatchEntityTrigger.json:12-75`):
- `schedule` reuses the App `appSchedule`.
- It also takes `entityTypes`, search `filters` and `batchSize`.
- `PeriodicBatchEntityTrigger.java:74-124` turns the schedule into a Flowable cron
  `TimerEventDefinition`.
- Its nodes include `checkEntityAttributesTask`, `setEntityAttributeTask`, `userApprovalTask`,
  `runAppTask`, `createAndRunIngestionPipelineTask`, and `sinkTask` (`git` / `webhook` /
  `httpEndpoint`, `sinkTask.json:16`).

Apps also have their own scheduler (`apps/scheduler/AppScheduler`).

**What is still true:**
- Tasks and approvals have no per-record due date or timeout.
- There is no escalation node (D7.2 note).
- Nothing in the product acts on external systems beyond the sink and webhook nodes.

So a scheduled "sweep entities where property X < today and open a task or post to a webhook"
is in reach at rung 1. A true countdown per request still needs a date field (rung 1 custom
property, or rung 5 `dueDate`). Revocation or deletion in external systems still needs a
rung 2 job.

## Notifications and events (rung 1)

Event subscriptions route change events to webhook, Slack, Teams, GChat or e-mail, with SpEL
filter functions, retries, a dead-letter record and delivery status (D11.4 = 4, K:
`eventSubscription.json:98-200,333-338`, `AbstractEventConsumer.java:144-200`). This is the
trigger for tag-change → policy-sync bridges such as 4.3.

**Gap noted by the research:** change events are not published to Kafka (D11.3): there is no Kafka
destination type. Two no-fork routes exist: a webhook → bridge service → Kafka (rung 1), or a
custom `AbstractEventConsumer` named in the subscription's `className` that produces to Kafka
in-process (rung 3; `EventSubscriptionScheduler.java:184-191`, verified from the loading code
only). Details in `openmetadata-platform-integration`.

## Pluggable classes (rung 3)

| Point | Config | Contract |
|---|---|---|
| Authorizer | `authorizerConfiguration.className` (`conf/openmetadata.yaml:330-332`, default `DefaultAuthorizer`) | Implement `Authorizer` (`security/Authorizer.java:56`). Use it to delegate decisions to Ranger/OPA (D6.7 1→3). No ready PDP integration ships |
| NL query | `naturalLanguageSearch.providerClass` (`:499`) | Implement `NLQService`, constructor `(ElasticSearchConfiguration)`. The factory falls back to `NoOpNLQService` on any error (`NLQServiceFactory.java:15-41`) |
| Custom DB connector | `sourcePythonClass` (`customDatabaseConnection.json:24-27`) | A Python source class instantiated by the ingestion workflow |

## Apps and connectors (rung 4)

- **Apps:** type `Internal` (Java) or `External` (Python) (`app.json:77-90`). They are
  installed, uninstalled and triggered at runtime over REST and registered via a marketplace
  definition (`appMarketPlaceDefinition.json:9-180`; D11.5 = 3). About ten apps ship
  (`apps/bundles/`: autoPilot, dataRetention, insights, rdf, searchIndex, …).
  - **Limits:** no UI-page extension points, a single-valued permission model ("All"), and
    versions not managed separately from core (D11.5 note).
- **Connectors:** they live under `ingestion/src/metadata/ingestion/source/<category>/` and
  implement the framework base classes (D11.5).
  - For Ranger, the `securityService` entity, `rangerConnection.json`,
    `securityServiceMetadataPipeline.json`, a test-connection definition and the
    `SecurityServiceSource` base all exist.
  - There is no plugin registration in `ingestion/setup.py`, and the topology has
    `children=[]` (`security_service.py:68`).

## Schema changes and forks (rung 5)

- Entity schemas are JSON Schema in `openmetadata-spec/src/main/resources/json/schema/`, each
  with a `javaType`. Server and UI types are generated from them, so a field change ripples
  into service and UI.
- DB changes ship as per-version migrations under `bootstrap/sql/migrations/`, applied by
  `openmetadata-ops.sh migrate` (D12.7 note). There is **no rollback path** (D12.7 = 2).
- New *entity types* cannot be created by users (`TypeResource.java:78-85`). Adding one means a
  fork touching spec, repository, resource, search index, UI and migrations, and it is the most
  expensive route. Prefer borrowing an existing type.
- Smallest high-value forks named by the research: `required` on `customPropertyConfig`
  (D1.2), `dueDate` on `thread.json` (D14.5/D14.6/D14.7/D14.3 deadlines), configurable
  `PIIMasker` (D6.3), and policy precedence in `PolicyEvaluator` (D14.8 tier 2, where today
  all Deny rules run first and then Allow, per `PolicyEvaluator.java:47-76`).

## Licensing

- `openmetadata-spec`, `openmetadata-service` and `openmetadata-ui` are **Apache-2.0**
  (`LICENSE:1-3`).
- `ingestion/` (connectors, Python SDK, external apps, PII scanners) is under the **Collate
  Community License v1.0** (`ingestion/LICENSE:1-5`). Internal use is free. The restriction is
  on offering a competing hosted service and on derivative distribution.
- If you intend to publish your work, write it in the Apache-2.0 trees or outside the repo
  (rung 2). A Ranger bridge as an external Airflow job avoids the ingestion licence entirely.
- The project is controlled by a single vendor (D12.6). Upstream contributions are the main way
  to avoid carrying a fork.

## Upgrade-safety checklist

1. Prefer rungs 1–2; record all configuration (custom property definitions, rules, workflows,
   subscriptions) as code and re-apply it idempotently through the API. Drift detection is not
   built in (D11.6 = 2).
2. Pin the OM version the extension was tested on, and re-run its verification after every
   upgrade.
3. For rung 3–4 code, depend only on the documented interface (`NLQService`, `Authorizer`,
   connector base classes). Keep a contract test.
4. For rung 5, upstream the change or isolate it in as few files as possible. Write migrations
   that are idempotent and tolerate re-runs, because there is no rollback.
5. Before building, re-check the roadmap and next release (2.0.0 policy agent, data access
   requests). Several gaps here are moving upstream.
