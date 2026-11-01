# Data Products, Ownership and Approval

A contract needs an owner, a place in the domain model, and a review path. This file covers the
rubric capabilities behind these: D9 (data products, domain and mesh), D7.1 (ownership) and D7.2
(approval workflow). Scores and `path:line` evidence come from the research (K unless marked).
The modelling advice is general practice.

## Where a contract sits (general practice)

```text
Domain (sales)                       owner: domain data lead; sets minimum standards
 └─ Data product (order-analytics)   owner: product team; lifecycle; SLA tier; visibility
     ├─ input ports   ← consumes other products' output ports (their contracts bind this product)
     └─ output ports  → table / topic / API, ONE CONTRACT EACH
                          └─ consumers (teams, pinned major version)
```

- The **data product owner** is accountable for every output contract.
- **Input ports** are where *other* producers' contracts bind this product. Lineage between
  products is the dependency graph you use for change impact.
- **Domain-level rules** (for example "every output port has an active contract with an owner and
  a freshness SLA") are the federated minimum standard from data mesh. Each domain may add
  stricter local rules.

## Scores

| ID | Capability | OM 1.13.3 | DH v1.7.0 | Atlas 2.5.0 |
|---|---|---|---|---|
| D9.1 | Domain modelling and ownership | 3 | 3 | 1 |
| D9.2 | Data product as a first-class entity | 3 | 2 | 1 (İ) |
| D9.3 | Data contract definition and validation | 3 | 2 | 1 |
| D9.4 | Access request and approval flow | 1 | 1 | 0 |
| D9.5 | Data marketplace and consumer experience | 2 | 2 | 0 |
| D9.6 | External data sharing and sharing agreements | 1 (D) | 1 | 1 |
| D7.1 | Ownership and stewardship assignment | 3 | 3 | 1 |
| D7.2 | Approval and review workflow | 3 | 1 | 0 (İ) |
| **D9** | **mean** | **2.2** | **1.8** | **0.7** |

Rubric levels to aim for: D9.2 level 3 means a product carries an SLA commitment and a lifecycle
state, both shown on its page. Level 4 means SLA compliance is measured in real time and
breaches are pushed to downstream consumers. No product reaches level 4 on D9.2 or D9.3.

## D9.1 Domains (K)

- **OM 3.** `Domain` is a structured, hierarchical object (sub-domains) with owners and experts.
  The policy function `hasDomain()` enforces domain-scoped access that follows the hierarchy.
  There are no per-domain local governance rules and no domain health dashboard.
  `openmetadata-spec/.../entity/domains/domain.json:11-80`,
  `openmetadata-service/.../security/policyevaluator/RuleEvaluator.java:103-170`,
  `.../json/data/policy/DomainAccessPolicy.json`.
- **DH 3.** Structured domain with ownership and `parentDomain` hierarchy. Domain-scoped
  authorisation extends to sub-domains. No local rules or health dashboard.
  `metadata-models/.../domain/DomainProperties.pdl:50-62`.
- **Atlas 1.** No domain concept. `atlas.metadata.namespace` is only a cluster/environment suffix
  on `qualifiedName`. A domain can be written as a free-text label.
  `common/src/main/java/org/apache/atlas/utils/AtlasPathExtractorUtil.java:147-150`.

## D9.2 Data products (K)

**OpenMetadata 1.13.3**, `openmetadata-spec/src/main/resources/json/schema/entity/domains/dataProduct.json`:

| Element | Values (line) |
|---|---|
| `lifecycleStage` | `IDEATION, DESIGN, DEVELOPMENT, TESTING, PRODUCTION, DEPRECATED, RETIRED`; default `DEVELOPMENT` (:11-15). Property note: "Driven by the governance workflow — not directly user-editable" (:201-203) |
| `dataProductType` | 13 values aligned with the ODPS v4.1 type taxonomy (`RAW_DATA`, `DERIVED_DATA`, `DATASET`, `REPORTS`, … `BI_DIRECTIONAL`) (:17-35) |
| `visibility` | `PRIVATE, INVITATION, ORGANISATION, DATASPACE, PUBLIC`; default `PRIVATE` (:36-41). Described as "visibility level … in the marketplace". It is a label: it does not grant or block data access |
| `priority` | `CRITICAL, HIGH, MEDIUM, LOW` (:42-46) |
| Port definition | `dataProductPort`: `name`, `portType` `INPUT/OUTPUT`, `protocol` (REST, GRPC, GRAPHQL, JDBC, KAFKA, FILE, S3, AZURE_BLOB, GCS, WEBHOOK, CUSTOM), `format` (JSON, CSV, PARQUET, AVRO, ORC, XML, PROTOBUF, DELTA, ICEBERG, CUSTOM), `endpoint`, `dataAsset` (:47-98) |
| Port management | Input and output ports are relationships managed through the API: `/{name}/inputPorts/add`, `/outputPorts/add`, list endpoints (`DataProductResource.java:452-734,1144-1300`); view schema `api/domains/dataProductPortsView.json:36-45` |
| `sla` | `slaDefinition`: `tier` (GOLD/SILVER/BRONZE/CUSTOM), `availability` %, `responseTime` ms, `dataFreshness` minutes, `dataQuality` minimum score (:100-133, :217-220) |
| Dependencies | `consumesFrom`, `providesTo` (other data products) (:221-228) |

Research note: SLA compliance is not measured in real time or reported automatically, which is
why the score is 3 and not 4. A data product can also hold a contract that its assets inherit.
Schema, quality expectations and test suite stay asset-specific (`DataContractRepository.java:1602-1612`,
checked for this skill).

**DataHub v1.7.0 (2).** `dataProduct` is a first-class entity with name, description, owners and
domain (`DataProductProperties.pdl:11-45`, includes custom properties). Assets are linked through
`DataProductAssociation`, and the only port notion is a boolean `outputPort` (default `false`)
on that link (`DataProductAssociation.pdl:13`). It is managed from YAML through the CLI, and
`dataproduct diff` shows drift between file and server
(`metadata-ingestion/src/datahub/cli/specific/dataproduct_cli.py:151-186`, D11.6 = 3). There is
no SLA, no product lifecycle state and no product-to-product dependency graph.

**Apache Atlas 2.5.0 (1, İ).** No data product type in any `addons/models` typedef, and no JIRA
record either. Assets can only be grouped with labels or glossary terms.

## D9.4–D9.6 consumer side

| ID | OM | DH | Atlas |
|---|---|---|---|
| D9.4 Access request | 1: no access-request task type; a request is a generic task and approval grants nothing (`entity/feed/thread.json:9-21,49-101`). A Data Access Request feature that actually grants access with expiry is merged upstream for a later major release (İ; not in 1.13.3) | 1: lists external roles and links out to a `requestUrl`; off by default (`role/RoleProperties.pdl:36-39`). Tracked access workflows are Cloud-only | 0 |
| D9.5 Marketplace | 2: separate marketplace page with data product/domain widgets, votes and threads; no side-by-side comparison (`DataMarketplacePage.component.tsx`) | 2: per-product page with output ports and assets; no ratings or comparison (`DataProductSummaryTab.tsx:8-16`) | 0 |
| D9.6 External sharing | 1 (D) | 1 | 1 |

For contracts this means the **consumer list is not a structured catalog object** in any of the
three products. OpenMetadata's contract has `identities` ("Consumer AD Groups"), but it is a
free-text list (`dataContract.json:85-93`). Keep the consumer registry with pinned versions in
the Git contract.

## D7.1 Ownership (K)

- **OM 3.** Several user/team owners per asset, inherited from the parent asset, bulk assignment
  by CSV, and unowned assets reported in data insights. No typed business/technical owner split.
  `experts`/`reviewers` exist only on domains, data products and glossaries; the data contract
  also has `reviewers`.
  `table.json:1111-1115`, `EntityRepository.java:7105-7145`, `json/data/role/DataSteward.json`.
- **DH 3.** `owners` with typed ownership (custom `ownershipType` entities), batch assignment,
  and a `hasOwners` index to find unowned assets. `common/Ownership.pdl:9-33`.
- **Atlas 1.** `Asset.owner` is a single free-text string. `addons/models/0000-Area0/0010-base_model.json:37-47`.

Contract rule (general practice): a contract cannot become `active` without a named owner and a
support channel. Check it in CI. In OpenMetadata, a governance workflow rule can also check the
`owners` field on the contract entity.

## D7.2 Approval workflows (K)

- **OM 3.** Flowable-based engine. User-defined `workflowDefinition`s support multi-step
  approvals, JsonLogic conditional branches, approve/reject thresholds and stage history. Approval
  tasks have **no timeout or escalation node**.
  `openmetadata-service/pom.xml:918-928`,
  `openmetadata-spec/.../governance/workflows/workflowDefinition.json`,
  `.../json/data/governance/workflows/GlossaryApprovalWorkflow.json:58-206`.
  Checked for this skill: only two default workflow definitions ship
  (`GlossaryApprovalWorkflow.json`, `RecognizerFeedbackReviewWorkflow.json`). There is **no
  ready-made contract approval workflow**, although the contract carries an approval-style
  `entityStatus` (`Draft … In Review … Approved`, default `Draft`). You would have to build one.
- **DH 1.** No pre-publication gate in OSS. A change emits an `EntityChangeEvent` that can only be
  notified (Slack/Teams). Approval workflows are marked OSS ❌ in the official matrix.
  `metadata-jobs/mae-consumer/.../hook/event/PlatformEventGeneratorHook.java:59-90`.
- **Atlas 0 (İ).** No workflow engine or approval step. Changes publish directly. No JIRA feature
  record. Cited: `https://issues.apache.org/jira/browse/ATLAS-2607`.

## Putting it together (general practice)

| Concern | Source of truth | Catalog role |
|---|---|---|
| Contract text and version | Git | Registry, display, validation run (OM) |
| Breaking-change gate | CI | Change events to notify downstream owners |
| Approval of major changes | Git review (CODEOWNERS / required reviewers) | Optional mirrored workflow for audit (OM) |
| Ownership | Catalog owners (D7.1 = 3 in OM, DH) | Same |
| Product lifecycle / SLA tier | Catalog data product (OM) or contract `x-` fields | Display; monitoring is external |
| Consumer registry | Git contract | Lineage to find unregistered consumers |
| Access enforcement | Engine / storage policies | Reference only (see `access-control-policies`) |
