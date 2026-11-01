# Contract Specification

This file has three parts:

1. An illustrative contract to copy (**general practice**, no evidence tag).
2. How three measured catalogs model a contract (**research-measured**, tagged K).
3. A field map between the two.

## Rubric definition (D9.3)

The research rubric scores "data contract definition and validation" as follows (condensed):

| Level | Meaning |
|---|---|
| 0 | No contract concept; expectations live in documents and chat. |
| 1 | Contract can be attached as free text; not machine-readable. |
| 2 | Contract in a structured format (schema, field types, required rules); **no automatic validation**. |
| 3 | Real data/schema is **validated automatically** against the contract; violations raise alerts; contracts are **versioned**. |
| 4 | Contract changes are **analysed for backward compatibility** (breaking vs non-breaking). An incompatible change **cannot be published** without consumer approval or a transition period (contract gate, CI/CD integration). |

Measured: OpenMetadata 1.13.3 = **3**, DataHub v1.7.0 = **2**, Apache Atlas 2.5.0 = **1** (all K).
No product reaches 4. [change-management.md](change-management.md) gives the level-4 gate as a
CI design.

## Illustrative contract (ODCS-style, general practice)

The shape follows the Open Data Contract Standard (ODCS) v3 layout: `apiVersion`, `kind`,
`schema` with `properties`, `quality`, `slaProperties`, `team`, `support`. OpenMetadata
imports/exports this standard (`odcsDataContract.json:5`, enum `v3.1.0 … v2.2.0`, K). **The
example is illustrative.** Validate it against the official ODCS JSON Schema for your target
version before you rely on exact key names. Keys under `x-` are local extensions, not ODCS.

```yaml
apiVersion: v3.1.0
kind: DataContract
id: 7f3c9a52-orders-daily
name: orders_daily
version: 2.1.0                      # semver of the CONTRACT, see change-management.md
status: active                      # proposed | draft | active | deprecated | retired
domain: sales
dataProduct: order-analytics
description:
  purpose: Daily confirmed orders for finance reporting and demand forecasting.
  usage: Join on customer_id to customer_master; amounts are in EUR, VAT excluded.
  limitations: Cancelled orders are removed at T+1; do not use for real-time stock.
team:
  - username: team-orders-lead
    role: owner
  - username: team-orders-oncall
    role: support
support:
  - channel: "#orders-data"
    tool: chat
schema:
  - name: orders_daily
    physicalName: sales.orders_daily
    physicalType: table
    properties:
      - name: order_id
        logicalType: string
        physicalType: varchar(36)
        required: true
        unique: true
        primaryKey: true
      - name: customer_id
        logicalType: string
        physicalType: varchar(36)
        required: true
        classification: internal
      - name: amount
        logicalType: number
        physicalType: decimal(18,2)
        required: true
        description: Order total in EUR, VAT excluded.
        quality:
          - type: library
            rule: valueRange          # tool-specific rule name; map to your engine
            mustBeBetween: [0, 10000000]
            severity: warning
      - name: order_date
        logicalType: date
        physicalType: date
        required: true
      - name: loaded_at
        logicalType: timestamp
        physicalType: timestamp
        required: true
quality:
  - type: sql
    description: No duplicate orders in the latest load
    query: "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM sales.orders_daily WHERE load_date = CURRENT_DATE"
    mustBe: 0
    severity: error
slaProperties:
  - property: frequency
    value: 1
    unit: d
  - property: latency                # max delay from source event to availability
    value: 4
    unit: h
    element: orders_daily.loaded_at
  - property: retention
    value: 3
    unit: y
x-compatibility: BACKWARD           # change policy, see change-management.md
x-deprecation-window-days: 90
x-consumers:
  - team: finance-reporting
    pinnedMajor: 2
  - team: demand-forecast
    pinnedMajor: 2
x-access:
  policyRef: sales-orders-read      # enforced in the engine, only referenced here
```

### Authoring rules (general practice)

- **One contract per output port** (table, topic, API). A data product with three outputs has
  three contracts.
- **Logical and physical types both.** Consumers depend on the logical type. The physical type
  catches silent widening or narrowing.
- **Every `required: true` and `unique: true` must have a matching check.** Otherwise the
  contract makes a promise that nothing tests. Generate the checks from the contract in CI.
- **SLA values must be measurable** by a query or sensor (see `data-quality-slas`).
- **Access and terms are references.** The contract names the policy; the engine enforces it.
- **The contract version is not the data version.** Bump it on contract changes. A new day of
  data does not change it.

## How the measured catalogs model a contract (K)

### OpenMetadata 1.13.3 — `DataContract` entity (D9.3 = 3)

File: `openmetadata-spec/src/main/resources/json/schema/entity/data/dataContract.json`
(required: `id`, `name`, `entity` — `:422`).

| Part | Field (line) | Notes |
|---|---|---|
| Target asset | `entity` (:290-293) | One asset per contract: table, topic, API endpoint, dashboard data model are schema-validated |
| Status | `entityStatus` (:285-289) → `type/status.json:8-16` | `Draft, In Review, Approved, Archived, Deprecated, Rejected, Unprocessed`; default `Draft`. A separate `contractStatus` enum (`Draft/Active/Deprecated`, :11-27) is defined but not used by any property |
| Schema | `schema[]` (:298-306), items = table `column` | Validated against the live asset |
| Semantics | `semantics[]` (:307-314), `basic.json` `semanticsRule` | JsonLogic rules run by `RuleEngine` against the asset's **metadata** (e.g. "has owner"), not data rows (`DataContractRepository.java:1228-1237`) |
| Quality | `qualityExpectations[]` (:332-339) → test cases; `testSuite` (:294-297) | Validation deploys and triggers the DQ test suite (`DataContractRepository.java:1059`) |
| ODCS round-trip | `odcsQualityRules[]` (:340-347) | Kept so ODCS import → export does not lose rules |
| SLA | `sla` (:326-331) → `contractSLA` (:128-221): `refreshFrequency` (interval + hour/day/week/month/year), `maxLatency` (value + minute/hour/day), `availabilityTime`, `timezone`, `retention`, `columnName` | **Stored, not evaluated** (no service code reads it; `slaValidation.json` result schema is never filled) |
| Security | `security` (:320-325) → `contractSecurity` (:105-127): `dataClassification`, `policies[]` with `accessPolicy`, `identities` ("Consumer AD Groups", free text), `rowFilters` | Documentation only; not enforced on data reads |
| Terms | `termsOfUse` (:315-319, def :222-234) | Markdown content for humans and AI agents |
| People | `owners` (:356), `reviewers` (:361) | |
| Validity | `effectiveFrom` (:366), `effectiveUntil` (:371) | |
| History | `contractUpdates[]` (:348-355), `changeDescription`, `version` | Same entity versioning as other assets (D1.4 = 3) |
| Result | `latestResult` (:393-411) → `contractExecutionStatus.json`: `Running, Success, Failed, PartialSuccess, Aborted, Queued` | |
| Inheritance | `inherited` (:416) | An asset in a data product inherits the product's contract. Schema, quality expectations and test suite are **not** inherited (`DataContractRepository.java:1602-1612`) |

Validation paths: scheduled `DataContractValidationApp`
(`.../apps/bundles/dataContracts/DataContractValidationApp.java:36-130`) and REST endpoints
`/{id}/validate`, `/entity/validate`, `/validate`, `/validate/yaml`
(`DataContractResource.java:940,985,1357,1384`). What `validateContract` checks
(`DataContractRepository.java:1031-1075`): schema fields against the entity, semantic rules, and
quality tests. **Schema failures = missing fields + duplicate fields only.** Type mismatches are
listed in `typeMismatchFields` but "don't count as failures" (`:456-458`). At creation they are
"non-blocking" because "ODCS contracts define data expectations, not necessarily matching
physical schema exactly" (`:186-189`).

### DataHub v1.7.0 — `dataContract` entity (D9.3 = 2)

File: `metadata-models/src/main/pegasus/com/linkedin/datacontract/DataContractProperties.pdl:11-59`.

| Part | Field | Notes |
|---|---|---|
| Target | `entity: Urn` (relationship `ContractFor` → `dataset`) | Datasets only |
| Schema | `schema: array[SchemaContract]` → `assertion` | A contract section is a link to an assertion |
| Freshness | `freshness: array[FreshnessContract]` → `assertion` | |
| Quality | `dataQuality: array[DataQualityContract]` → `assertion` | |
| Raw | `rawContract: optional string` | Original YAML kept as text |
| Status | `DataContractStatus.pdl`: `ACTIVE` (:21), `PENDING` (:25) | |

DataHub OSS does **not** validate the contract. Its state in the UI is derived from assertion
results reported from outside. There is no contract versioning or CI gate. The `datacontract` CLI
is deprecated: "This CLI is DEPRECATED and no longer supported"
(`metadata-ingestion/src/datahub/cli/specific/datacontract_cli.py:21-28`). An open RFC
(DataHub GitHub #17242, "Bilateral Data Contracts") (lifecycle PENDING → ACTIVE → EXPIRED → ARCHIVED, multi-party
sign-off, version history, ODCS templates) is **İ** evidence of direction, not a feature.

### Apache Atlas 2.5.0 (D9.3 = 1)

No producer–consumer contract concept. The closest object is `avro_schema` pulled from a schema
registry (`addons/models/1000-Hadoop/1065-avro_model.json:29-46`: `avro_notation`, `versionId`,
`isLatest`). It applies no compatibility rule. A contract can only be written into a description
field.

## Field map: template → catalogs

| Template key | OpenMetadata `DataContract` | DataHub `dataContract` | ODCS v3 |
|---|---|---|---|
| `version` | entity `version` (auto, see change-management) | none (no versioning) | `version` |
| `status` | `entityStatus` | `status` ACTIVE/PENDING | `status` |
| `schema[].properties[]` | `schema[]` (columns) | `schema` → schema assertion | `schema[].properties[]` |
| per-column `quality` | `qualityExpectations` (test cases) | `dataQuality` → assertions | `quality[]` |
| `slaProperties` frequency/latency | `sla.refreshFrequency`, `sla.maxLatency` (not evaluated) | `freshness` → freshness assertion | `slaProperties[]` |
| `slaProperties` retention | `sla.retention` | — | `slaProperties[]` |
| `team` owner/support | `owners`, `reviewers` | ownership aspect on the dataset | `team[]`, `support[]` |
| `description.usage/limitations` | `termsOfUse` (markdown) | `rawContract` text | `description` |
| `x-access.policyRef` | `security.policies[].accessPolicy` | — | `roles[]` |
| `x-consumers` | `security.policies[].identities` (free text) | — | — |
| effective dates | `effectiveFrom/Until` | — | `contractCreatedTs` |
| `x-compatibility`, deprecation window | — (no field) | — | — (use custom properties) |

**Gaps common to all three:** no field for a compatibility mode, no deprecation window, and no
registry of consumers with pinned versions. Keep these in the Git copy of the contract and have
the CI gate read them.
