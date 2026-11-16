# Azure-style RBAC and ABAC on Ranger

Status: starter rules and all sections filled from the Azure RBAC and ABAC documentation summaries (merged 2026-11-14); conflicts #20 and #21 decided 2026-11-15. Later sources add to these sections; NIST SP 800-162 merged into Conditions and Role assignments (2026-11-15); Authorization in Action merged into building blocks, role definitions, assignments, scope, deny, conditions, best practices and a new section on policy layers (2026-11-15); Apache Ranger wiki merged into deny, scope (zones), conditions table, privileged access and differences (2026-11-15); Attribute-Based Access Control merged into scope, deny, conditions and delegation (2026-11-15); Hadoop Security merged into scope, deny, delegation and privileged access (2026-11-15); Kafka: The Definitive Guide merged into role definitions, deny and scope (2026-11-15); Trino: The Definitive Guide merged into scope and best practices (2026-11-16); Data Governance: The Definitive Guide merged into building blocks, role definitions, scope, assignments, policy layers and best practices (2026-11-16); Data Management at Scale, 2nd ed. merged into scope, role assignments and best practices (2026-11-16); KVKK Personal Data Security Guide markers in building blocks, best practices and privileged access (2026-11-16); Data Mesh merged into scope, role definitions and policy layers (2026-11-16).

The platform's authorization model follows Azure RBAC and Azure ABAC concepts; Ranger is the runtime that enforces it. This file is the bridge: it says how each Azure concept is expressed in platform manifests and compiled to Ranger.

Source marker `Azure docs NN` = summary NN of the Azure RBAC and Azure ABAC documentation (Microsoft Learn); `conflicts #N` = a decided row in `synthesis/conflicts.md`.

## Starter rules

### The four building blocks
Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 01–05, 08 (merged 2026-11-14); Hadoop Security, ch. 6, 7 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 7 (merged 2026-11-16); KVKK Personal Data Security Guide, §2.4, §3.1 (merged 2026-11-16).

1. Model every grant as a **role assignment = security principal + role definition + scope** (+ optional condition); grant only by creating such a record and revoke only by removing it; read each assignment as PARC: the principal, the actions of its role definition, the resources of its scope, and its condition. Why: Azure has no other grant or revoke path, a record with all three parts is reviewable and compiles to exactly one Ranger policy item, and reading it as PARC makes manifests and requests comparable. (plugin v1; Azure docs 01, 04; Authorization in Action ch. 4.3.1, 7.2.)
2. Keep **role definitions**, scopes and principals as separate fields and never encode one inside another (no scope hidden in a role name); a role definition is a named set of allowed actions per engine with `actions` (management) and `dataActions` (data), plus `notActions`/`notDataActions` as subtractions. Why: the same role must be reusable at many scopes for many principals, and subtraction narrows a wildcard, it is not a deny. (plugin v1; Azure docs 01, 02.)
3. Define **scope** as a hierarchy and assign the smallest role to the smallest group at the narrowest level that covers the task, even when a broader grant would be easier; where per-dataset grants become unmanageable, assign at the domain that forms the team's trust boundary (published layers only, Scope design rule 7) so that new datasets inherit the grant; access granted at a parent scope is inherited by children, so review a grant on a container (catalog, schema pattern, topic prefix) as a grant on every object inside it; CI rejects allow items for `public` or any all-users group on a zone-wide wildcard. Why: a narrow scope limits what a compromised principal exposes while keeping assignments few, grants repeated for every new dataset either lag behind or creep, a catalog-level read reads every table in the catalog, and in the book's case one broad allow exposed financial forecasts to interns. (plugin v1; Azure docs 01, 03, 04, 08; Authorization in Action ch. 10.1.2; Hadoop Security ch. 6, 7; Data Governance DG ch. 7; KVKK guide §2.4, §3.1; conflicts #16, #48.)
4. Treat the model as **additive**: effective access is the union of all assignments that reach a principal, directly or through groups; a narrower assignment with a weaker role does not restrict a broader one, so reduce access by removing or narrowing the broader assignment; use deny only as a platform guardrail. Why: Azure sums assignments and evaluates its system-managed deny assignments first; mirroring that keeps the model predictable. (plugin v1; Azure docs 01, 05.)

### Scope mapping
| Azure scope | Platform scope | Ranger expression |
| --- | --- | --- |
| Management group | platform (all institutions) | platform tag service linked to every zone (guardrails); shared platform policies as tag policies or generated items in every zone; unzoned policies only for platform resources |
| Subscription | institution | security zone `t-{institution}` |
| Resource group | domain or workspace | schemas `<layer>_<domain>` of the published layers (`t_acme_lake.gold_sales`); raw and staging only at layer or dataset scope, topic prefix (`t.acme.sales.*`) |
| Resource | dataset | table, topic |
| Sub-resource (e.g. blob path) | column or row slice | column resource; row-filter policy |

How inheritance is generated: see "Scope design and inheritance", rule 2.

### Built-in role definitions (platform)
| Platform role | Azure analogue | Control-plane actions | Data actions |
| --- | --- | --- | --- |
| Institution Owner | Owner | approve and merge changes to the institution folder (CODEOWNERS); manage datasets inside the zone, including Kafka topic `alter`, `alter_configs`, `configure` and `delete` (eligible, conflicts #49); no `delegateAdmin` | none (data only through a separate, eligible data-role assignment) |
| Data Contributor | Storage Blob Data Contributor / Contributor | create and alter tables; on Kafka only `create` topics (conflicts #49) | select, insert, update, delete; publish, consume |
| Data Reader | Storage Blob Data Reader / Reader | none | select; consume; describe |
| Access Administrator | User Access Administrator / RBAC Administrator | create and remove assignments, constrained by condition (which roles, which principal types) | none |
| PII Reader | (custom data role) | none | unmasked read on resources tagged `PII` |
| Restricted Reader | (custom data role; reason: conflicts #62) | none | unmasked read of columns tagged `RESTRICTED`; never a table tagged `RESTRICTED` at table level |
| Auditor | Reader on logs | read audit and policy metadata | none |

Custom roles are allowed only with a documented need, when no built-in role fits; see "Role definitions" for their required properties. Why: built-in roles keep every zone on the same definitions. (plugin v1; Azure docs 07, 17.)

### Conditions (ABAC)
| Azure condition element | Platform source | Ranger mechanism |
| --- | --- | --- |
| `@Principal` attribute (custom security attribute) | Keycloak user attribute (institution, region, clearance) | user attribute in condition or row filter; else materialised per-institution items |
| `@Resource` attribute (e.g. blob index tag, path) | OpenMetadata classification, naming prefix | Ranger tag (tag-based policy), resource pattern |
| `@Request` attribute | engine request context (operation, columns) | access type, column resource, mask policy; a condition evaluator on the policy item, fed by context enrichers registered in the service definition (Apache Ranger wiki p. 6); whether a shipped evaluator tests request sub-operations is open |
| `@Environment` attribute (time, network) | request time, client IP | `validitySchedules`, IP-range condition |

How conditions narrow assignments: see "Conditions: format, attributes and patterns", rule 1.

### Evaluation
Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 01, 05 (merged 2026-11-14).

Azure: deny assignments → role assignments (actions minus not-actions) → conditions. Ranger: tag policies → resource policies, each deny (minus exceptions) → allow (minus exceptions); no match is denied. Write policies so both orders give the same answer: guardrail denies live in tag policies; grants live in resource policies; conditions only narrow. Explain any decision by walking the same steps in order: guardrail deny, matching allow item (access type after subtraction), condition. Why: this is Azure's documented decision sequence and the basis for troubleshooting. (plugin v1; Azure docs 01, 05.)

### Privileged and time-bound access
Eligible, approved, time-limited elevation (Azure PIM) = access-request workflow that creates an assignment with a validity period (`validitySchedules`) and a request label; expiry needs no action. Periodic access reviews = `/ranger-audit-review` with removal of unused assignments. Details: "Privileged access (eligible, time-bound) and access reviews". (plugin v1.)

### What Azure RBAC is not
Azure Policy governs resource configuration, not access decisions; the platform equivalent is CI validation and the OPA retention engine. Do not put access rules there. (plugin v1.)

## Manifest shape (platform)
```yaml
kind: RoleDefinition
metadata: { id: data-reader, displayName: Data Reader }     # id is immutable
spec:
  actions: []
  dataActions: ["trino:select", "kafka:consume", "kafka:describe"]
  notDataActions: []
  assignableScopes: ["/institutions/*"]
---
kind: RoleAssignment
metadata: { id: ra-0001 }
spec:
  principal: { type: group, name: platform_t-acme_analyst }
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/sales          # compiles to zone t-acme, published layers only: t_acme_lake.gold_sales.*
  condition: "@Resource[tag:classification] != 'RESTRICTED'"
  validity: { end: "2027-01-31" }
```

## Platform naming
Source: platform decisions after a trial (trial: ranger-policy-compile, 2026-11-16). Every reference example, the compiler and the `examples/` files use these names.

| Object | Pattern | Example |
| --- | --- | --- |
| Ranger resource service | `<engine>-<purpose>` | `trino-interactive`, `trino-etl`, `kafka-main`, `spark-sql` |
| Tag service | `platform-tags` (`platform-tags-dev` outside production) | `platform-tags` |
| Zone | institution ID | `t-acme` |
| Trino and Iceberg catalog | `<institution with _>_lake` | `t_acme_lake` |
| Spark database | `<catalog>_<schema>` (provisional, conflicts #66) | `t_acme_lake_gold_sales` |
| Kafka topic | `<prefix>.<domain>.<pub or int>.<name>.v<n>` | `t.acme.sales.pub.order-created.v1` |
| Human group | `platform_<institution>_<role>`, or `platform_<institution>_<team>-<role>` for a stable team; never context such as region or project (attribute-and-tag-design.md › When to decide by role, by scope, or by attribute, rule 1) | `platform_t-acme_pii-reader`, `platform_t-globex_credit-risk-analyst` |
| Workload identity | `wi-<institution>-<pipeline>` | `wi-t-acme-orders-ingest` |
| Break-glass group | `platform_breakglass` | |
| Assignment ID | `ra-<4 digits>`, next free number, unique in the whole repository (CI check, Role assignments, rule 4) | `ra-0042` |
| Resource policy | `<zone>-<assignment ID>-<service>`, suffix `-cg` for consumer groups; one row-filter policy per table, `<zone>-<table>-rowfilter`, built from every assignment and agreement on it | `t-acme-ra-0001-trino-interactive`, `t-globex-loans-rowfilter` |
| Tag policy | `<zone>-tag-mask` per zone; `guardrail-<name>` for platform guardrails | `t-acme-tag-mask`, `guardrail-restricted-read` |
| Policy labels | `source`, `institution`, `assignment`, `role`, `owner`, `review-by`, `manifest` (`<path>@<sha>`), `request` | `assignment:ra-0001` |

## Role definitions: actions, data actions and subtraction
Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 02, 06, 07, 08, 14 (merged 2026-11-14); Authorization in Action, ch. 9, 10, 13 (merged 2026-11-15); Hadoop Security, ch. 7 (merged 2026-11-15); Kafka: The Definitive Guide, 2nd ed., ch. 11 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 2, 3, 7 (merged 2026-11-16); Data Mesh, ch. 9, 12 (merged 2026-11-16)

1. Define every platform role with a management allow list, a management exclude list, a data allow list, a data exclude list, assignable scopes, an immutable ID and a description. Why: Azure role definitions carry these independent lists, and description and assignable scopes are mandatory for custom roles. (Azure docs 02, 07.)
2. Put engine data access types (Trino `select`, `insert`, `update`, `delete`; Kafka `publish`, `consume`, `describe`) only in the data lists, and DDL, policy, zone or service management rights (Kafka `create`, `delete`, `alter`, `configure`, `alter_configs`) only in the management lists. Why: Azure keeps the two planes in separate lists so a wildcard management role never reaches data, and Kafka exposes both kinds of operation through one authorizer. (Azure docs 02; Kafka DG ch. 11.)
3. Compute effective permissions per plane as allow list minus exclude list, and let the compiler emit the explicit remaining access types into the Ranger policy item; keep access types fine-grained there and bundle them only as named platform roles, never as one catch-all access type. Why: Azure defines effective rights this way, Ranger has no subtraction, and in the book's case a catch-all action let customers share data externally. (Azure docs 02; Authorization in Action ch. 10.1.4.)
4. Treat an exclude list as subtraction inside one role, never as a deny: do not compile it into Ranger deny items, and when the exclusion matters check that no other role held by the same principal grants the excluded type. Why: Azure states that `NotActions` and `NotDataActions` are not deny rules, so another role can still grant the excluded action. (Azure docs 02.)
5. Use an exclude list only next to a wildcard allow entry; otherwise list access types explicitly. Why: exclude lists only subtract from wildcards. (Azure docs 02.)
6. Reference roles by their immutable ID in manifests, CI and scripts; display names are for humans only. The role key (for example `data-reader`) is that ID and never changes; a rename changes only `displayName`. Why: IDs survive renames, including a preview role losing its suffix. (Azure docs 02, 07, 08; decision 2026-11-15.)
7. Flag as privileged any role that can manage everything, write or delete everything, or create or delete assignments, deny rules or role definitions; on the platform this includes every role carrying policy-admin, zone-admin or `delegateAdmin` rights. Why: Azure lists such roles as privileged administrator roles that need extra control. (Azure docs 02, 06.)
8. Cut the management built-in roles as Azure does: the read-only role (Auditor) sees metadata and no data, a role that manages resources has no right to change access, full management plus assignment rights sit in one rarely assigned owner role, and assignment rights alone sit in the narrower Access Administrator. Why: Azure Reader, Contributor, Owner and RBAC Administrator are cut this way to keep access changes rare and narrow. (Azure docs 06, 09.)
9. Grade data roles as a ladder: Data Reader reads, Data Contributor reads, writes and deletes; each data role carries only the metadata rights needed to reach the data; consumers of an output port (the published layers, conflicts #48; `.pub.` topics, conflicts #58) hold only Data Reader (Trino `select`; Kafka `consume`, `describe`), and no consumer receives update or delete, not even to correct data. Why: the Azure Storage Blob Data roles follow this ladder and the reader adds only container read, and output ports are read-only so that only the product's own transformation changes its data, which immutability and consistency between copies depend on. (Azure docs 06; Data Mesh ch. 12; conflicts #48, #58.)
10. Create a custom role only when no built-in role fits, start from the closest built-in role and record which one, leave out what the job must not do (for example a data supplier role that may create tables but not alter or delete them), and derive its access types by listing the engines the job needs and then the access types each engine's Ranger service definition exposes. Why: Azure recommends starting from a built-in role and mapping services before actions, access types differ per service definition, and predefined roles are often broader than the job. (Azure docs 07; Data Governance DG ch. 7.)
11. Give every custom role a platform-unique display name, a description, at least one access list and assignable scopes at institution level or below; never the platform root and never a wildcard in assignable scopes; CI checks that every institution named in assignable scopes exists. Why: Azure makes these properties mandatory and forbids root and wildcard scopes so nobody gains scope by editing the definition, and a scope naming a missing institution is a silent error. (Azure docs 07; decision 2026-11-15.)
12. List access types explicitly in custom roles; the compiler rejects wildcards there. Why: a wildcard also grants access types added later. (Azure docs 07, 08.)
13. Make roles assignable broadly (institution or domain) and assign them narrowly (dataset); never create a role per dataset. Why: per-resource roles exhaust the role budget, and access is set by the assignment, not by assignable scopes. (Azure docs 02.)
14. Never assign a custom role with data access types at platform scope. Why: Azure does not allow custom roles with data actions at management-group scope. (Azure docs 07.)
15. Test every custom role in CI against expected allow and deny cases for each target service definition, before rollout and after every change. Why: Azure lists testing as a required step, and a role valid for one service definition may not be valid for another. (Azure docs 07.)
16. Keep custom role definitions in the platform folder of the policy repository so that only its code owners can create, change or delete them. Why: Azure lets only principals with role-definition write rights on all assignable scopes manage a role, institution folders reference only platform role definitions, and role creation belongs at platform scope while granting happens lower down. (Azure docs 07; Hadoop Security ch. 7; conflicts #7.)
17. Delete a role only in the same change that removes every assignment using it, eligible ones included; CI blocks a deletion while references remain. Why: Azure refuses to delete a role that is still referenced, including by eligible assignments. (Azure docs 07.)
18. Report custom roles with no assignments and delete them; merge stacked roles into a new custom role only for a stable job function. Why: unused roles consume the role budget, and every merged role is one more definition to maintain. (Azure docs 14.)
19. Never let a management role imply data access: Institution Owner and every other control-plane role carry no data access types, and data is granted only through an explicit data-role assignment, eligible for owners. Why: an Azure Owner with `*` still cannot read blob data without a data role. (Azure docs 02, 06; Data Mesh ch. 9; decision 2026-11-15, conflicts #20.)
20. Do not create a Data Owner role: reach the Azure "data owner" level by holding Data Contributor (data) and Institution Owner (management), each through an eligible assignment, with Data Contributor compiled only to raw, staging and sandbox scopes (rule 23). Why: the platform keeps six built-in roles and makes broad access time-bound. (Azure docs 06, 08; decision 2026-11-15, conflicts #21.)
21. Treat every platform role definition as a centrally owned, versioned template: an institution uses it only by supplying parameters in its folder (scope, its own groups, condition values), CI validates them against the schema and the upper policy layers, and every compiled policy records the role-definition version it came from; institutions never edit role definitions, Ranger JSON or the Ranger UI. Why: templates keep repeated grants consistent and auditable, and institutions control values, not structure. (Authorization in Action ch. 9.7.1, 13 p. 263–264, 13.5.7; conflicts #6, #7.)
22. Keep reading data, reading or editing metadata, changing data, exporting or downloading data and running ETL as separate permissions reached through separate assignments: Data Reader carries select only; metadata editing is an OpenMetadata right; export and download are Superset feature permissions (trino-security.md › Paths beside the Trino plugin, rule 4); ETL writes belong to the institution's ETL group on raw and staging (Scope design and inheritance, rule 7); never define one analyst role that bundles read with export or ETL. Why: each of these permissions carries a different risk, and a bundled role hands all of them to everyone who needs one. (Data Governance DG ch. 2, 3.)
23. Let only the owning institution's pipeline identities (workload identities, identity-and-principals.md › Service accounts and ingest identities), through reviewed pipeline changes, write to the published layers (conflicts #48) and `.pub.` topics (conflicts #58); compile human Data Contributor assignments, the eligible owner tier of rule 20 included, only to raw, staging and sandbox scopes, and correct published data at its golden source or through a pipeline change (access-control-models.md › Central vs domain policy ownership, rule 3). Why: an output port changes only through the product's own transformation, which keeps copies consistent and every published change reviewed and reproducible. (Data Mesh ch. 12; decision 2026-11-16, conflicts #61; conflicts #21.)

Example (manifest):
```yaml
kind: RoleDefinition
metadata:
  id: rd-t-acme-sales-analyst          # immutable key used by assignments and CI
  displayName: t-acme Sales Analyst
spec:
  basedOn: data-reader                 # closest built-in role
  description: Read sales tables and consume sales topics for monthly reporting
  actions: []
  notActions: []
  dataActions: ["trino:select", "kafka:consume"]   # explicit, no wildcard
  notDataActions: []
  assignableScopes: ["/institutions/t-acme"]
  tests:
    - { expect: allow, access: "trino:select", resource: t_acme_lake.gold_sales.orders }
    - { expect: deny,  access: "trino:insert", resource: t_acme_lake.gold_sales.orders }
```

Example (manifest and compiled description):
```yaml
kind: RoleAssignment
metadata: { id: ra-0095 }
spec:
  principal: { type: group, id: kc-grp-5512, name: platform_t-acme_analyst }
  roleDefinition: data-reader                    # platform template; the institution sets only the fields below
  scope: /institutions/t-acme/domains/sales
  description: "Sales dashboards, ticket ACC-221"
# compiled Ranger policy description: "ra-0095 (data-reader v3): Sales dashboards, ticket ACC-221"
```

Example (rewritten, manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0270 }
spec:
  principal: { type: group, id: kc-grp-5512, name: platform_t-acme_analyst }
  roleDefinition: data-reader            # select; consume, describe — never update or delete
  scope: /institutions/t-acme/domains/sales
  description: "Consumer of the sales output ports, ticket ACC-512"
# compiles to t_acme_lake.gold_sales.* (published layers) and t.acme.sales.pub.* only
```

## Scope design and inheritance
Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 03, 05, 14 (merged 2026-11-14); Authorization in Action, ch. 10 (merged 2026-11-15); Apache Ranger wiki, p. 4 (merged 2026-11-15); Attribute-Based Access Control, ch. 3.10, 5.3.1 (merged 2026-11-15); Kafka: The Definitive Guide, 2nd ed., ch. 10, 11 (merged 2026-11-15); Trino: The Definitive Guide, 2nd ed., ch. 4, 10 (merged 2026-11-16); Data Governance: The Definitive Guide, ch. 3, 7 (merged 2026-11-16); Data Management at Scale, 2nd ed., ch. 3 (merged 2026-11-16); Data Mesh, ch. 3 (merged 2026-11-16)

1. Use one ordered scope hierarchy: platform, institution (Ranger security zone), domain (catalog, schema or topic prefix), dataset (table or topic), column (Trino only), and scope every assignment on this axis only, never on a cross-cutting organisational axis such as a department; on Trino every compiled resource is a catalog → schema → table → column path. Why: Azure scopes form a strict parent-child chain in which each level is more specific, two scope axes give contradictory answers for one request, and Trino presents every data source in these four levels. (Azure docs 03; Authorization in Action ch. 10.1.1; Trino DG ch. 4; DMaS ch. 3.)
2. Make a grant at a level cover all its children and nothing outside it, by generating the wildcard Ranger resources (`*`, prefixes, `isRecursive`) in CI from the scope path, and write identical rules once on their container (schema pattern, topic prefix or tag) rather than per object; never hand-type wildcards and never copy policies to children. Why: Azure inheritance is implicit, while Ranger inherits only through wildcards, so a missing wildcard silently breaks inheritance; one container grant covers every contained object; and review cost grows with principals times rule sets, so grouping resources keeps reviews tractable. (plugin v1; Azure docs 03; Attribute-Based Access Control ch. 3.10, 5.3.1.)
3. Write every scope as a canonical slash-separated path with fixed segment keywords and stable ID values, for example `/institutions/t-acme/domains/sales/datasets/orders`; never use display names in a path, and bind every scope and policy to the dataset's logical address (its catalog entry, compiled to catalog → schema → table or to a topic), never to an object-storage path or a partition. Why: Azure scopes are parseable strings built from IDs, which keeps them comparable, and a logical address survives schema, partition and access-mode changes. (Azure docs 03; Data Mesh ch. 3.)
4. Treat the scope path as part of an assignment's identity: the manifest key of an assignment is its scope path plus its assignment ID. Why: an Azure role assignment is an extension resource named by its scope and its ID. (Azure docs 03.)
5. Look up a dataset's canonical scope from the catalog (OpenMetadata) instead of typing it. Why: resource-level scopes are the hardest to determine and Azure points to the resource ID as their source. (Azure docs 03.)
6. Use platform scope only for platform operators and guardrail tag policies across all institutions; never for data grants, and never grant at platform scope expecting row filters to hide other institutions. Why: Azure's level above subscriptions serves broad governance, and an institution-level data grant must not reach another institution's zone. (Azure docs 03, 14.)
7. Name schemas `<layer>_<domain>` (for example `gold_sales` in catalog `t_acme_lake`) and compile a domain scope to the schemas of the published layers only, listed in the platform folder (for example `gold_<domain>`); grant raw and staging layers only at layer or dataset scope to the institution's ETL group and ingest principals, and let CI reject analyst assignments on raw or staging schemas. Why: one naming rule keeps scope paths, compiled resources and the runnable examples consistent, while a pattern across all layers would let analysts read unvalidated raw data. (decision 2026-11-15; Hadoop Security ch. 10, 13; Data Governance DG ch. 3; decision 2026-11-15, conflicts #48.)
8. Give every dataset one owning institution whose manifest holds its policy; when several parties share the data, write their agreed restrictions once in the owning zone, and turn a restriction that must hold platform-wide into a platform tag guardrail; institutions never author deny items. Why: NIST protects shared data by the most restrictive policy, and this achieves it inside the additive model. (NIST SP 800-162 §3.2.1.2; decision 2026-11-15, conflicts #27.)
9. Compile each institution to exactly one Ranger security zone `t-<institution>` whose resources list the institution's catalogs and schema patterns in Trino and its topic and consumer-group prefixes in Kafka (for example `t_acme_lake` and `t.acme.*`), plus the source-alias forms of those prefixes on a mirror target cluster (kafka-security.md › Cross-cluster mirroring, rule 5); every catalog belongs to exactly one institution and carries its ID in its name, and an institution may hold several catalogs (trino-security.md › Data source credentials and cluster separation, rule 1); keep every institution resource inside its zone and leave only platform resources outside zones. Why: the zone is the Azure subscription equivalent, a catalog binds one connector to one data source and is the coarsest unit Trino can separate, and a resource left outside its zone is decided by unzoned policies instead of its institution's. (Apache Ranger wiki p. 4; Kafka DG ch. 10, 11; Trino DG ch. 4, 10; Data Governance DG ch. 7; DMaS ch. 3.)
10. Keep one zone per institution even when parts of it differ in trust level, region or security profile: a partner with a different trust level becomes its own institution, and differences inside an institution are expressed with tags, guardrails and the pool or silo cluster choice of conflicts #14. Why: extra zones per profile multiply policies and break the institution-equals-subscription mapping. (DMaS ch. 3; decision 2026-11-16, conflicts #57.)

Example (manifest and compiled Ranger resource):
```yaml
kind: RoleAssignment
metadata: { id: ra-0007 }
spec:
  principal: { type: group, id: kc-grp-5512, name: platform_t-acme_analyst }
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/sales
# compiled by CI into zone t-acme, service trino:
#   catalog: t_acme_lake   schema: gold_sales (published layers only)   table: *   column: *
```

## Role assignments: identity, idempotency, limits
Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 01, 04, 08, 14 (merged 2026-11-14); NIST SP 800-162, §3.2.2.11 (merged 2026-11-15); Authorization in Action, ch. 1, 5, 9, 14 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 1 (merged 2026-11-16); Data Management at Scale, 2nd ed., ch. 8 (merged 2026-11-16).

1. Give every assignment an assignment ID, a principal ID, a principal type (user, group, service principal), a role definition ID, a scope, a description and, when used, a condition with its condition version. Why: Azure assignments carry exactly this property set. Use condition version 2.0 only. (Azure docs 04; research: Microsoft Learn, Troubleshoot Azure role assignment conditions, updated 2026-06-11: version 1.0 gives an invalid-condition error, accessed 2026-11-15.)
2. Identify principals by their immutable Keycloak ID and resolve them to Ranger user or group names at compile time; display and login names are for humans only. Why: Azure assignments reference object IDs, while Ranger keys principals by name. (Azure docs 04.)
3. Declare the principal type explicitly in every assignment: humans always through groups; a service client may be assigned directly as a service principal, one entry per client with an owner and a review date. Why: a wrong principal type causes deployment errors, and group assignment keeps human access reviewable and the assignment count low. (Azure docs 01, 04, 08, 14; conflicts #12.)
4. Make every assignment ID unique across the platform, set it explicitly in the manifest, keep it unchanged across redeploys, never reuse an old ID for a different grant, and copy it into the compiled Ranger policy's description or label; deploying the same manifest twice must change nothing in Ranger. Why: Azure requires tenant-wide unique assignment names, rejects reuse, and asks infrastructure-as-code to set names explicitly. Assign IDs sequentially by hand (`ra-0042`); never derive them from a hash of scope, principal and role, because re-creating a removed grant would then bring back its old ID. (Azure docs 04; decision 2026-11-15.)
5. Create every assignment only from an access request that names the use case, is approved by the institution's Access Administrator (the steward of conflicts #13) and covers only the requested resource; require a short justification in every assignment's description (purpose, ticket, owner, and the ID of the written policy, requirement or ADR the grant implements) and carry it into the Ranger policy description, so that a change to a written policy finds every affected assignment by its ID. Why: the description tells auditors why access exists and whether it is still needed, where an access list shows only what someone can do, NIST requires that a written-policy change can be traced to every digital policy derived from it, and a request bound to its use case keeps the grant to what was asked for. (Azure docs 04; NIST SP 800-162 §3.2.2.11; Authorization in Action ch. 5.1.5, 14.2.1; Data Governance DG ch. 1; conflicts #13.)
6. Detect assignments whose principal no longer exists in Keycloak and remove them through Git, at the latest before each access review. Why: Azure keeps such assignments as "Identity not found" and Ranger keeps names of deleted users and groups in policies. (Azure docs 04.)
7. Treat removal of an assignment as revocation everywhere: the compiler removes the policy item from every Ranger service, and the revocation targets of conflicts #3 apply (policy change 30 s; group removal one usersync cycle; tokens by lifetime). Why: Azure replicates assignments globally so revocation holds everywhere, yet a deleted principal keeps access until its token expires. (Azure docs 01, 04; conflicts #3.)
8. Track the number of assignments per zone and report it; set a fixed budget with an alert only once the first institutions' data or a load test gives a value, consolidate user or service assignments that share role and scope, and when one principal holds one role at several scopes keep only the scope it needs. Why: Azure assignment limits cannot be raised and its remedies are least-privilege clean-ups. (Azure docs 14; decision 2026-11-15.)
9. Treat assignments that differ only in their condition as distinct when looking for duplicates. Why: Azure's duplicate queries take conditions into account. (Azure docs 14.)
10. Express an exception as a separate, marked assignment next to the standard one: narrowly scoped, time-bound through `validitySchedules`, with a ticket in its description; never as a new role, an edited role definition or an allow without an end date. Why: exception roles multiply until nobody can predict what an assignment grants, and in the book's case a "temporary" override stayed active for nine months. (Authorization in Action ch. 1.2.3, 9.7.2, 14.2.5, 14 Do/Don't.)
11. Do not use the `{USER}` macro for human data access; humans get access only through groups. Revisit only if per-user sandbox schemas are introduced, then as a manifest condition "principal name equals resource segment" without `delegateAdmin`. Why: a per-user macro grant is invisible in group-based reviews. (Apache Ranger wiki p. 5; decision 2026-11-15, conflicts #40; conflicts #12.)
12. Share data with another institution only as a column-scoped Data Reader assignment in the owning zone to a named group of the receiving institution, never a write role: approved by the owning Institution Owner through an access request that records the agreement (purpose, consuming environment, row-filter criteria, validity and approver) in the assignment, time-bound through `validitySchedules`, still under guardrails and masks, with the conflict classes of role-engineering.md › Static and dynamic separation of duties, rule 3 checked in CI; decide it in the owning zone against the receiving group itself, never through a view, a copy or a delegation that lets the receiving institution's administrators re-grant it; serve it only through Trino (and Superset on top of it), never Spark or Kafka, and generate its row filter from the agreement, with an unfiltered item for the owning institution's own assigned groups. Why: a Spark job runs with its own institution's storage credentials and cannot reach the owner's data, and a row filter that lists only the receiver would hide every row from the owner; further, the owner keeps control and the share stays reviewable, the recorded agreement shows what was allowed and why, outsiders cannot judge risk outside the data's context, delegated trust loses control after the grant, and a view would carry the owner's rights (conflicts #15). (Hadoop Security ch. 13; DMaS ch. 8; decision 2026-11-15, conflicts #46; trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #65.)
13. Run every grant across an institution boundary through five steps in order, each using the previous step's output: onboard (the dataset is registered, owned and classified, attribute-and-tag-design.md › Classification to policy chain, rule 1), request (an access request carrying the agreement of rule 12; a requester already in a purpose group that holds the share needs no new grant, conflicts #12), attributes (principal attributes from Keycloak, data attributes from OpenMetadata, the consuming environment), decide and provision (the compiler emits the assignment with its row filter, masks and `validitySchedules`), and review (audit-design.md › Access review process and cadence, rule 10); a step whose input is missing stops the flow, and data the consumer derives starts it again. Why: each step depends on what the previous one produced, and an agreement that is never reviewed cannot prove it was respected. (DMaS ch. 8; conflicts #12.)

Example (manifest):
```yaml
kind: RoleAssignment
metadata:
  id: ra-0042                          # explicit, unique platform-wide, never reused
spec:
  principal: { type: group, id: kc-grp-5512, name: platform_t-acme_analyst }
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/sales/datasets/orders
  description: "Monthly sales reporting, ticket ACC-112"
# compiled Ranger policy description: "ra-0042: Monthly sales reporting, ticket ACC-112"
---
kind: RoleAssignment
metadata: { id: ra-0043 }
spec:
  principal: { type: group, id: kc-grp-5512, name: platform_t-acme_analyst }
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/sales
  requirement: WP-ACME-007            # written policy this grant implements
  description: "Quarterly revenue reporting, ticket ACC-140"
# compiled Ranger policy description: "ra-0043 [WP-ACME-007]: Quarterly revenue reporting, ticket ACC-140"
```

Example (manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0090 }
spec:
  principal: { type: group, id: kc-grp-5512, name: platform_t-acme_analyst }
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/finance/datasets/ledger
  exception: true                       # marked override next to the standard assignment ra-0043
  description: "Year-end close check, owner t-acme finance lead, ticket ACC-305"
  validity: { start: "2026-12-28", end: "2027-01-10" }
```

Example (rewritten, manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0250 }
spec:
  principal: { type: group, id: kc-grp-7020, name: platform_t-beta_risk-analyst }
  roleDefinition: data-reader                          # read-only across the boundary
  scope: /institutions/t-acme/domains/sales/datasets/orders
  columns: [order_id, order_date, region, amount]
  agreement: { purpose: financial-reporting, environment: prod, rowFilter: "region = 'north'", approvedBy: t-acme-institution-owner }
  description: "Share to t-beta, request SHR-14"
  validity: { end: "2027-06-30" }
```

## Deny assignments and guardrails
Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), page 05 (merged 2026-11-14); Authorization in Action, ch. 7, 9, 15 (merged 2026-11-15); Apache Ranger wiki, p. 1, p. 2 (merged 2026-11-15); Attribute-Based Access Control, ch. 4.4.3, 5.3.2 (merged 2026-11-15); Hadoop Security, ch. 7 (merged 2026-11-15); Kafka: The Definitive Guide, 2nd ed., ch. 11 (merged 2026-11-15).

1. Let only the platform team create deny rules, only as a few guardrail tag policies (for example on tables tagged `RESTRICTED`; `PII` and `RESTRICTED` columns are masked, never denied, rule 10) for platform-wide constraints that must win over any allow, generated from the platform folder; institution manifests contain no deny items, CI rejects deny items and `override` priority anywhere else, and manual edits are reported as drift. Why: Azure deny assignments are created by Azure and system protected; Ranger accepts deny items in any policy and cannot restrict them itself; one platform guardrail replaces a check repeated in every institution; and a prohibition expresses a constraint that grants cannot, while an exception to an inherited grant is made by narrowing the allow (rule 2). (Azure docs 05; Authorization in Action ch. 7.2.1, 9.6, 15.3.1; Apache Ranger wiki p. 1, p. 2; Attribute-Based Access Control ch. 5.3.2; conflicts #1, #6.)
2. Remove access by deleting or narrowing the allow assignment, never by adding a deny; platform role definitions hold only allow entries and rely on default deny. Why: deny is a guardrail, not an access-management tool, and under default deny removing the allow is the whole revocation. (Azure docs 05; Apache Ranger wiki p. 1; Hadoop Security ch. 7; Kafka DG ch. 11; conflicts #5.)
3. Give each guardrail at least one denied access type, written per plane, with principals = group `public` and exceptions only for the eligible break-glass group. This exception is the platform's only permit-overriding path and stays named, time-bound and audited (conflicts #8). An exception only skips the deny, so an activated break-glass member still needs a matching allow assignment. Record the reason for every exception; platform service principals are never an exception. Why: Azure deny assignments name at least one action and support "all principals" with excluded principals, standing exceptions would bypass the guardrail, a permit-overriding policy bypasses every other restriction, and in Ranger a deny-exception match moves evaluation on to the allow phase instead of granting. (Azure docs 05; Apache Ranger wiki p. 1; Attribute-Based Access Control ch. 4.4.3; conflicts #8, #9.)
4. Use "everyone" only on the deny side, never in the exception list. Why: Azure allows All Principals only in the principals list. (Azure docs 05.)
5. Apply a guardrail to all child scopes by default (wildcard resources); a guardrail narrowed to part of the hierarchy records its reason. Why: Azure deny assignments inherit unless explicitly stopped, and Ranger has no such flag, so narrowing happens through the resource path. (Azure docs 05.)
6. Give every guardrail a name unique within its scope and a description. Why: Azure requires unique deny names per scope. (Azure docs 05.)
7. Show guardrails, including inherited ones, in every zone's access view, and let operators list the guardrails that affect a user through nested groups. Why: Azure lets anyone who can read assignments see deny assignments at and above a scope, transitively through groups. (Azure docs 05.)
8. Do not write "deny everyone else" rules to emulate default deny. Why: where no allow item matches, Ranger already denies, and extra denies only add guardrails nobody owns. (Azure docs 05.)
9. Keep guardrail tag policies to deny items plus the break-glass deny exception; CI rejects allow items in tag access policies. Unmasked reads go through tag masking only: every institution zone has its own tag mask policy (`{zone}-tag-mask`), and the compiler adds a PII Reader or Restricted Reader group to that zone policy's unmasked item only on activation, with the activation window as that item's validity; the reader also needs its normal assignment in the zone. CI rejects an unmasked tag item outside a zone. Confirm zone-scoped tag policies in the lab before rollout. Why: a tag allow decides before any resource policy and would apply in every linked zone, bypassing institution-scoped assignments, and an unzoned unmasked item reaches as far. (Apache Ranger wiki p. 2; decision 2026-11-15, conflicts #37; trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #63.)
10. Treat `RESTRICTED` by level: on a column it is a mask (default `MASK_NULL`) lifted only by an activated Restricted Reader assignment; on a table it is the deny guardrail of rule 3, with break-glass as its only exception. A workload identity holds Restricted Reader only as a time-bound direct assignment approved like an activation, and never reads a table tagged `RESTRICTED` at table level. `PII` is mask-only and never denied. Why: a column-level deny makes `SELECT *` fail for every reader and leaves no path for a legitimate purpose or pipeline, while a mask keeps queries working and the value hidden; the table-level deny still confines the most sensitive datasets. (trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #62, #72.)

Example (Ranger tag policy JSON):
```json
{
  "service": "platform-tags",
  "name": "guardrail-restricted-read",
  "description": "Platform guardrail: no read on tables tagged RESTRICTED (level=table) except activated break-glass; RESTRICTED columns are masked (rule 10)",
  "resources": { "tag": { "values": ["RESTRICTED"] } },
  "denyPolicyItems": [
    { "groups": ["public"], "accesses": [ { "type": "trino:select", "isAllowed": true }, { "type": "hive:select", "isAllowed": true } ],
      "conditions": [ { "type": "expression", "values": ["TAG.level == 'table'"] } ] }
  ],
  "denyExceptions": [
    { "groups": ["platform_breakglass"], "accesses": [ { "type": "trino:select", "isAllowed": true }, { "type": "hive:select", "isAllowed": true } ] }
  ]
}
```

## Policy layers: platform guardrails, shared platform policies, institution policies

Source: Authorization in Action, ch. 6, 13, 15 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 7 (merged 2026-11-16); Data Mesh, ch. 5, 9 (merged 2026-11-16)

1. Assign every policy to exactly one of three layers, each with one owner: platform guardrails (platform team: tag deny on tables tagged `RESTRICTED`, the per-zone tag mask policies, institution isolation, declared platform principals; Azure root management group and deny assignments), shared platform policies (platform team: rules that hold across institutions and tools, such as a maintenance window or on-call-only change, defined once; Azure management-group assignments), and institution policies (the institution's code owners: business access inside its own zone only; Azure subscription and resource-group assignments); a decision belongs to the two platform layers only when it cuts across every dataset (access-control-models.md › Central vs domain policy ownership (federated governance), rule 2), and one dataset may carry policies of several layers at once. Why: layering gives institutions autonomy under uniform constraints, a rule defined once does not drift per tool, and one owner per policy avoids duplication. (Authorization in Action ch. 6.2.4, 13.5.6, 13.5.9, 15.3.1–15.3.3; Data Mesh ch. 5, 9; conflicts #7, #9.)
2. Let a lower layer only grant inside the limits of the upper layers, never broaden or override them, and check this in CI before deploy (policy-testing-and-verification.md › Verification properties, rule 2). Why: upper layers are authoritative, and isolation must hold whatever an institution configures. (Authorization in Action ch. 13.4.1, 13.5.9; NIST SP 800-162 §3.2.1.2; Data Governance DG ch. 7.)
3. Map the layers onto Ranger as follows: guardrails are platform tag policies, evaluated before the zone's resource policies; shared platform policies are platform-folder policies compiled as tag policies or as generated items in every zone, never as unzoned resource policies, for example with time conditions; institution policies are the allow items of the institution's zone; decisions of every layer are audited. Why: Ranger evaluates tag and zone policies in one engine, so the layer order has to come from where each policy lives. (Authorization in Action ch. 13.4.1–13.4.3, 15 Azure mapping; decision 2026-11-15, conflicts #36.)

Example (repository layout, conflicts #7):
```text
policies/
  platform/guardrails/restricted-read.yaml      # layer 1, owner: platform team (tag deny)
  platform/shared/maintenance-window.yaml       # layer 2, owner: platform team (time condition)
  institutions/t-acme/assignments.yaml          # layer 3, CODEOWNERS: t-acme Institution Owner
```

## Conditions: format, attributes and patterns
Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 04, 10, 11, 12, 13 (merged 2026-11-14); NIST SP 800-162, §2, §2.1, §3.1.1, §3.1.2 (merged 2026-11-15); Authorization in Action, ch. 3, 5, 6, 10, 15 (merged 2026-11-15); Attribute-Based Access Control, ch. 3.5, 3.13 (merged 2026-11-15).

1. Attach every condition to exactly one assignment and let it only narrow that assignment; never compile a condition, or its failure, into a deny item, and never write a condition that grants by testing a role or group as a plain attribute (for example "role is analyst and institution is acme, then allow"): roles enter a decision only as assignments. Why: Azure conditions filter the permissions of their assignment and cannot explicitly deny, and a role used as a plain attribute no longer bounds the permission set. (plugin v1; Azure docs 04, 10; Attribute-Based Access Control ch. 3.13.)
2. Start with plain role assignments for broad job functions, treating the role as one principal attribute, and add a condition only where the decision depends on what roles cannot express (time, relationship to the resource, sensitivity, purpose, region or clearance) and principals and resources carry rich attributes that the decision combines: for finer granularity, for multi-factor needs (location, training, project) that would otherwise create narrow roles, or to replace many assignments that differ only by an attribute value, for example one group assignment with "principal project equals dataset project tag" instead of one assignment per project. Why: Azure positions RBAC for most cases and conditions for cutting assignment counts; NIST treats RBAC as ABAC over the role attribute, warns that static roles for dynamic needs cause role explosion, and notes that attribute policy costs more to build and run; one reusable attribute policy replaces many roles while roles stay the base. (Azure docs 10, 12, 13; NIST SP 800-162 §2, §2.1, §3.1.2.1; Authorization in Action ch. 5.3, 6.2, 6.2.4, 6.3.3.)
3. Base conditions on governed attributes with business meaning (project, stage, classification; Keycloak groups and attributes, OpenMetadata tags) and treat their values as changeable; never compare against literal department names, user IDs, job titles or project codes. Why: access then follows people across teams without editing assignments, and a renamed literal silently locks out legitimate users. (Azure docs 10; Authorization in Action ch. 10.1.3, 10.1.5.)
4. Put conditions only on data-plane access types, and require the assignment-write right at the assignment's scope to add or change one. Why: Azure supports conditions only on data actions and keeps them at their assignment's scope. (Azure docs 10.)
5. Write a condition as a list of targeted access types plus an expression; non-targeted access types of the role pass unchanged, and the compiler attaches the condition to a Ranger policy item that holds exactly the targeted types. Why: Azure evaluates "not a targeted action, or the expression is true", and a condition that targets nothing restricts nothing. (Azure docs 11.)
6. Target every access type that reaches the same data and put the restricting condition on all of a principal's assignments that carry those types. Why: under the additive model one unconditioned assignment or one missed access type reopens the access. Azure states that further assignments granting the same data action, with or without conditions, leave a condition unenforced, and that a condition targeting only one of two actions granting the same permission is bypassed. (Azure docs 12; research: Microsoft Learn, Troubleshoot Azure role assignment conditions, updated 2026-06-11, accessed 2026-11-15.)
7. When one condition targets several access types, use only attributes available to all of them, and join per-type blocks with AND. Why: Azure narrows attributes to the intersection and requires every block to pass. (Azure docs 11.)
8. Prefix every attribute with its source: principal, resource, request or environment. Why: the source decides where the value is read. (Azure docs 11.)
9. Parenthesise every expression that mixes AND and OR across three or more parts; CI rejects it otherwise. Why: Azure treats such mixes as ambiguous. (Azure docs 11.)
10. State case sensitivity for every string comparison, and use `*` and `?` only with like-style operators, escaped when meant literally. Why: the wrong variant or an unescaped wildcard silently widens or narrows access. (Azure docs 11.)
11. Compare multi-valued attributes with an explicit quantifier (any-of or all-of on each side); a user attribute matched against a dataset tag uses any-of. Why: each quantifier gives a different result on multi-valued tags. (Azure docs 11, 12.)
12. Guard every attribute that may be missing with an existence check, and design the condition so that a missing attribute narrows access rather than widening it: a missing attribute is undefined and never a match, and a request whose required context is missing or failed is not allowed by that assignment, even for a valid identity. Why: Azure documents no defined result for a missing attribute, only that evaluation may give an unexpected response, and it offers an `Exists` operator for this case; weak or missing context makes authorization fragile and must end in deny; and treating an absent attribute as undefined applies least privilege to attributes. (Azure docs 11; research: Microsoft Learn, Troubleshoot Azure role assignment conditions, updated 2026-06-11; Azure role assignment condition format and syntax, updated 2026-03-25, accessed 2026-11-15; Authorization in Action ch. 3.8, 15 p. 297; Attribute-Based Access Control ch. 3.5.)
13. Keep a condition to at most 5 expressions unless a reviewed reason is recorded. Why: Azure caps the visual editor at 5 expressions to keep conditions reviewable. (Azure docs 10.)
14. Build conditions from these patterns: dataset tag value; required classification tag before a dataset is published (catalog or CI gate); location prefix (topic or schema prefix); principal attribute equals dataset tag; time window (available from a date); network origin, required only for `RESTRICTED` data. Why: the Azure example catalogue reduces to these six patterns. (Azure docs 10, 12.)
15. Introduce conditions and tag policies step by step: start with datasets that are well defined, owned and documented (for example PII masking on the gold tables of `t_acme_lake`), use the platform-wide attribute and policy definitions from the first step, widen only after governance sign-off, and tell affected users beforehand that some legitimate requests may be denied. Why: pilots refine definitions and governance, unclear protection needs raise cost sharply, and policies never match business needs perfectly at first. (NIST SP 800-162 §3.1.1, §3.1.2.2, §3.1.2.4.)

Example (manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0051 }
spec:
  principal: { type: group, id: kc-grp-5512, name: platform_t-acme_analyst }
  roleDefinition: data-reader
  scope: /institutions/t-acme
  description: "Project-scoped analytics, ticket ACC-130"
  condition:
    version: "2.0"                     # 1.0 is rejected as invalid
    targets: ["trino:select", "kafka:consume"]
    expression: "exists(@Principal.projects) and any(@Principal.projects) in @Resource.tag.project"
    caseSensitive: false
```

## Delegating assignment management with conditions
Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 08, 09, 17 (merged 2026-11-14); Attribute-Based Access Control, ch. 4.5.2, 5.5, 5.8.2 (merged 2026-11-15); Hadoop Security, ch. 6, 7, 13 (merged 2026-11-15).

1. Delegate assignment management to an institution when request volume, waiting users or local knowledge of who needs access justify it. Why: these are the reasons Azure gives for delegating. (Azure docs 09.)
2. Delegate only through the assignment-only Access Administrator role (add and remove assignments in the institution folder, read metadata, no data), never through Institution Owner or Ranger admin, zone-admin or `delegateAdmin` rights; an institution administers its own namespace through two assignments limited to its zone and made through Git: Institution Owner (manage datasets) and Access Administrator (assignments), neither with rights outside the zone or implicit data read. Why: full rights let a delegate assign any role to anyone, including themselves, Ranger cannot constrain its own delegation, and namespace delegation exists so that an institution administers itself without platform rights. (Azure docs 08, 09, 17; Apache Ranger wiki p. 4, p. 12; Hadoop Security ch. 6, 7, 13; conflicts #6, #20.)
3. Constrain every delegate in CI with four lists: roles they may add, principal types they may assign, principals they may assign (only their institution's own groups), and assignments they may remove; check additions and removals separately. The lists never exceed what the delegating authority itself holds, and every delegation traces back to its record in the platform folder; CI rejects a delegation or a delegated assignment whose chain does not reach that record. Why: Azure conditions on assignment write and delete restrict roles, principal types and principals, with separate add and remove rules, and delegated authority can be verified only when each delegate passes on rights it holds and the chain ends at a trusted root. (Azure docs 08, 09, 17; Attribute-Based Access Control ch. 4.5.2, 5.5.)
4. Never let a delegate assign a role that can itself assign roles (Institution Owner, Access Administrator) or a privileged data role such as PII Reader, and never to themselves. Why: constrained delegation exists to stop onward delegation and self-elevation. (Azure docs 09, 17; conflicts #13.)
5. Before delegating, answer five questions in the delegation record: which roles may be assigned, which assignments may be removed, which principal types, which principals, and whether any assignment may be removed at all. Why: these are the inputs Azure lists for building the condition. (Azure docs 09.)
6. Give automation clients that manage assignments the same constrained delegation, never full rights. Why: conditions allow automation without granting a service account full permissions. (Azure docs 09.)
7. For a domain that needs its own administrator (for example sensitive data), use an Access Administrator assignment whose allowed-roles list is a fixed family of data roles that excludes owner and access-administration roles. Why: Azure ships data-access-administrator roles fixed to one role family to improve separation of duties. (Azure docs 09.)
8. Record every assignment a delegate makes with the delegate's identity as its issuer (Git author and approver), include it in access reviews, and tell the delegate which roles they may now assign. Why: delegated changes must stay attributable, a grant whose issuer cannot be traced cannot be verified, and Azure ends delegation by informing the delegate. (Azure docs 09; Attribute-Based Access Control ch. 4.5.2, 5.8.2.)
9. Let a delegate remove only assignments it created itself by default; an institution may widen this through the delegate's remove list. Why: removing someone else's grant is a separate decision that the institution must take explicitly. (Azure docs 09; decision 2026-11-15.)

Example (manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0060 }
spec:
  principal: { type: group, id: kc-grp-5600, name: platform_t-acme_access-admin }
  roleDefinition: access-administrator
  scope: /institutions/t-acme
  description: "Local access management for t-acme, ticket GOV-21"
  delegation:
    add:
      roles: [data-reader, data-contributor]
      principalTypes: [group]
      principals: ["platform_t-acme_*"]
    remove:
      roles: [data-reader, data-contributor]
```

Example (rewritten, manifest; delegation record the CI chain check resolves):
```yaml
kind: Delegation
metadata: { id: del-t-acme-01 }          # lives in policies/platform/, the trusted root
spec:
  delegate: { type: group, name: platform_t-acme_access-admin }
  grantedBy: platform-team                # holds every role listed below
  scope: /institutions/t-acme
  add:    { roles: [data-reader, data-contributor], principalTypes: [group], principals: ["platform_t-acme_*"] }
  remove: { roles: [data-reader, data-contributor] }
# an assignment in institutions/t-acme/ authored by a member of platform_t-acme_access-admin
# is accepted only if its role and principal fall inside del-t-acme-01
```

## Best practices and limits
Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 08, 14, 17 (merged 2026-11-14); Authorization in Action, ch. 13 (merged 2026-11-15); Trino: The Definitive Guide, 2nd ed., ch. 10 (merged 2026-11-16); Data Governance: The Definitive Guide, ch. 3 (merged 2026-11-16); Data Management at Scale, 2nd ed., ch. 3, 8 (merged 2026-11-16); KVKK Personal Data Security Guide, §3.3 (merged 2026-11-16).

1. Use roles to separate duties inside a team instead of giving everyone broad rights. Why: Azure positions RBAC as the tool to allow only certain actions at a given scope. (Azure docs 08.)
2. Keep at most 3 Institution Owner principals per institution and check it in CI. Why: Microsoft recommends at most 3 subscription owners, and the platform maps a subscription to an institution. (Azure docs 08; decision 2026-11-15.)
3. Remove privileged assignments that a job-function role can replace; when a privileged role is unavoidable, assign it at domain or dataset scope rather than institution or platform scope. Why: privileged roles widen the attack surface and a narrow scope contains damage. (Azure docs 08.)
4. Provision every new institution at onboarding from Git with a unique institution ID that does not depend on any database key and appears in its zone, catalog, schema, topic-prefix and group names; a directory record naming its owner, its identity provider and its cluster model (pool or silo, trino-security.md › Data source credentials and cluster separation, rule 4); its standard groups, standard built-in role assignments and platform guardrails; and, before any of its existing data is connected, a review of its classification and access model mapped onto platform tags and built-in roles. Why: the Cloud Adoption Framework makes the platform team responsible for these when a landing zone is created, a shared naming convention without its own ID is a convention, not a boundary, a separate cluster corresponds to Azure's separate subscription, so the choice belongs to the institution's record, and data brought in under a foreign classification and access model otherwise stays ungoverned. (Azure docs 17; Authorization in Action ch. 13.3.1; Trino DG ch. 10; Data Governance DG ch. 3; decision 2026-11-14, conflicts #14.)
5. Give each institution and each environment (dev/test, prod) its own groups and service identities; never share generic groups across zones; scope every principal, resource and audit record to one institution, except the declared platform principals of conflicts #9; never assign production data tagged `PII` or `RESTRICTED` to a development or test group or service identity (security-operations.md › Exfiltration by authorized users, rule 2). Why: groups shared across zones cause misconfiguration and breaches, tenancy must be part of the authorization model, and the consuming environment is itself a decision attribute. (Azure docs 17; Authorization in Action ch. 13.3.1; DMaS ch. 8; KVKK guide §3.3; conflicts #9; cross-institution shares only per Role assignments, rule 12, decision 2026-11-15, conflicts #46.)
6. Generate every zone from the same platform blueprint: the same platform role definitions, the platform tag-service link with its guardrails (access-control-models.md › Security zones, rule 4) and the same audit settings on every service, and let CI compare every zone with the blueprint. Why: standard roles simplify administration and cut misconfiguration, and controls applied from central code make governance automatic. (Azure docs 17; DMaS ch. 3; conflicts #7.)
7. Give platform administrators access to an institution's data only through time-bound elevation, from privileged accounts kept separate from their daily accounts, and keep the number of platform-wide administrator accounts to at most 5; these accounts also have no write rights in the Ranger Admin UI and change policy only through Git. Why: this limits blast radius when an admin account is compromised. Note on the limit: Azure applies "at most 5" to Microsoft Entra ID Global Administrators, the tenant-wide role above every subscription; the platform maps that role to its platform-wide administrators. (Azure docs 17; Apache Ranger wiki p. 12; conflicts #6, #8; decision 2026-11-15, conflicts #22.)
8. Prefer eligible assignments over permanent ones for occasional access. Why: eligible assignments grant nothing until activated and do not consume the assignment budget. (Azure docs 14.)

## Privileged access (eligible, time-bound) and access reviews
Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 08, 10, 15, 16 (merged 2026-11-14); Apache Ranger wiki, p. 14 (merged 2026-11-15); Hadoop Security, ch. 6 (merged 2026-11-15); KVKK Personal Data Security Guide, §3.1 (merged 2026-11-16).

1. Make privileged roles (Institution Owner, Access Administrator) and sensitive data roles (PII Reader, Restricted Reader) eligible by default: the manifest holds the eligibility, and no Ranger policy item exists until activation. Why: eligible assignments grant nothing until activated, which cuts exposure time. (Azure docs 08, 15; KVKK guide §3.1.)
2. Require Keycloak step-up MFA and a justification for every activation, and an approval by a different person in Flowable for privileged roles. Why: PIM lets administrators demand MFA, justification and approval before activation. (Azure docs 15; conflicts #8.)
3. Give every activation a start and an end compiled into a validity schedule on the activated assignment's own policy item (Ranger 2.5 and later accept validity per policy item), so expiry revokes without action, and give every eligibility an end date; permanent eligibility needs a recorded reason. PII Reader and Restricted Reader eligibilities last at most 3 months and each of their activations at most 8 hours (`PT8H`, also the default); a request for "three months unmasked" becomes a 3-month eligibility, never a standing grant. Why: PIM is time-bound by design and its API example expires eligibility after 365 days, per-item validity lets one policy hold several time-bound assignments with their own windows, and short activations keep unmasked exposure to the working session. (Azure docs 08, 15; Apache Ranger wiki p. 14; trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #68.)
4. Do not offer activation windows shorter than 5 minutes. Why: PIM forbids shorter durations, and a window must outlast policy distribution. Do not remove an assignment within 5 minutes of creating it either. (Azure docs 15; research: Microsoft Learn, Assign Azure resource roles in Privileged Identity Management, updated 2026-09-29, accessed 2026-11-15.)
5. Allow conditions on eligible assignments. Why: temporary access should also be narrow. (Azure docs 10, 15.)
6. Route built-in and custom roles, Institution Owner included, through the same request flow. Why: PIM covers both kinds of role. (Azure docs 15.)
7. Let only the zone's Access Administrator, within its delegation lists, create eligibilities. Why: PIM requires an access-administration role to create eligible assignments. (Azure docs 15.)
8. Keep requests, approvals and justifications in Flowable history shipped to OpenSearch and immudb. Why: activation must be auditable, and Ranger audits only access. (Azure docs 15.)
9. Review privileged roles, eligible and active assignments and service principals on a recurring schedule, as defined in `ranger-audit-compliance/references/audit-design.md` › Access review process and cadence. Why: access reviews remove stale privileged access. (Azure docs 16.)
10. Keep one sealed Ranger Admin credential in OpenBao for control-plane lockout only (Keycloak, Flowable or the Git pipeline unavailable and Ranger not reachable otherwise): checkout needs two different people, every use raises a SIEM alert, every change made with it is re-applied from Git afterwards and the drift check must come back clean, the credential is rotated after each use, and each use is reviewed like a break-glass activation (conflicts #8). Data access in an emergency still uses the eligible break-glass group, never this credential. Why: Azure keeps break-glass accounts to prevent lockout, and a protected administrator of last resort prevents a state in which nobody can administer, while the platform allows no standing human admin, so the account exists but is sealed, two-person and visible. Two-person checkout and rotation after use are platform choices, not taken from the source. (Azure docs 17; Hadoop Security ch. 6; conflicts #6, #8; decision 2026-11-15, conflicts #22.)
11. Do not rely on eligible activation for Access Administrator assignments that carry delegation lists until a Flowable trial shows the lists are enforced after activation. Why: Azure documents that role assignment management for custom roles with conditions cannot be delegated through PIM, so the pattern has no proven reference. (research: Microsoft Learn, Delegate Azure role assignment management to others with conditions, updated 2026-03-30, accessed 2026-11-15.)

Example (manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0075 }
spec:
  principal: { type: group, id: kc-grp-5700, name: platform_t-acme_pii-eligible }
  roleDefinition: pii-reader
  scope: /institutions/t-acme/domains/sales
  description: "Fraud case analysis, ticket SEC-88"
  assignmentType: eligible
  eligibility: { end: "2027-06-30" }
  activation: { maxDuration: PT4H, requireMfa: true, requireJustification: true, requireApproval: true }
# on approval the compiler emits a Ranger policy item with validitySchedules = activation window
```

## Differences between Azure and Ranger to keep in mind
Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 01–05, 07–11, 13–16 (merged 2026-11-14); Authorization in Action, ch. 5 (merged 2026-11-15); Apache Ranger wiki, p. 1, p. 3, p. 4 (merged 2026-11-15).

1. Keep role definitions and assignments in the manifest, versioned in Git: Ranger has no role-definition or assignment object and stores only compiled policy items. Why: without the manifest record, nobody can answer "who has what where, and why", and versioned roles are one of RBAC's strengths. (Azure docs 01, 02, 04; Authorization in Action ch. 5 Table 5.5.)
2. Expand exclude lists and wildcard access types at build time; a new engine access type needs a manifest change. Why: Ranger has no subtraction and its access types are fixed per service definition. (Azure docs 02, 07.)
3. Express inheritance only through generated wildcard or recursive resources. Why: Ranger has no implicit scope inheritance. (Azure docs 03; conflicts #2.)
4. Map All Principals to group `public`, excluded principals to deny exceptions, and "do not apply to child scopes" to a narrower resource path. Why: these are the nearest Ranger mechanisms. (Azure docs 05; Apache Ranger wiki p. 1.)
5. Remember that Ranger admins can edit any policy and Ranger allows resource-level deny, which Azure customers cannot do; CI and the drift check stand in for Azure's system protection. Why: Ranger will not enforce the guardrail-only deny model itself. (Azure docs 01, 05; conflicts #1, #6.)
6. Remember that Azure supports conditions only on storage data roles, while the platform applies the same grammar to Trino, Kafka and Spark access types and compiles it into Ranger conditions, tag policies or row filters. Why: the platform extends the model, and Ranger condition evaluators differ from Azure operators. (Azure docs 10, 11.)
7. Enforce delegation limits in the control plane, because Ranger `delegateAdmin` and zone admin cannot be conditioned by role, principal type or principal. Why: Azure constrains delegation with conditions that Ranger lacks. (Azure docs 08, 09; conflicts #6.)
8. Model eligibility in the manifest and Flowable, and the time window with `validitySchedules`. Why: Ranger has no dormant or eligible state. (Azure docs 14, 15.)
9. Produce access reviews from the manifest plus Ranger audit in `/ranger-audit-review`. Why: Ranger has no review object. (Azure docs 16.)
10. Set an assignment budget per zone yourself, from measured data (see Role assignments, rule 8). Why: Ranger has no hard cap on policies or items, unlike Azure's fixed limits. (Azure docs 14; decision 2026-11-15.)
11. Source principal attributes from Keycloak and sync them to Ranger, not from tokens. Why: Azure custom security attributes are not emitted in tokens, and the platform's equivalent path is usersync. (Azure docs 13.)
12. Remember that Ranger zones do not inherit from anything: a zoned resource is decided only by its zone's resource policies plus the tag policies linked to the zone, unlike Azure, where a subscription inherits assignments from its management groups. Why: platform-wide rules must therefore reach each zone through a linked tag service or through items in the zone itself (access-control-models.md › Security zones, rules 1 and 4; decision 2026-11-15, conflicts #36). (Apache Ranger wiki p. 4.)
13. Remember that Ranger row-filter and mask policies are ordered: the first item that matches the user decides, while Azure assignments have no order; the compiler emits exemption items first and the `public` default item last (trino-security.md › Row filters, masks and views, rule 1). Why: a reordered item silently changes who sees unmasked data. (Apache Ranger wiki p. 3.)
14. Do not use allow exceptions: narrow the group or the scope instead. Why: Azure subtracts actions inside a role, not principals, and a Ranger allow exception only leaves the request undetermined, which hides the intent from reviewers. (Apache Ranger wiki p. 1.)
