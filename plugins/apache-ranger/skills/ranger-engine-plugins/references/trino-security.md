# Trino authorization notes

Status: starter rules; Azure mapping, paths beside the plugin and enforcement points filled from the Azure docs, NIST SP 800-162 and Authorization in Action (2026-11-15); system versus connector access control, rule ordering and defaults, data source credentials and cluster separation, and session properties and query management filled from Trino: The Definitive Guide, 2nd ed. (merged 2026-11-16), with materialized views (conflicts #29, lab V7); no section pending; row filters, masks and views and adding an engine from the Apache Ranger wiki (merged 2026-11-15); single SQL entry, object-store credentials, BI-tool permissions and interactive impersonation limits from Hadoop Security (merged 2026-11-15); Superset export permissions from the Data Governance: The Definitive Guide engine notes (merged 2026-11-16); consumer-side stores, semantic layers and self-service sandboxes from Data Management at Scale, 2nd ed. (merged 2026-11-16).

## Starter rules

1. Use Ranger as the only access control plug-in on every Trino cluster that serves governed data, beside the connectors and covering all catalogs: never run Trino's `allow-all` or `read-only` control, and use file rules only to bootstrap a cluster before it holds governed data. Why: one decision point then covers every catalog, and a fresh Trino install authenticates nobody and allows everything. (plugin v1; Trino DG ch. 4, 10.)
2. Give every Trino user a baseline: their own queries visible, read on `information_schema` of each catalog where they hold an assignment, and session properties limited to an allow list; other users' queries, the rest of the `system` catalog and data-exposing session properties stay closed (Rule ordering and defaults, rule 3; Session properties and query management permissions, rules 1–3). Why: default deny otherwise breaks tools, and open properties or system tables let users raise limits, change what a query returns or read other users' SQL. (plugin v1; Trino DG ch. 8, 12.)

### Azure model mapping on the Trino path

Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 01, 02, 03, 05, 07, 10, 11 (merged 2026-11-14). Spark and object storage have no engine file yet; their notes are kept here as the closest file; NIST SP 800-162, §3.1 (merged 2026-11-15); Hadoop Security, ch. 6, 8, 11 (merged 2026-11-15); Trino: The Definitive Guide, 2nd ed., ch. 4, 10 (merged 2026-11-16).

1. Put `select`, `insert`, `update`, `delete` in the data lists of platform roles, and `create`, `drop`, `alter`, `grant`, `impersonate` plus Ranger admin rights in the management lists. Why: Azure keeps data actions and management actions apart. (Azure docs 02.)
2. Map scope to catalog, schema, table and column, the four levels in which Trino presents every data source, and inherit with generated `*` wildcards. Why: every Trino policy resource is a path on this hierarchy, and Ranger inherits only through wildcards. (Azure docs 03; Trino DG ch. 4.)
3. Make Trino's group provider return the same transitive groups that usersync gives Ranger. Why: groups reaching the plugin come from the engine, and a mismatch changes the decision for the same user. (Azure docs 01.)
4. Compile attribute conditions on Trino into tag-based policies, and principal-versus-row checks into row filters. Why: these are the Ranger mechanisms that can express an assignment condition on the SQL path. (Azure docs 10, 11.)
5. Validate custom roles against the Trino service definition's access types. Why: a role valid for one service definition may not be valid for another. (Azure docs 07.)
6. Check that every Ranger service definition in use (Trino, Kafka, Spark) enables deny and exceptions in policies before relying on a guardrail there. Why: deny items take effect only when the service definition enables them. (Azure docs 05; Apache Ranger wiki p. 1.)
7. Object storage: a dataset scope corresponds to a path prefix, but enforcement there depends on credential scoping, not on Ranger, and direct bucket access bypasses catalog policy entirely; therefore object-store credentials go only to engines, never to people or user-run tools, no per-user grants exist on the store, and its access log, which shows only engine credentials, is not access audit; the Iceberg catalog's object-store credential acts as that catalog's service user and caps what every Trino user of the catalog can reach (Data source credentials and cluster separation, rule 1). Why: Ranger cannot see direct object-store reads, a tool that reads table files directly ignores SQL authorization, and every query of the catalog reaches storage with that one credential. (Azure docs 03, 10; NIST SP 800-162 §3.1; Hadoop Security ch. 6, 8, 11; Trino DG ch. 10.)

### Paths beside the Trino plugin (Superset, ClickHouse)

Source: NIST SP 800-162, §3.1, §3.2 engine notes (merged 2026-11-15). Superset and ClickHouse have no engine file; Trino is the closest; Authorization in Action, ch. 4, 11, 13 (merged 2026-11-15); Hadoop Security, ch. 5, 11, 12, 13 (merged 2026-11-15); Trino: The Definitive Guide, 2nd ed., ch. 10 (merged 2026-11-16); Data Governance: The Definitive Guide, ch. 2, 7 (merged 2026-11-16); Data Management at Scale, 2nd ed., ch. 11 (merged 2026-11-16).

1. Treat Superset charts that query through Trino as covered by Ranger and prefer them to cached extracts; treat Superset direct database connections and extracts as separate access paths that must be reviewed like any other path, an extract being a copy under conflicts #10. Why: only the Trino path passes the Ranger plugin, and a cached extract duplicates data outside it. (NIST SP 800-162 §3.1; DMaS ch. 11; conflicts #10.)
2. Let an interactive service client that acts for a user (Superset) query Trino as that user through explicit impersonation rules kept as Ranger policies, so that Ranger decides on the end user, and let every other principal query only as its own user: the rules name the service client and the groups whose members it may act for, never a wildcard user or group, and also its source network where the engine supports it (Ranger's `trinouser` resource holds user names, so the compiler expands the named groups' members into user names and recompiles on every membership change, trial: ranger-policy-compile, 2026-11-16); the client opens a separate Trino session per end user and never shares one connection across users; catalogs keep service credentials toward the data sources (Data source credentials and cluster separation, rule 1); every impersonation rule is listed in the access review. Why: Trino by default lets any principal run queries as any user, a decision taken on the service client cannot be attributed to the individual, a service deciding with its own authority becomes a confused deputy, a proxy holds the rights of everyone it may impersonate, and a cached shared connection mixes identities. Pipelines never impersonate; they run as their own workload identity (identity-and-principals.md › Impersonation and delegated access rules, rules 1 and 3). (NIST SP 800-162 §3.2; Authorization in Action ch. 11.1.1, 13.4; Hadoop Security ch. 5, 11, 12, 13; Trino DG ch. 10; DMaS ch. 11; conflicts #14; decision 2026-11-16, conflicts #23.)
3. Query source data products in place through Trino, and copy data into a consumer-side store (a consuming domain's own schemas or ClickHouse) only when the products cannot be queried directly; record each such dataset in OpenMetadata as a copy, integrated data or a new data product, keep copies and integrated data inside the consuming use case with the source tags through lineage, distribute them further only with every source owner's approval (access-control-models.md › Central vs domain policy ownership, rule 3), and publish integrated data as a new data product before another domain uses it (rule 4 of that section). ClickHouse has no Ranger plugin: a governed copy there follows attribute-and-tag-design.md › Policy inheritance through lineage, rule 1; for every engine without a Ranger plugin (ClickHouse, Superset direct connections, Nessie) decide and document where enforcement happens before it holds governed data. Why: the same data must carry the same policy on every path, every copy adds governance burden, each type has its own sharing rules, and integrated data is tied to one use case. (NIST SP 800-162 §3.1; Authorization in Action ch. 4 engine notes; DMaS ch. 11; conflicts #10.) A copy that adds no new facts records its `sourceOwners`, who approve its creation and any further sharing (decision 2026-11-16, conflicts #56).
4. Treat Superset permissions as feature permissions only (which groups may open SQL Lab, build charts, manage dashboards, or export and download data), granted to Superset roles mapped from Keycloak groups; give export and download to separate groups from viewing, log the data served, and apply no-download and watermarking controls at the dashboard layer, where Ranger does not reach; keep every data decision in the Trino plugin for the end user. Why: a tool permission to run a statement still fails without the engine privilege, every tool action is controlled by a list of groups, and an export turns read access into a copy outside every control. (Hadoop Security ch. 12; Data Governance DG ch. 2, 7.)
5. Keep Superset administrators a small named platform group, separate from engine and storage superusers and from Ranger administration; keep any capability to act or view as another user disabled, and where it cannot be disabled treat every holder, tool administrators included, as able to see that user's data and audit each use. Why: tool administrators manage users, groups and permissions, and acting as another user reveals objects the logged-in user is not authorized to see. (Hadoop Security ch. 12.)
6. Give a semantic layer (Superset datasets and metrics) no row security of its own: rows and columns are limited because it queries Trino as the end user (rule 2), so Ranger row filters and masks apply; a semantic model that changes meaning (new metrics, curated joins) gets a named owner and is treated as a new data product (access-control-models.md › Central vs domain policy ownership, rule 4). Why: row security must hold at the semantic layer without a second policy store, and curation shifts ownership. (DMaS ch. 11.)
7. Keep self-service results in a sandbox schema per institution and domain that is non-certified and time-bound: assignments on it carry `validitySchedules`, nothing in it is offered as a data product, and data leaves it only when a pipeline publishes it as a governed dataset (conflicts #10) or through the Superset export permissions of rule 4; published datasets never read from a sandbox, and CI flags any lineage edge from a sandbox into a published layer. Why: local copies escape governance, and managed data must not depend on manual work. (DMaS ch. 11; conflicts #10, #48.)
8. Treat Ranger's decision on the end user as the effective right on the Superset path: Superset roles and dataset permissions may only narrow it, never widen it. Why: a second permission layer that can widen access would bypass the policies under review. (decision 2026-11-16)
9. Give each Superset dashboard the highest sensitivity label of the columns it reads (column-level lineage, attribute-and-tag-design.md › Policy inheritance through lineage, rule 4), taken from OpenMetadata, and show that label on the dashboard. Why: a viewer must see how sensitive the data behind a chart is, the label belongs to the data, not to the chart, and a column the dashboard does not read does not raise its label. (decision 2026-11-16; trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #70.)
10. Keep CSV export and download in Superset disabled by default and enable them only for holders of the separate export permission (azure-style-rbac-abac.md › Role definitions: actions, data actions and subtraction, rule 22); send Superset's event log of served queries and exports to OpenSearch; use no watermarking. Why: export moves data outside every platform control, so it is a permission of its own, and the event log is the only record of what a dashboard served. (decision 2026-11-16)
11. Let no data leave a sandbox by direct export: it leaves only through a pipeline that publishes it as a product with an owner and a classification; name sandboxes `sandbox_<domain>`. Why: data that leaves without an owner or a class escapes every later control. (DMaS ch. 11; decision 2026-11-16.)

Example (rewritten, path inventory and dataset types):
```yaml
paths:
  - { path: "Superset dataset sales_kpis via Trino as end user", enforcement: "ranger plugin (trino)", audited: true }
  - { path: "Superset cached extract of t_acme_lake.gold_sales.orders", enforcement: none, status: copy, action: "governed dataset under conflicts #10 or removed" }
datasets:
  - { id: t_acme_lake.sandbox_sales.churn_try, type: sandbox, certified: false, validUntil: "2027-01-31" }
  - { id: t_acme_ch.sales_orders_copy, type: copy, sources: [t_acme_lake.gold_sales.orders], sourceOwnerApproval: ACC-530 }
```

### Enforcement points on every path

Source: Authorization in Action, ch. 7, 11, 13, 15, 17 (merged 2026-11-15); Hadoop Security, ch. 3, 11, 13 (merged 2026-11-15); Trino: The Definitive Guide, 2nd ed., ch. 6 (merged 2026-11-16); KVKK Personal Data Security Guide, §3.3 (merged 2026-11-16).

1. List every path to governed data (interactive queries, background jobs, service-to-service calls, direct storage access) with its enforcement point, and keep an inventory of systems that use Ranger, use it partly or bypass it. Why: an unprotected route bypasses central policy, and the inventory shows where. (Authorization in Action ch. 11.1.5, 11 p. 216, 15.5.1.)
2. Let the Trino coordinator's Ranger plugin decide table, column and row access; Trino Gateway, network or mTLS rules and OAuth scopes only gate entry, and the plugin check runs even when the token is valid and in scope. Why: the edge and token scopes lack membership, sensitivity and institution context. (Authorization in Action ch. 11.1.5, 11.2.2, 13.5.1, 13.5.2, 13 engine notes.)
3. Take the principal only from the validated Keycloak token in each engine, never from a name in a query comment, header or tool parameter, and use the Ranger plugin as the one shared request-building and audit component instead of a hand-written integration per engine. Why: a caller must not assert its own identity, and hand-written integrations drift apart. (Authorization in Action ch. 7.3.1 sidebar, 7 Do/Don't, 13.5.4.)
4. Authorize AI retrieval before content is loaded: a search or retrieval-augmented path filters candidates on their metadata (institution, owner, classification tag) or checks candidate IDs through Ranger before loading content, and its queries through Trino carry the user's row filters and masks. Why: data that reaches a model's context window can leak. (Authorization in Action ch. 17.4.2, 17.4.3.)
5. Give people one way to lake data: SQL through Trino Gateway, with TLS from the client to the gateway, or another engine path that passes a Ranger plugin and is listed in the path inventory (rule 1); no network path to Trino workers or object storage, no object-store credentials, and no tool that reads table files directly (storage command-line or REST clients, engine-bypassing readers). Why: a gateway is the single choke point that acts for the user, while direct paths bypass the authorization point and a client that reads files ignores SQL authorization. (Hadoop Security ch. 3, 11.) (Hadoop Security ch. 3, 11; KVKK guide §3.3.)
6. Test on every release that objects a user cannot access are absent from listings (catalogs, schemas, tables, columns), denied when named directly, and unreachable through any direct storage path. Why: the case study expects all three outcomes, and listings reveal what exists. (Hadoop Security ch. 13.)
7. Grant no access to query pass-through table functions (functions that hand native SQL to the data source without Trino parsing it) in any catalog, so that they stay denied by default, and list each catalog's pass-through function as a bypass path in the path inventory (rule 1); an exception is only an eligible, time-bound assignment to a named engineer group on a catalog where that group already holds catalog-wide select and nothing carries a mask, a row filter or a `PII` or `RESTRICTED` tag; CI rejects every other case, and each exception is inventoried and reviewed. Why: Ranger table, mask and row-filter policies never see a pass-through query, so masks would differ by client path. (Trino DG ch. 6; decision 2026-11-16, conflicts #51.)
8. Build no AuthZEN-style decision endpoint for engines without a Ranger plugin for now: ClickHouse and Superset keep their engine-level controls and audit, and the choice is reviewed once a year. Why: an extra decision service is a new component to secure and run, while the engine-level path is already inventoried and audited. (Authorization in Action ch. 8.5.5; decision 2026-11-16.)

Example (path inventory):
```yaml
paths:
  - { path: "Superset chart on t_acme_lake.gold_sales via Trino", enforcement: "ranger plugin (trino)", audited: true }
  - { path: "Spark job writing t_acme_lake.silver_sales",        enforcement: "ranger plugin (spark)", audited: true }
  - { path: "Superset direct connection to ClickHouse",          enforcement: none, status: bypass, action: review }
```

Example (rewritten, path inventory additions):
```yaml
paths:
  - { path: "analyst SQL via Trino Gateway (TLS) on t_acme_lake.gold_sales", enforcement: "ranger plugin (trino)", audited: true }
  - { path: "object-store client with user credentials",                   enforcement: none, status: forbidden, action: no user credentials issued }
```

Example (rewritten, path inventory addition):
```yaml
paths:
  - { path: "query pass-through table function in catalog t_acme_crm", enforcement: "none below the catalog credential", status: bypass, action: "no allow item; CI rejects grants (conflict T1 pending)" }
```

### Row filters, masks and views on the SQL path

Source: Apache Ranger wiki, p. 3 Row-level filtering and column masking (2016, Ranger 0.6, Hive 2.1; Hive examples rewritten for Trino) (merged 2026-11-15); lab V2 (Trino 484); Hadoop Security, ch. 7 (merged 2026-11-15); Trino: The Definitive Guide, 2nd ed., ch. 8, 10 (merged 2026-11-16); lab V7 (Trino 484); Data Governance: The Definitive Guide, ch. 3 (merged 2026-11-16).

1. Order the items of every row-filter and mask policy from the most specific group to a last item for `public`: exempt groups first (an empty filter, or the mask type "Unmasked"), then narrower groups, then `public` with the default filter or mask; bind exemption items to groups, never to named users except service clients under conflicts #12. Why: the plugin applies the first item that matches the user, so a `public` item placed first hides every later exemption, and the base rule requires a default item for everyone. (base; Apache Ranger wiki p. 3; conflicts #12.)
2. Derive a row scope from the user with one `public` filter item that looks up the current user in a governed reference table (for example user → region, filled from Keycloak attributes by the control plane) instead of one item per group, and treat write access to that table like policy-write access, audited. Why: one lookup filter replaces many per-group items, and whoever can edit the table decides who sees which rows. (Apache Ranger wiki p. 3.)
3. Write custom masks as Trino SQL expressions that return the column's own data type, and mask dates by keeping a valid date (for example the year, with month and day set to 01). Why: the mask replaces the column value inside the query, so a type change breaks queries and tools. (Apache Ranger wiki p. 3.)
4. Create views over a table that has a mask or row filter as `SECURITY INVOKER`, or define the same mask and row filter on the view resource itself; CI rejects `SECURITY DEFINER` views over governed tables; never treat a view as a security boundary on the reading that a view runs as the querying user, which holds only for `SECURITY INVOKER`. Why: Trino 484 creates views as `SECURITY DEFINER` by default, and through such a view the base-table mask and filter were evaluated for the view owner, so a user without a base-table grant read unmasked rows; a mask defined on the view resource applied. (Apache Ranger wiki p. 3; Trino DG ch. 8; trial: verification lab V2, 2026-11-14; decision 2026-11-14, conflicts #15.)
5. Keep every mask and row filter in Ranger only: no source-side security views, no unmasked derived copy outside a governed dataset, and no A/B tests of masks or filters (use shadow evaluation against the audit log); a view defined inside a source system reaches Trino as a table and is governed by table policies like any other table. Why: a second path to the same data is a bypass, and a limit hidden in a source view is invisible to the central policy store and to other client paths. (Apache Ranger wiki p. 3; Trino DG ch. 8, 10; Data Governance DG ch. 3; decision 2026-11-14, conflicts #10.)
6. Review the mask policies and the row-filter policy of a table together. Why: both apply to the same query result. (Apache Ranger wiki p. 3.)
7. Allow creating a view or a table from a query (create table as select) only through roles that hold create on the target schema and select on every source, and test both halves in the decision table; views over governed tables also follow rule 4. Why: these statements need several privileges at once, and a missing source check would let a derived object expose data its author cannot read. (Hadoop Security ch. 7; conflicts #15.)
8. Give every Iceberg materialized view over a table with a mask or row filter the same mask and row filter on the materialized-view resource itself, and let CI reject a materialized view over a governed table that lacks them. Why: a materialized view stores its data in a separate table, and in the lab a masked user read a fresh or a stale materialized view unmasked and unfiltered, while a mask defined on the materialized-view resource applied. (Trino DG ch. 8; trial: verification lab V7, 2026-11-15; decision 2026-11-15, conflicts #29.)
9. Segregate data by region inside one table when the requirement is who may see which region's rows: keep a region column (for example `data_region`) in the table and compile, from the manifest, one row-filter item per region group (`data_region = 'TR'` for that region's group) on every client path, with cross-region reporting as a separate, documented assignment; keep separate catalogs and storage per region only where the law requires the data to stay physically in a region, because one Iceberg table has one storage location. Why: a row filter gives each group only its region's rows without duplicating tables or pipelines, while physical residency is a storage property that no filter provides. (Apache Ranger wiki p. 3; decision 2026-11-16.) Note: a single filter using a user-attribute macro (`${{USER.region}}`) instead of one item per group is used only after a lab confirms the macro on the Trino plugin of Ranger 2.8.
10. Give the owning pipeline of a table (the workload identity that holds Data Contributor on it) an unfiltered item in that table's row-filter policy when it must read its own rows, for example for `MERGE` or upsert; this is the only row-filter exemption a workload identity may hold, labelled `exemption:owning-pipeline` and reviewed every quarter (audit-design.md › Access review process and cadence). Why: the default `public` item `1 = 0` would hide every row from the writer and break its updates. (trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #67.)
11. Expect `SELECT *` on a table with `PII` or `RESTRICTED` columns to succeed with masked values, and only a table tagged `RESTRICTED` at table level to fail for every reader outside activated break-glass; cover both in the decision table. Why: column-level `RESTRICTED` is a mask, not a deny (azure-style-rbac-abac.md › Deny assignments and guardrails, rule 10), so whole-row queries and Superset charts keep working. (trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #62.)

Example (rewritten, Ranger row-filter policy JSON):
```json
{
  "service": "trino-interactive",
  "zoneName": "t-acme",
  "name": "ra-0131-rowfilter",
  "policyType": 2,
  "resources": {
    "catalog": { "values": ["t_acme_lake"] },
    "schema":  { "values": ["gold_sales"] },
    "table":   { "values": ["customers"] }
  },
  "rowFilterPolicyItems": [
    { "groups": ["platform_t-acme_sales_lead"], "accesses": [ { "type": "select", "isAllowed": true } ],
      "rowFilterInfo": { "filterExpr": "" } },
    { "groups": ["public"], "accesses": [ { "type": "select", "isAllowed": true } ],
      "rowFilterInfo": { "filterExpr": "region IN (SELECT region FROM t_acme_lake.gold_reference.user_region WHERE user_name = current_user)" } }
  ]
}
```

Example (rewritten, Ranger mask policy JSON on a materialized view):
```json
{
  "service": "trino-interactive",
  "zoneName": "t-acme",
  "name": "ra-0212-mask-mv",
  "description": "ra-0212: same mask as gold_sales.customers, conflicts #29",
  "policyType": 1,
  "resources": {
    "catalog": { "values": ["t_acme_lake"] },
    "schema":  { "values": ["gold_sales"] },
    "table":   { "values": ["customers_by_region_mv"] },
    "column":  { "values": ["email"] }
  },
  "dataMaskPolicyItems": [
    { "groups": ["platform_t-acme_pii-reader"], "accesses": [ { "type": "select", "isAllowed": true } ],
      "dataMaskInfo": { "dataMaskType": "MASK_NONE" } },
    { "groups": ["public"], "accesses": [ { "type": "select", "isAllowed": true } ],
      "dataMaskInfo": { "dataMaskType": "MASK_SHOW_LAST_4" } }
  ]
}
```

### Adding an engine to Ranger (custom service definition and authorizer)

Source: Apache Ranger wiki, p. 8 Custom plugin (2022) (merged 2026-11-15). Flink and ClickHouse have no engine file; Trino is the closest.

1. Bring a new engine (Flink, ClickHouse) under Ranger through a service definition kept in Git (policy-testing-and-verification.md › Ranger REST objects, rule 5) and a thin authorizer that calls the Ranger plugin library, never through engine-local permission rules. Why: Ranger Admin then manages the engine's policies without code changes and stays the only decision point. (Apache Ranger wiki p. 8.)
2. In that authorizer create one plugin instance at engine start with an audit handler, and pass user, groups, resource, access type, time and client IP on every call; treat a missing result as deny (access-control-models.md › Policy combining, rule 5). Why: initialisation loads the policies and starts the refresher, and the decision needs all request parts. (Apache Ranger wiki p. 8.)
3. Keep the 30 s (30,000 ms) policy poll interval on every plugin, together with the local policy cache of security-operations.md › Attribute and policy maintenance, rule 4. Why: the poll interval is the revocation target for policy changes (conflicts #3). (Apache Ranger wiki p. 8; conflicts #3.)

### System access control versus connector access control

Source: Trino: The Definitive Guide, 2nd ed., ch. 4, 10 (merged 2026-11-16).

1. Use Ranger, Trino's engine-wide system access control, as the only authorization layer, and keep connector access control (SQL grants, roles and grant option managed inside a catalog) disabled in every catalog. Why: connector grants form a second, ownership-driven policy store outside Git and its review, while the system layer covers all catalogs at one decision point. (Trino DG ch. 4, 10; conflicts #6.)
2. Make every Trino grant a Ranger policy item compiled from the manifest for a group (a service client only under conflicts #12), and never grant, revoke or create roles with SQL statements in Trino. Why: removing a group's assignment removes all its privileges at once and stays reviewable, while a SQL grant to one user is reviewed nowhere. (Trino DG ch. 10; conflicts #12, #38.)

### Rule ordering and defaults (catalog, schema, table)

Source: Trino: The Definitive Guide, 2nd ed., ch. 4, 8, 10 (merged 2026-11-16).

1. Run every Trino cluster deny by default: Ranger replaces `allow-all`, a request no allow item matches is denied (access-control-models.md › Policy combining, rule 5), and the `system` catalog is covered by explicit policies instead of being left open. Why: a fresh Trino install allows everything, and even Trino's file rules leave the `system` catalog open by default. (Trino DG ch. 10.)
2. Give every new catalog, from a standard or a custom connector, its zone resource and no allow item before it is exposed; access follows only from manifest assignments. Why: a connector can be added for any source, so each catalog is a new data path. (Trino DG ch. 4.)
3. Grant read on each catalog's `information_schema` as a baseline to every group that holds an assignment in that catalog, and rely on Trino to list only the objects the user may access (Enforcement points on every path, rule 6). Why: BI tools discover catalogs, schemas, tables and columns through `information_schema`. (Trino DG ch. 8.)

Example (rewritten, Ranger policy JSON for the baseline):
```json
{
  "service": "trino-interactive",
  "zoneName": "t-acme",
  "name": "ra-0213-infoschema",
  "description": "ra-0213: information_schema baseline for groups assigned in t_acme_lake",
  "resources": {
    "catalog": { "values": ["t_acme_lake"] },
    "schema":  { "values": ["information_schema"] },
    "table":   { "values": ["*"] },
    "column":  { "values": ["*"] }
  },
  "policyItems": [
    { "groups": ["platform_t-acme_analyst"], "accesses": [ { "type": "select", "isAllowed": true } ] }
  ]
}
```

### Data source credentials and cluster separation

Source: Trino: The Definitive Guide, 2nd ed., ch. 6, 10 (merged 2026-11-16). Catalog credentials are external service principal credentials (identity-and-principals.md › Service accounts and ingest identities, rule 7; decision 2026-11-16, conflicts #23).

1. Give every catalog its own least-privilege service user at the data source, never an end user's identity or credential pass-through, treat that user's source rights as the ceiling for every Trino user of the catalog, and let Ranger decide per user below it; when one source needs two access levels, create two catalogs with different service users and assign them to different groups. Why: all users of a catalog share one connection, so the source cannot tell them apart and the service user caps everyone. (Trino DG ch. 6, 10; decision 2026-11-14, conflicts #14.)
2. Keep catalog credentials out of configuration files and images: inject them from OpenBao at deploy time, and protect coordinator and worker nodes as carefully as the credential itself. Why: anyone who can read the stored credential opens the source. (Trino DG ch. 6.)
3. Review each catalog's source-side grants together with its Ranger policies in the access review. Why: the service user's rights at the source are the real upper bound of what Ranger can allow. (Trino DG ch. 10.)
4. Run institutions on shared Trino clusters, one zone each (pool), and give an institution its own cluster (silo) only when a written legal or contractual isolation requirement exists; record the choice in the institution's directory record (azure-style-rbac-abac.md › Best practices and limits, rule 4). Why: cluster separation is the strongest isolation Trino offers, while the zone already gives the institution boundary on a shared cluster. (Trino DG ch. 10; decision 2026-11-14, conflicts #14.)
5. Run ETL and interactive read-only analysis on separate Trino clusters, and grant no write access types on the analysis cluster: the compiler sends Data Contributor write access types only to the ETL cluster's Ranger service (`trino-etl`) and to Spark, and `trino-interactive` receives read access types only. Why: separating workloads by cluster isolates them at the largest scale, and read-only analysis needs no write path. (Trino DG ch. 10; trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #69.)

Example (rewritten, platform manifest):
```yaml
kind: Catalog
metadata: { id: t_acme_crm_ro }
spec:
  institution: t-acme
  zone: t-acme
  sourceUser: { name: svc-t-acme-crm-ro, secretRef: "openbao:kv/trino/t_acme_crm_ro", sourceRights: read-only }
  passThroughFunctions: none          # Enforcement points rule 7
  cluster: analysis                   # read-only cluster: no write access types
---
kind: Institution
metadata: { id: t-acme }
spec:
  clusterModel: pool                  # silo only with a written legal or contractual requirement (conflicts #14)
```

### Session properties and query management permissions

Source: Trino: The Definitive Guide, 2nd ed., ch. 8, 12 (merged 2026-11-16).

1. Allow session properties per role from an allow list compiled to Ranger session-property policies, and give analysts no catalog session property that exposes data, such as `unsupported_type_handling`. Why: a user-set property can change what a query returns, for example turning otherwise omitted columns into readable text. (Trino DG ch. 8.)
2. Show other users' queries (the query list, query details and the `system.runtime` tables) only to the platform operator group and the Auditor role; every other user, a Data Reader included, sees only their own queries. Why: the query list covers all users and is searchable by SQL text, and that text can hold data values (conflicts #30). (Trino DG ch. 8, 12.)
3. Allow killing another user's query only to the platform operator group, and audit every kill. Why: the kill procedure stops any query, and the web UI shows a killed query only as cancelled by the user, not who stopped it. (Trino DG ch. 8, 12.)
4. Use Trino resource groups only for capacity, one per institution, never as an access decision, and end the selector list with a catch-all selected by group only. Why: selectors match client-set source names and tags in order and the first match wins, so a client can choose its own resource group. (Trino DG ch. 12.)

Example (rewritten, CI decision-table cases):
```yaml
- { id: own-query,     principal: { user: alex, groups: [platform_t-acme_analyst] },  action: view-query, owner: alex, expect: allow }
- { id: other-query,   principal: { user: alex, groups: [platform_t-acme_analyst] },  action: view-query, owner: kim,  expect: deny }
- { id: auditor-view,  principal: { user: ayse, groups: [platform_t-acme_auditor] },  action: view-query, owner: kim,  expect: allow }
- { id: auditor-kill,  principal: { user: ayse, groups: [platform_t-acme_auditor] },  action: kill-query, owner: kim,  expect: deny }
- { id: operator-kill, principal: { user: ops1, groups: [platform_trino_operator] },   action: kill-query, owner: kim,  expect: allow }
```


### Spark path (Kyuubi AuthZ, hive-type service)

Source: platform decisions after a trial; Spark has no engine file and Trino is the closest (trial: ranger-policy-compile, 2026-11-16).

1. Name the Spark database of an Iceberg namespace `<catalog>_<schema>` (for example `t_acme_lake_gold_sales`), so that the zone resource `database: t_acme_lake_*` holds exactly one institution; treat the name as provisional until a lab test confirms how Kyuubi AuthZ reports the Iceberg catalog and namespace to Ranger. Why: the hive-type service has no catalog level, and a bare namespace such as `gold_risk` would collide across institutions. (trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #66.)
2. Compile every role assignment of an institution to its Spark service as well as to Trino, and put the Spark access types (`hive:select` and the hive mask types) into every tag mask and guardrail item. Why: masks and row filters must be identical on every client path, and a tag item without the Spark access type leaves Spark reads unmasked. (trial: ranger-policy-compile, 2026-11-16.)
3. Never compile a cross-institution share to Spark; shares are served only through Trino (azure-style-rbac-abac.md › Role assignments, rule 12). Why: a Spark job runs with its own institution's object-store credentials and cannot reach another institution's data. (trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #65.)

Example (rewritten, Ranger policy JSON for the Spark service):
```json
{
  "service": "spark-sql",
  "zoneName": "t-acme",
  "name": "t-acme-ra-0001-spark-sql",
  "resources": { "database": { "values": ["t_acme_lake_gold_sales"] }, "table": { "values": ["*"] }, "column": { "values": ["*"] } },
  "policyItems": [ { "groups": ["platform_t-acme_analyst"], "accesses": [ { "type": "select", "isAllowed": true } ] } ]
}
```

### Nessie: branch guard with CEL rules

Source: research: Project Nessie documentation, Authorization (https://projectnessie.org/nessie-latest/authorization/) and Configuration (https://projectnessie.org/nessie-latest/configuration/), Nessie 0.110.0, accessed 2026-11-16; Quarkus OIDC Bearer token authentication guide (https://quarkus.io/guides/security-oidc-bearer-token-authentication), accessed 2026-11-16. Nessie has no Ranger plugin and no engine file; Trino is the closest.

1. Use Nessie authorization only as a coarse guard over references (who may commit to `main`, create, reassign or delete branches); table and column access stays with Ranger in Trino and Spark. Why: Nessie rules see operations, references and content paths, not the platform's roles, scopes, tags and masks. (research: Nessie Authorization, 2026-11-16.)
2. Enable it with `nessie.server.authorization.enabled=true` and keep the rules (`nessie.server.authorization.rules.<id>`, CEL) in the platform folder; rules only allow, any matching rule allows, and everything no rule allows is denied. Always write an explicit `VIEW_REFERENCE` rule. Why: when no rule mentions `VIEW_REFERENCE`, Nessie adds one that lets everyone view every reference. (research: Nessie Authorization and source `CelBatchAccessChecker`, 2026-11-16.)
3. Match humans by group through `roles` (the Keycloak `groups` claim, as Quarkus maps it) and workload identities through `role` (the principal name); never list human user names. Why: humans get access only through groups, and Quarkus fills `role` from `upn`, `preferred_username` or `sub`. (research: Quarkus OIDC guide, 2026-11-16; conflicts #12.)
4. Before rollout, confirm in the lab which token Trino and Spark send to Nessie (the end user's or the engine's) and the principal name of a Keycloak service account. Why: the rules depend on both, and neither is documented. (research: Nessie Authorization, 2026-11-16, unconfirmed items.)

Example (rewritten, Nessie configuration; group and client names follow the platform naming table):
```properties
nessie.server.authorization.enabled=true
nessie.server.authorization.rules.view=op=='VIEW_REFERENCE' && 'platform_t-acme_engineer' in roles
nessie.server.authorization.rules.main_pipeline=op in ['VIEW_REFERENCE','READ_ENTRIES','READ_CONTENT_KEY','READ_ENTITY_VALUE','COMMIT_CHANGE_AGAINST_REFERENCE','CREATE_ENTITY','UPDATE_ENTITY','DELETE_ENTITY'] && ref=='main' && role=='wi-t-acme-orders-ingest'
nessie.server.authorization.rules.dev_branches=op in ['CREATE_REFERENCE','DELETE_REFERENCE','COMMIT_CHANGE_AGAINST_REFERENCE','CREATE_ENTITY','UPDATE_ENTITY'] && ref.startsWith('dev-') && 'platform_t-acme_engineer' in roles
```
