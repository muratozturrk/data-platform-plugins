# Contract Change Management

How a contract changes safely: compatibility modes, which changes break, version bumps,
deprecation, and a CI gate. Most of the method is **general practice** (untagged). The parts about
what catalogs already do are **research-measured** or were checked for this skill against source
code (K).

## Compatibility modes (general practice)

These terms come from schema registries for event streams, and they apply to tables too. "Reader"
means a consumer (query, job, dashboard). "Writer" means the producer.

| Mode | Guarantee | Safe changes (record/table) | Upgrade order |
|---|---|---|---|
| `BACKWARD` | Readers on the **new** schema can read data written with the **previous** one | Remove a field; add an optional field with a default | Readers first |
| `FORWARD` | Readers on the **previous** schema can read data written with the **new** one | Add a field; remove an optional field | Writers first |
| `FULL` | Both directions | Add or remove optional fields only | Any order |
| `*_TRANSITIVE` | The same, checked against **all** earlier versions, not only the last one | As above | As above |
| `NONE` | No check | Anything | Coordinated release |

**For warehouse tables, state the mode explicitly.** Consumer SQL is written against the old
schema and reads new data. The guarantee that protects existing consumers is therefore
"old readers keep working", which is `FORWARD` in registry terms. Many teams loosely call this
"backward compatible". Put the mode name in the contract (`x-compatibility`) and define it
there, so nobody argues about the word.

## Change classification for tabular data (general practice)

| Change | Old consumer queries | Contract bump | Notes |
|---|---|---|---|
| Fix description, add a glossary link | unaffected | PATCH | |
| Add a nullable column at the end | work | MINOR | Breaks `SELECT *` loaded by position and `INSERT` without a column list. Announce it |
| Add a NOT NULL column without a default | work for readers | MINOR (readers) / MAJOR for writers that insert | |
| Widen a type (`int`→`bigint`, `varchar(36)`→`varchar(64)`, more decimal precision) | usually work | MINOR | Can overflow strictly typed consumers (e.g. 32-bit ints in code) |
| Add an enum value | work | MINOR + notice | Breaks exhaustive `CASE`/switch logic |
| Tighten a quality threshold or SLA (fresher, fewer nulls) | work | MINOR | |
| Remove a column | **break** | MAJOR | Deprecate first |
| Rename a column | **break** | MAJOR | Do it as add-new + copy + deprecate-old |
| Narrow a type, or change the type family (`string`→`int`) | **break** | MAJOR | |
| Optional → required for writers | work for readers | MAJOR for writers | |
| Required → optional (nulls may now appear) | **may break** | MAJOR | Readers did not expect nulls |
| Change meaning with the same type (unit EUR→USD, gross→net, new grain) | **silent break** | MAJOR | The most dangerous kind: no schema diff shows it |
| Change primary key or grain | **break** | MAJOR | |
| Loosen an SLA or quality threshold | work, but promise weaker | MAJOR | Consumers planned around the old promise |

Contract semver: `MAJOR.MINOR.PATCH` of the contract document. Consumers pin a **major**
version. Producers may ship minor and patch changes without consumer sign-off. A major change
needs a deprecation window, or sign-off from every pinned consumer.

## What the measured catalogs already classify (K)

The catalogs version metadata and emit change events (D1.4 and D1.6 = 3 in OpenMetadata and
DataHub). They can therefore *notice* a change. **None blocks one** (D9.3 < 4). Their
classifications also differ from the table above. Do not use them as your gate.

**OpenMetadata 1.13.3: entity version bump** (`openmetadata-service/.../jdbi3/EntityRepository.java`):
- Versions are decimals: a minor change adds 0.1 and a major change adds 1.0
  (`util/EntityUtil.java:787-793`). The choice is made in `updateVersion` (`:8963-8970`).
- **Major** when a column is deleted (`:9724`), or when `dataLength`, `precision` or `scale`
  shrinks or is cleared (`:9767-9806`). Columns are matched by name (case-insensitive), data
  type and array element type (`util/EntityUtil.java:138-142`). A type change therefore looks
  like delete + add, and is also major.
- Everything else, including **adding** a column, is minor.
- Contract schema validation is a separate check: only missing and duplicate fields fail it, and
  type mismatches do not (`DataContractRepository.java:456-458`).

**DataHub v1.7.0: schema change events**
(`metadata-io/.../timeline/eventgenerator/SchemaMetadataChangeEventGenerator.java`):
- Field removal → `MAJOR`, "A backwards incompatible change" (`:391`).
- Native type change → `MAJOR` (`:455`). Primary-key field added or removed → `MAJOR`
  (`:539`, `:564`).
- Field addition → `MINOR`, "A forwards & backwards compatible change" (`:422`).
- **Field rename → `MINOR`, "forwards & backwards compatible"** (`:471-485`). For SQL consumers a
  rename breaks every query. This is the clearest case where a product's label must not be your
  gate.
- Diffs are available through the Timeline API. Changes produce `EntityChangeEvent`s that can be
  routed to chat through Actions (D10.3 = 3).

**Apache Atlas 2.5.0:** a schema change is written to the audit log as `ENTITY_UPDATE`
(`intg/.../model/audit/EntityAuditEventV2.java:260-266`) and published on Kafka
(`EntityNotification.java:210-211` operation types). It has no diff, no rollback (D1.4 = 2) and no
classification.

**Change events (D1.6) for consumer notification:**

| Product | Mechanism (K) | Gaps |
|---|---|---|
| OpenMetadata | Documented `ChangeEvent` schema, webhook/Slack/Teams/GChat/e-mail subscriptions filtered by event type, offset-based at-least-once delivery, failed-event record, replay via `syncOffset`; `notifyDownstream` + `downstreamDepth` walk lineage to downstream owners (`events/eventSubscription.json:98-217`, `AbstractEventConsumer.java:144-199`) | Not CloudEvents; no Kafka publishing |
| DataHub | MCP/MCL/PlatformEvent on Kafka with a documented schema; Actions framework filters by event type, aspect and change type (`datahub-actions/.../filter/event_type_filter.py`) | No outgoing webhook; subscriptions are file-based, not managed through an API |
| Atlas | `EntityNotificationV2` on Kafka topic `ATLAS_ENTITIES` (`EntityNotification.java:100-213`) | No webhook; no per-subscription filter |

## Deprecation policy (general practice)

| Consumer tier | Minimum window from announcement to removal | Extra |
|---|---|---|
| External / regulatory reports | 180 days | Written sign-off |
| Cross-domain production consumers | 90 days | Sign-off or silence after two reminders |
| Same-domain consumers | 30 days | |
| Exploratory / sandbox | 7 days | |

Steps:
1. Mark the field `deprecated: true` with `removalDate` and a `replacement` in the contract
   (MINOR bump).
2. Notify registered consumers. Use lineage to find **unregistered** consumers as well: query
   logs, downstream tables, dashboards.
3. Watch usage of the deprecated field (D10.5 usage analytics, where available). Remind at
   halfway and one week before removal.
4. Remove the field in a MAJOR release. Keep the old major readable (a view or a versioned table
   `orders_daily_v2`) until the last pinned consumer moves.
5. Close: record the removal and who signed off. Version history keeps this record in OM and DH
   (D1.4 = 3).

## CI contract check outline (general practice)

The level-4 gate the products do not provide. It runs on every pull request that touches a
contract or a model or migration that produces the dataset.

```text
1. lint        validate contract YAML against the ODCS JSON Schema (+ local x- keys)
2. diff        old = contract at merge-base; new = contract in PR
               also: compiled model schema (dbt manifest / DDL) vs new contract schema
3. classify    apply the change table -> PATCH | MINOR | MAJOR (+ reasons)
4. version     declared version bump >= required bump? else FAIL
5. gate        if MAJOR:
                 require deprecation entries for removed/renamed fields with removalDate >= today + window
                 require approval from every consumer with pinnedMajor == old major (CODEOWNERS / review label)
               else PASS
6. generate    emit checks from required/unique/quality/sla -> GX suite / dbt tests / SQL
7. publish     on merge: PUT contract to catalog API, attach generated tests, post change event
```

Example job (GitHub Actions syntax; tool names are placeholders for your scripts):

```yaml
name: contract-check
on:
  pull_request:
    paths: ["contracts/**", "models/**", "migrations/**"]
jobs:
  contract:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }
      - name: Lint contracts
        run: python tools/contract_lint.py contracts/          # JSON Schema validation
      - name: Classify changes
        run: |
          python tools/contract_diff.py \
            --base "$(git merge-base origin/main HEAD)" --head HEAD \
            --out diff.json                                      # PATCH/MINOR/MAJOR per contract
      - name: Enforce version bump and deprecation window
        run: python tools/contract_gate.py diff.json --min-window-days 90
      - name: Require consumer approval for MAJOR
        run: python tools/require_consumer_approvals.py diff.json --pr ${{ github.event.number }}
      - name: Generate data tests from contract
        run: python tools/contract_to_tests.py contracts/ --target dbt
```

Core of `contract_diff.py` classification (sketch):

```python
BREAKING = {"removed", "renamed", "type_narrowed", "type_family_changed",
            "nullable_loosened", "pk_changed", "semantics_changed", "sla_loosened"}

def classify(old, new):
    reasons = []
    old_cols = {c["name"]: c for c in old["schema"][0]["properties"]}
    new_cols = {c["name"]: c for c in new["schema"][0]["properties"]}
    for name, oc in old_cols.items():
        nc = new_cols.get(name)
        if nc is None:
            reasons.append(("removed", name))
            continue
        if oc["logicalType"] != nc["logicalType"]:
            reasons.append(("type_family_changed", name))
        elif narrower(nc["physicalType"], oc["physicalType"]):
            reasons.append(("type_narrowed", name))
        if oc.get("required") and not nc.get("required"):
            reasons.append(("nullable_loosened", name))
        if oc.get("description") != nc.get("description") and nc.get("x-meaning-changed"):
            reasons.append(("semantics_changed", name))
    for name in new_cols.keys() - old_cols.keys():
        reasons.append(("added", name))
    level = "MAJOR" if any(r[0] in BREAKING for r in reasons) else ("MINOR" if reasons else "PATCH")
    return level, reasons
```

A meaning change cannot be detected by diffing. Require the author to set a flag (here
`x-meaning-changed`), and have reviewers check for it.

## Approval routing (links to D7.2)

- Minor and patch changes: producer team review only.
- Major changes: producer owner + data product owner + each pinned consumer. A domain data lead
  breaks ties.
- Where the catalog has a workflow engine (OpenMetadata D7.2 = 3, Flowable-based, with multi-step
  approvals and JsonLogic branching), it can mirror the approval for audit. Git review remains the
  gate. DataHub OSS has no pre-publication approval (D7.2 = 1), and Atlas has none (D7.2 = 0). See
  [data-products-and-ownership.md](data-products-and-ownership.md).
