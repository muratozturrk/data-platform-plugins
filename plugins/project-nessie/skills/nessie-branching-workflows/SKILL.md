---
name: nessie-branching-workflows
description: Using Nessie branches and tags on the platform: branch-per-institution isolation, write-audit-publish (WAP) for pipelines, multi-table atomic releases, release tags for audited reports, backfills and experiments on branches, merge/transplant/conflict handling, rollback by reassigning references, and naming/retention rules for references. Use when designing pipeline promotion, release or recovery procedures.
---

# Branching Workflows

## Reference layout (platform)
| Reference | Purpose | Who commits | Lifetime |
|---|---|---|---|
| `main` | published, governed state | promotion job only | forever |
| `t-{institution}` | institution working branch (pool tier) | institution pipelines | forever |
| `wap-{pipeline}-{run_id}` | write-audit-publish for one run | that run | hours; deleted after merge |
| `backfill-{pipeline}-{date}` | historical reload | pipeline | until merged |
| `exp-{workspace}-{name}` | analyst experiments | workspace engineers | ≤ 30 days (GC) |
| `rel-{YYYYMMDD}` (tag) | audited snapshot behind a scheduled report / regulatory delivery | promotion job | retention per policy |
Naming per `naming-conventions`; the control plane creates/deletes references through the Nessie API (`svc-control-plane`).

## Write-Audit-Publish
1. Create `wap-…` from the institution branch (hash pinned).
2. Pipeline writes (Spark/Flink/Trino/dbt) commit only to the WAP branch — multiple tables in one commit where possible.
3. Audit: data-quality rules (GE/Soda/dbt tests/Deequ), Elementary checks and lineage registration run **against the WAP branch** (engines read `table@wap-…`).
4. Publish: merge WAP → institution branch (and institution → `main` on schedule) — atomic across all changed tables; on conflict (same key changed concurrently) retry with rebase/transplant or fail the run.
5. Delete the WAP branch; GC later removes unreachable files.
Consumers never see partial loads; failed runs leave `main` untouched.

## Releases and audit
`rel-{date}` tags mark the exact catalog state used by Superset scheduled reports and regulatory exports; the report archive object stores the tag + hash (Iceberg snapshot reference requirement in the technology list). Tags are immutable; deleting a tag requires platform-admin rights and an audit record.

## Recovery
- Bad publish: reassign the branch to the previous hash (`ASSIGN_REFERENCE_TO_HASH`) — instant rollback across all tables; data files remain until GC.
- Single table: Iceberg `rollback_to_snapshot` via Trino/Spark on the branch, then commit.
- Experiments: never merged; branch deleted at expiry.

## Multi-table transactions
Spark/Flink commit several tables in one Nessie commit (`CATALOG_UPDATE_MULTIPLE` via REST); Trino commits per table but a WAP branch merge publishes them atomically.

## Anti-patterns
- Long-lived feature branches that drift from `main` (merge conflicts, GC blocked)
- Reading `main` from pipelines that write to `main`
- Tags without a policy for deletion (unbounded storage)
- Using branches as per-environment separation

Sources: Nessie docs — Features (branches/tags, merge/transplant, conflicts), Spark SQL extensions syntax, Iceberg REST multi-table commits; *Apache Iceberg: The Definitive Guide* (WAP patterns); Dremio/Nessie "data as code" guides.
