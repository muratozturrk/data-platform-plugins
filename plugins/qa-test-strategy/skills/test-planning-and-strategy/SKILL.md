---
name: test-planning-and-strategy
description: Writing test strategies and test plans for the platform: strategy (organization, test types in scope per product risk, design/execution approaches, automation policy, environments, tools, coverage targets) versus plan (scope, project impact, quality and acceptance criteria, risk management, team, schedule, deliverables, entry/exit criteria). Use when producing or reviewing these documents.
---

# Test Planning and Strategy

## Strategy vs plan
- **Test strategy** (stable, per platform/component): how the target-of-test will be tested — how the team organizes testing, which testing types are in scope to address product risks, which design and execution approaches apply to each type.
- **Test plan** (per release/project): complete planning document with scope, approach, schedule and resources; must include project-specific impact on testing, scope, quality and acceptance criteria, risk management, team/schedule/deliverables.

## Strategy content for the platform
1. **Organization**: developers own unit/contract/component tests and PR gates; QA owns integration/system/e2e/non-functional suites, test design review, RTM, reports; platform ops own infra/DR tests; data owners own data-quality rules per domain.
2. **Risk → type matrix**: for each quality attribute list top product risks and the test types that address them (e.g. security → isolation tests, OPA policy tests, secret scanning, pen test; performance → Trino/ClickHouse load tests, pipeline throughput tests; reliability → chaos/failure injection, FTE retry tests; availability → HA failover tests, DR drill).
3. **Approach per type**: white box for compiler/reconcilers and Spark transforms; grey box for pipelines (DB/catalog access + design docs); black box for APIs/UI/BI; manual exploratory for new UI and onboarding flows; automated for everything repetitive or large-scale.
4. **Test pyramid(s)** (how-to per level in `unit-testing-data-platform`, `integration-testing-data-platform`, `e2e-testing-data-platform`): code pyramid (unit → contract/component → integration → system/e2e → acceptance) and data pyramid (schema/contract → row-level rules → aggregate reconciliation → cross-system consistency → business acceptance).
5. **Automation policy**: always automate unit, contract, data-quality, API, regression, security policy, manifest validation; UI automation for critical journeys only; performance tests automated with thresholds; manual: exploratory, usability, accessibility, DR drill with runbook.
6. **Environments**: ephemeral per PR (control plane + lightweight engines), staging (prod-like, all engines, two tenants), prod (synthetic canaries only).
7. **Tooling**: pytest/chispa, dbt tests, Great Expectations/Soda, Trino assertions, k6/JMeter, Playwright, kubeconform/conftest/OPA tests, Testcontainers (Kafka/Postgres/Keycloak), Litmus/chaos-mesh, OpenTelemetry-based checks; test management: Jira + Zephyr/Xray, TestRail or Azure DevOps (or repo-based YAML + RTM generated in CI).
8. **Coverage targets and metrics**: requirement coverage 100% for Critical/High; code coverage ≥ 85% core/sdk; data-quality rule coverage per gold table; regression pass rate ≥ 98%; defect escape rate trend.
9. **Governance**: case review before activation, monthly inspection, deprecation policy, upgrade regression policy for engines.

## Plan content (template in examples)
Project impact → scope in/out → criteria per attribute → risks with mitigations → types/suites from inventory → environments/tools (incl. new ones) → team and responsibilities → schedule by STLC phase → deliverables (cases, RTM, reports, automation) → entry/exit criteria → metrics → approvals.

## Anti-patterns
- Plan that restates the strategy; strategy that lists dates
- Test types chosen by habit, not by risk
- Acceptance criteria that are not measurable
- No owner for non-functional testing
