# apache-superset

Running Apache Superset 6.x as the BI/serving layer of a multi-tenant, open-source data platform, and designing dashboards that people can read. v0.2 adds chart-API limits, chart selection, dashboard lifecycle, storytelling, colour accessibility, privacy compliance, an AI layer and Superset internals. v0.3 adds the testing layer (strategy, harness, upgrade regression) aligned with the `qa-test-strategy` plugin.

| Kind | Name | Purpose |
|---|---|---|
| Agent | `superset-platform-engineer` | Deployment (Stackable/Helm), Keycloak + roles, RLS, embedding, performance, upgrades, assets delivery |
| Agent | `superset-dashboard-designer` | Dashboards/charts per Few, Knaflic, Tufte + Superset native features |
| Agent | `superset-asset-reviewer` | PR review of exported asset YAML (security, tenancy, naming, performance, change safety) |
| Agent | `superset-compliance-reviewer` | Privacy and governance review of configs, assets and dashboards |
| Command | `/superset-tenant-onboard` | Everything the Tenant controller creates in Superset (pool/silo) |
| Command | `/superset-embed-plan` | Guest-token service, RLS injection, SDK integration, hardening, tests |
| Command | `/superset-dashboard-review` | Review against the thirteen mistakes + Superset checks; redesign layout |
| Command | `/superset-assets-sync` | Dashboards-as-code pipeline (repo, templating, CI sync, drift, rollback) |
| Command | `/superset-dashboard-lint` | Heuristic lint of an export bundle: Few's thirteen mistakes + declutter rules |
| Command | `/superset-dashboard-brief` | Dashboard brief: audience, decisions, KPIs, data, security, success measures |
| Command | `/superset-chart-pick` | Recommend viz_type and settings for a question, with what to avoid |
| Command | `/superset-test-plan` | Test list for a Superset change/feature/release with T-bi-* ids, tools, harness, schedule, acceptance criteria |
| Command | `/superset-assets-validate` | Static validation of an assets bundle (schema, UUID stability, credentials, RLS presence, naming) with CI exit code |
| Command | `/superset-upgrade-regression` | UPDATING.md triage, config/flag diff, DB-copy rehearsal, parity tiers, rollout/rollback, evidence |
| Skill | `superset-architecture-deployment` | Components, operator vs Helm, image, config essentials, scaling, upgrades |
| Skill | `superset-security-model` | FAB roles/permissions, DAR, dashboard RBAC, RLS, Keycloak OIDC mapping, hardening |
| Skill | `superset-multi-tenancy-embedding` | Pool/silo/bridge, embedded SDK + guest tokens, tenant-aware operations |
| Skill | `superset-dashboards-as-code` | Export/import, superset-cli sync native, templating, CI, drift |
| Skill | `superset-dataset-semantic-layer` | Datasets as contracts, metrics, dbt sync, Trino/ClickHouse/Iceberg specifics |
| Skill | `superset-performance-caching` | Cache layers, async, limits, warehouse protection, capacity, budgets |
| Skill | `superset-dashboard-design` | Information-design standard, chart selection, layout, filters, definition of done |
| Skill | `superset-extensions-mcp` | 6.1 Extensions framework, MCP service, API vs CLI vs MCP, Preset Agent Skills |
| Skill | `superset-chart-api-limits` | Verified viz_type facts, missing controls and workarounds, chart-data API quirks (verified on 4.1.2) |
| Skill | `superset-chart-selection` | Chart choice, scales and number formatting mapped to viz_types that exist |
| Skill | `superset-dashboard-lifecycle` | Brief, discovery, prototype, testing, adoption measurement, maintenance, scenario patterns |
| Skill | `superset-storytelling-declutter` | Knaflic's lessons, Big Idea, storyboard, declutter rules for Superset |
| Skill | `superset-color-accessibility` | Palettes, contrast, colour-blind safety, Superset schemes and label_colors |
| Skill | `superset-privacy-compliance` | Small-cell suppression, retention, masking-aware export, SQL Lab guardrails, audit logging |
| Skill | `superset-ai-layer` | Governed MCP gateway, NL2SQL with allow-listed semantic layer, query validation, evaluation |
| Skill | `superset-internals-contributing` | DAO/Command layering, superset-core, WebSocket server, Extensions CLI, CI/test stack |
| Skill | `superset-testing-strategy` | Superset-as-component pyramid: static, unit, contract (OpenAPI/schemathesis, chart-data), integration (RLS/mask consistency across JSON/CSV/embedded, guest-token scope, assets idempotency, Alerts & Reports, cache), E2E Playwright, k6 performance, visual regression, security headers, upgrade; T-bi-* mapping |
| Skill | `superset-test-harness` | Disposable light/full Superset stacks (compose/Testcontainers: PostgreSQL, Redis, Trino, Keycloak, Mailpit), test config, bootstrap, two-institution seeding, API client fixtures, reset/teardown |
| Skill | `superset-upgrade-regression` | Upgrade triage, metadata-DB rehearsal, assets re-export diff, SDK/extension compatibility, parity tiers, rollout and evidence |

`examples/` live inside the skill that uses them:
- `superset-architecture-deployment`: production `superset_config.py`, Stackable `SupersetCluster` CR, Helm values
- `superset-multi-tenancy-embedding`: guest-token service, embed SDK snippet, two-tenant isolation test
- `superset-dashboards-as-code`: templated assets (database/dataset/RLS/roles), CI workflow
- `superset-dashboard-design`: review checklist, `dashboard_lint.py` (offline export lint)
- `superset-storytelling-declutter`: `declutter_lint.py`
- `superset-testing-strategy`: `assets_validate.py`, API contract / RLS-mask consistency / guest-token scope / security-header pytest modules, `k6_dashboard_load.js`, Playwright journey and visual-regression specs, `superset_test_inventory.yaml`
- `superset-test-harness`: `docker-compose.test.yml`, `superset_config_test.py`, `conftest.py` (compose lifecycle, API client, seeding, guest tokens), `seed_institutions.sh`, idempotent-import / Alerts & Reports / cache-isolation tests
- `superset-upgrade-regression`: `upgrade_check.py`, upgrade evidence template
- `superset-chart-api-limits`: `superset_client.py`, `viz_registry.py`, `warehouse.py` from the lab base
- `superset-chart-selection`: `rules.py`, `chartdata.py`
- `superset-color-accessibility`: `checks.py`, `superset_schemes.json`

## Version note
Facts labelled "verified on 4.1.2" come from lab runs against Superset 4.1.2; 6.x may differ. Re-check with `registered_viz_keys` and the lab client before relying on them.

## Companion
Install Preset's open-source Agent Skills for API/MCP/CLI mechanics (`/plugin marketplace add preset-io/agent-skills`); this plugin carries platform policy and design standards, not call sequences.

## Sources
- Superset documentation: architecture, installation methods, Kubernetes/Helm, configuration, security (incl. Keycloak PKCE + group mapping example), RLS, embedding and embedded SDK, caching/async, alerts & reports; `UPDATING.md`/`CHANGELOG`; Developer Portal (6.1)
- Preset: 6.0 and 6.1 release posts; monthly Repo Recaps (MCP, extensions); "Managing Apache Superset Assets as Code", "Creating Custom Superset Assets Programmatically"; "Running Superset on the Open Internet"; Preset Agent Skills (preset-io/agent-skills); preset-cli / superset-cli docs
- Stackable Superset operator docs; apache/superset Helm chart; Galaxy "Running Superset in Kubernetes"
- Few, *Information Dashboard Design* 2e and *Show Me the Numbers*; Knaflic, *Storytelling with Data* and *Let's Practice!*; Tufte, *The Visual Display of Quantitative Information*; Cairo, *The Functional Art* / *The Truthful Art*; Wong, *WSJ Guide to Information Graphics*; Wexler et al., *The Big Book of Dashboards* and *Dashboards That Deliver*; Kirk, *Data Visualisation*; Desbarats, *Practical Charts*; Cleveland & McGill (1984)
- Xebia, "Simple embedded analytics in practice" (RLS per client); AWS SaaS Lens isolation models
- Shekhar, *Apache Superset Quick Start Guide* (Packt, 2018; dated)
Per-skill `references/sources.md` files list exact items.
