---
name: superset-extensions-mcp
description: Extending and automating Superset 6.1+: the Extensions framework (@apache-superset/core stable APIs, apache-superset-extensions-cli, Developer Portal), the MCP service (separate process, JWT auth, scope-based access, MCP_DISABLED_TOOLS, prompt-injection hardening), Preset Agent Skills, and when to use REST API vs CLI vs MCP. Use when building a Superset extension, wiring AI agents to Superset, or automating asset operations.
---

# Extensions and MCP

## Extensions framework (6.1)
- Extensions contribute functionality through published, versioned APIs in `@apache-superset/core` (namespaces exposed on `window.superset`: dashboard, explore, dataset, sqlLab…) instead of forking; scaffold and build with `apache-superset-extensions-cli`; documentation on the Developer Portal with an architecture overview, frontend API reference and CLI reference.
- Semantic layers are delivered as extensions (2026); custom chart plugins still follow the ECharts-based plugin architecture.
- Platform use cases: tenant switcher, platform catalog links (OpenMetadata) in explore, custom SQL Lab actions, theme per tenant, embedding-aware UI tweaks. Keep extensions in their own repo, versioned against the Superset version, tested against each upgrade.

## MCP service (6.1)
- Separate optional process; JWT auth; field-level access derived from token scopes; tool catalog covers dashboards, charts, datasets, SQL Lab, saved queries, tags, users/roles (read tools), with `MCP_DISABLED_TOOLS` to suppress tools and a "Data Boundary" instruction treating tool output as untrusted (prompt-injection hardening).
- Run it only where the platform's AI assistant needs it; place it behind the platform's auth (Keycloak-issued JWT), per-tenant scopes, and audit every mutation; disable destructive tools by default.

## API vs CLI vs MCP
| Need | Use |
|---|---|
| CI/CD, bulk sync, GitOps | `superset-cli sync native` (CLI) |
| Control-plane automation, exact control | REST API (`/api/v1/...`) with OpenAPI client |
| Interactive agent help for analysts (create chart from prompt, inspect dashboards) | MCP service |
Preset Agent Skills (Apache-2.0, `preset-api-skills`, `preset-mcp-skills`, `preset-cli-skills`; install with `/plugin marketplace add preset-io/agent-skills`) encode the mechanics and safety boundaries for each path; this plugin encodes platform policy. Install both; do not duplicate API call sequences here.

## Rules
- Pin extension and SDK versions to the Superset minor; test on staging after every upgrade.
- MCP mutations gated by role scope and allow-list; log with tenant and principal.
- Extensions never read internal imports; CI fails on non-`@apache-superset/core` imports.

Sources: Preset "Apache Superset 6.1 Release"; Preset Repo Recaps (April–June 2026) on MCP tools, `MCP_DISABLED_TOOLS`, Data Boundary; apache/superset PRs on `window.superset` namespaces; Superset Developer Portal; preset-io/agent-skills README; SIP-187 (MCP) and SIP-214 (AI chatbot integration).
