# Sources

## Official documentation (superset.apache.org)
- Installation: Architecture (components, optional cache/worker/beat), Installation methods (Docker Compose vs Kubernetes vs PyPI; Kubernetes as best practice; SIP-149 operator), Kubernetes/Helm installation, Building your own production Docker image.
- Configuration: Configuring Superset (feature flags, caches, Celery, async queries, timeouts), Security (roles, permissions, RLS, OAuth examples incl. Keycloak with PKCE and group mapping), Alerts & Reports, Embedding Superset (embedded SDK, guest tokens, feature flags).
- `UPDATING.md` and `CHANGELOG/` in the apache/superset repository for each release; `RELEASING/` release notes.
- Developer Portal (6.1): Extensions framework architecture, frontend API reference, `apache-superset-extensions-cli`.

## Release information
- Preset, "Apache Superset 6.0 Release" (Dec 2025): Ant Design v5 design system, dark mode, token-based theming, embedded SDK improvements.
- Preset, "Apache Superset 6.1 Release" (May 2026): Extensions framework with stable `@apache-superset/core` APIs, MCP service, Global Task Framework (feature-flagged), Matrixify; PyPI `apache-superset` 6.1.0 released 13 May 2026.
- Preset monthly "Apache Superset Repo Recap" posts (MCP tool catalog growth, `MCP_DISABLED_TOOLS`, prompt-injection hardening, semantic layers as extensions).
- Preset, "Running Apache Superset on the Open Internet" (security hardening).

## Deployment references
- apache/superset Helm chart (`helm/superset`) values and README.
- Stackable Data Platform: Superset operator docs, `SupersetCluster` CRD, Keycloak/OPA end-to-end security demo. https://docs.stackable.tech/home/stable/superset/
- Galaxy, "Running Apache Superset in Kubernetes: A Complete Guide" (component overview, scaling notes).

## Books
- Shashank Shekhar. *Apache Superset Quick Start Guide*. Packt, 2018 (dated; architecture basics still valid).
- Joe Reis, Matt Housley. *Fundamentals of Data Engineering* (serving layer chapter).
