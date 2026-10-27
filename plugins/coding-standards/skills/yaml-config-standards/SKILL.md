---
name: yaml-config-standards
description: Standards for YAML and configuration files: platform manifests, Kubernetes/Helm, Airflow and dbt YAML, CI workflows, Dockerfiles. Covers formatting (yamllint), structure, secrets, pinning, schema validation and environment layering. Use when writing or reviewing any config file.
---

# YAML and Configuration Standards

## Core Rules
| # | Rule | Enforced by |
|---|---|---|
| 1 | 2-space indent, no tabs, `---` document start, ≤ 120 chars, no trailing spaces, keys lowercase kebab or snake per the system's convention, document ends with newline | yamllint |
| 2 | Every config file validates against a schema: platform manifests → JSON Schema; K8s → kubeconform; Helm → `helm lint` + `ct lint`; dbt → `dbt parse`; GitHub Actions → actionlint | CI |
| 3 | Secrets never inline; references only (`secretRef`, external-secrets, Vault paths); `.env` files git-ignored | gitleaks, review |
| 4 | Pin everything: image tags by digest or exact version (never `latest`), Helm chart versions, action versions by SHA, Python/npm lockfiles | hadolint DL3007, review |
| 5 | Kubernetes: resource requests and limits on every container; liveness/readiness probes; `securityContext` non-root; labels per infra-naming | kube-score/polaris |
| 6 | Helm: values documented with comments and `values.schema.json`; no logic in templates beyond simple conditionals; one chart per component | helm lint |
| 7 | Environment differences via overlays (Kustomize) or values files (`values-{env}.yaml`), never copies of whole manifests; the base must be valid alone | review |
| 8 | Airflow YAML/DAG config and dbt YAML: descriptions on every model/column/DAG; owners set; tags from the naming scheme | dbt-checkpoint |
| 9 | Dockerfiles: multi-stage, non-root user, pinned base image, `COPY` specific paths, `.dockerignore`, no secrets in build args | hadolint |
| 10 | CI workflows: one reusable workflow per check type; jobs named by what they verify; caches keyed by lockfile hash | review |
| 11 | Comments explain non-obvious values (`# 30s: upstream p99 latency + margin`) | review |
| 12 | Anchors/aliases allowed within a file for repeated blocks; never across files | yamllint |

## Anti-patterns
- `latest` images, floating `@main` actions
- 500-line values.yaml with every option; per-env copies drifting apart
- Secrets committed "temporarily"
- Jinja-templated YAML that is not valid YAML before rendering (Helm excepted)

Sources: yamllint docs; Kubernetes configuration best practices; Helm chart best practices; hadolint rules; actionlint; Kustomize docs; dbt "How we style our YAML".
