---
name: platform-as-product
description: Run the data platform as a product with users, a roadmap, golden paths and adoption metrics instead of a pile of infrastructure. Use when planning platform scope, prioritising features, defining paved roads, measuring platform success, or preparing to sell the platform externally.
---

# Platform as a Product

The single most important quality of a successful platform, according to the CNCF Platforms whitepaper and the O'Reilly *Platform Engineering* book, is that it is treated as a product: it has customers, a value proposition, a roadmap, support and metrics.

## When to Use This Skill

- Deciding what the platform should and should not do
- Designing the paved road / golden path for a common task
- Choosing success metrics for the platform
- Preparing an internal platform for external customers
- Explaining to stakeholders why platform work needs product management

## Core Concepts

### Users are customers
Data engineers, analysts, ML engineers and operators who use the platform are customers. Later, external companies are too. Treat friction as a defect, run feedback loops (office hours, surveys, internal NPS), and publish a roadmap.

### Thinnest Viable Platform (Team Topologies)
Build the smallest thing that reduces users' cognitive load. A wiki page can be a platform. Add thickness only when a real, repeated need appears. This is the antidote to speculative generality while building a generic product.

### Golden paths / paved roads
An opinionated, pre-built route for a common job: `platform init pipeline --template ingest-batch` yields a manifest, tests, CI, observability and docs pre-wired. Teams that follow it get best practices for free; deviation is allowed but unsupported. Golden-path adoption rate is a core product metric.

### Platform planes (Data Mesh)
Dehghani's multiplane model is a useful decomposition for a data platform product:
- **Infrastructure provisioning plane**: storage, compute, orchestration, secrets, networking
- **Data product developer experience plane**: declarative manifests, CLI/SDK, scaffolds, local dev, CI templates
- **Supervision / mesh plane**: catalog, lineage, policy enforcement, observability, cost
Expose the developer-experience plane as the product surface; hide the provisioning plane behind it.

### Maturity, not level-chasing
The CNCF Platform Engineering Maturity Model has five aspects (investment, adoption, interfaces, operations, measurement) across four levels. Use it to pick the next step; the model itself warns that pushing every aspect to the top is costly and sometimes harmful.

## Patterns

### Capability → feature definition
For every capability, write: user, job-to-be-done, feature name, in/out of scope, plan/tier, metering dimension, success metric, docs page. Capabilities without this are not shippable.

### Metrics that matter
- Adoption: % of pipelines on the golden path; active tenants/projects
- Time-to-value: time from tenant onboarding to first production pipeline
- Reliability per tenant: SLO attainment, incidents, MTTR
- Cost per tenant and per pipeline
- Developer satisfaction: internal NPS, support ticket volume and categories

### Internal-first productization
The internal data team is customer #1. Everything they need via a Slack message is a missing product feature. Onboarding, docs, upgrades and support must work without the platform team in the room before any external customer is considered.

### Say no, record why
Keep a "deliberately not built" list with the trigger that would change the decision. This protects TVP and makes roadmap conversations concrete.

## Anti-patterns
- Platform defined by the tools it runs instead of the jobs it does for users
- Features added because one loud tenant asked, without a second use case
- No owner for docs, onboarding or deprecation
- Measuring platform work by tickets closed instead of user outcomes

See `references/sources.md` for the source material behind this skill.
