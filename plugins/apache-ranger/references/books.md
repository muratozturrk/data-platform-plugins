# Books behind this plugin

There is no current book dedicated to Apache Ranger. The list below covers (A) the Hadoop-era security books that describe Ranger's lineage and concepts, (B) access-control theory that Ranger's tag/attribute model implements, (C) engine-specific security chapters, (D) governance context. Chapter summaries are kept outside the plugin, in the workspace (`summaries/<book-slug>/NN_CHAPTER_N_TITLE.md`); the rules adopted from them are merged into `skills/*/references/*.md`, each section naming its source.

## 0. Model source
0. Microsoft Learn — Azure RBAC and Azure ABAC documentation (overview, role definitions, scope, role assignments, deny assignments, conditions, best practices, PIM, access reviews). The platform's authorization model follows these concepts. **Priority: highest.**

0a. Apache Ranger wiki (cwiki.apache.org, space RANGER) — the project's own pages on deny conditions, tag-based policies, row filtering and masking, security zones, audit filters, usersync, REST APIs, hardening and release notes 2.5–2.9. Many pages date from Ranger 0.5–1.x; concepts are authoritative, version details only when the 2.x release notes agree. **Priority: highest for Ranger behaviour.**

## A. Big-data security (Ranger lineage)
1. Ben Spivey, Joey Echeverria. *Hadoop Security: Protecting Your Big Data Platform*. O'Reilly, 2015 (ISBN 978-1491900987). Ch. 6 Authorization, auditing/accounting chapters, case studies, afterword on unified authorization. Sentry-centric (Cloudera authors) but the best conceptual treatment; read for authorization patterns, not product steps. **Priority: high (ch. 2, 5–8, afterword).**
2. Piethein Strengholt. *Data Management at Scale*, 2nd ed. O'Reilly, 2023 (ISBN 978-1098138868). Domain-oriented ownership, classification, unified data security reference architecture, security process flow, metadata-driven security. Replaces Lakhe, *Practical Hadoop Security* (Apress, 2014): the platform uses Iceberg, not Hadoop. **Priority: medium-high (ch. 8 full; 2, 3, 4, 9 short).**
3. Sudheesh Narayan. *Securing Hadoop*. Packt, 2013. Short, dated. **Priority: low (skip unless available).**
4. Anurag Shrivastava, Tanmay Deshpande. *Hadoop Blueprints*. Packt, 2016 — ch. 7 builds a data lake with Ranger 0.5 (policies, audit). **Priority: low (one chapter).**

## B. Access-control theory
5. Vincent C. Hu, David F. Ferraiolo, Ramaswamy Chandramouli, D. Richard Kuhn. *Attribute-Based Access Control*. Artech House, 2017 (ISBN 978-1630811341). ABAC models, policy languages, deployment considerations — the theory behind tag-based policies. **Priority: high.**
6. Phil Windley. *Authorization in Action: Build policy-based access control for Zero Trust, APIs, SaaS, and AI agents*. Manning, 2026 (ISBN 978-1633435179). PARC model, RBAC/ABAC/ReBAC unified as policy-based access control, policy as code, authorization context, policy governance. Replaces Ferraiolo, Kuhn, Chandramouli, *Role-Based Access Control*, 2nd ed. (Artech House, 2007), which has no newer edition. **Priority: high.**

## C. Engine security chapters
7. Matt Fuller, Manfred Moser, Martin Traverso. *Trino: The Definitive Guide*, 2nd ed. O'Reilly, 2022 — security chapter (authentication, authorization, system access control). **Priority: high (already on your Trino list).**
8. Gwen Shapira, Todd Palino, Rajini Sivaram, Krit Petty. *Kafka: The Definitive Guide*, 2nd ed. O'Reilly, 2021 — ch. 11 Securing Kafka (authentication, Authorizer, ACLs, auditing, principals). **Priority: high (for the Kafka plugin).**

## D. Governance and privacy context
9. Evren Eryurek, Uri Gilad, Valliappa Lakshmanan, Anita Kibunguchy-Grant, Jessi Ashdown. *Data Governance: The Definitive Guide*. O'Reilly, 2021. Policies, classification, access management, auditing, people/process. **Priority: medium-high (classification → policy chapters).**
10. Zhamak Dehghani. *Data Mesh*. O'Reilly, 2022 — federated computational governance, policies as code. **Priority: medium (governance chapters only).**
11. Razi Rais, Christina Morillo, Evan Gilman, Doug Barth. *Zero Trust Networks*, 2nd ed. O'Reilly, 2024 — policy engine/enforcement separation, trust signals. **Priority: low-medium.**

## Free standards worth summarising like books
NIST SP 800-162 (ABAC guide, ~50 pages), NIST SP 800-205, KVKK *Kişisel Veri Güvenliği Rehberi* — see `standards.md`.
