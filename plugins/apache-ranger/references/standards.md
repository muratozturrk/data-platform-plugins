# Standards and regulations referenced

| Standard | Use in this plugin |
|---|---|
| NIST SP 800-162, *Guide to Attribute Based Access Control (ABAC) Definition and Considerations* | vocabulary (subject/object/environment attributes, PDP/PEP/PAP/PIP), tag-policy design |
| NIST SP 800-205, *Attribute Considerations for Access Control Systems* | attribute quality, sync and trust for user/tag attributes |
| NIST SP 800-53 Rev. 5 — AC (Access Control) and AU (Audit and Accountability) families | control mapping in `ranger-audit-compliance` |
| NIST SP 800-207, *Zero Trust Architecture* | policy decision/enforcement separation, continuous verification |
| OASIS XACML 3.0 | reference architecture terms; deny-overrides vs permit-overrides combining (compare with Ranger evaluation order) |
| ANSI INCITS 359 (RBAC) | role model baseline |
| ISO/IEC 27001:2022 Annex A 5.15–5.18, 8.2–8.5, 8.15–8.16; ISO/IEC 27002:2022 | access control and logging controls |
| ISO/IEC 27701 | privacy extension (purpose limitation evidence) |
| KVKK (Law 6698) art. 4, 12; KVKK Board *Kişisel Veri Güvenliği Rehberi (Teknik ve İdari Tedbirler)* | purpose limitation, access logging, masking as technical measures |
| GDPR art. 5, 25, 32 | privacy by design, security of processing (for EU-facing datasets) |
| CIS Benchmarks (Kubernetes, PostgreSQL) | hardening of the Ranger deployment substrate |
