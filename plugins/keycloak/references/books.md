# Books and documents behind this plugin

Chapter summaries are kept outside the plugin, in the workspace (`summaries/<source>/NN_CHAPTER_N_TITLE.md`); the rules adopted from them are merged into `skills/*/references/*.md`, each rule naming its source. Which chapters are read is defined in the workspace's `summaries/_template.md`, section 7.

## Priority: highest
0. Keycloak official documentation (Server Administration Guide, Securing Applications, Server/Operator/High Availability guides, Upgrading guide), version 26.8. Version-specific facts come from here.
1. Stian Thorgersen, Pedro Igor Silva. *Keycloak – Identity and Access Management for Modern Applications*, 2nd ed. Packt, 2023 (ISBN 9781804616444). Written by Keycloak maintainers; examples target Keycloak 22 — concepts current, screens and some defaults dated. Chapters selected from the user's table of contents: 4–6, 8–12 and 14 full (OIDC, OAuth, application types, authorization strategies, production configuration, users and federation, authentication, tokens and sessions, hardening); 3, 7 and 13 short; 1–2 not read.
2. Gary Archer, Judith Kahrer, Michał Trojanowski. *Cloud Native Data Security with OAuth: A Scalable Zero Trust Architecture*. O'Reilly, 2025 (ISBN 9781098164874). All 14 chapters selected from the user's table of contents: 3–7, 9–11, 13–14 full (architecture, data design, API validation, token design and hardening, entitlements, workload identities, operating the authorization server, browser apps, user authentication); 1–2, 8, 12 short. Examples use Curity and other products; principles are mapped to Keycloak 26.8.
3. Standards pack: RFC 9700, NIST SP 800-63B-4 and 63C-4, OWASP ASVS 5.0 chapters V6–V10.

## Priority: high
4. Yvonne Wilson, Abhishek Hingnikar. *Solving Identity Management in Modern Applications: Demystifying OAuth 2, OpenID Connect, and SAML 2*, 2nd ed. Apress, 2022 (ISBN 9781484282601). Lifecycle view: provisioning, SSO, sessions, logout, deprovisioning, compliance, failures. Chapters selected from the user's table of contents: 4, 8, 9, 12–15, 17 and 18 full (provisioning, authorization levels, sessions, stronger authentication, logout, account management, deprovisioning, exceptions, less common requirements); 1, 2, 5–7, 10, 11, 16, 19 and 20 short; 3, 21, 22 and the appendices not read.
5. Neil Madden. *API Security in Action*. Manning, 2020 (only edition; ISBN 9781617296024). Chapters selected from the user's table of contents: 3–5 short (controls, session cookies, token storage), 6–8 and 11 full (JWTs, OAuth2/OIDC, identity-based access control, service-to-service), 9–10 short (capabilities, Kubernetes); 1–2 and IoT chapters 12–13 not read. OAuth advice predates RFC 9700.
6. Bilgi ve İletişim Güvenliği Rehberi 1.1 (identity sections) and KVKK guide (Turkish compliance).

## Priority: medium
7. Morey J. Haber, Darran Rolls. *Identity Attack Vectors*, 2nd ed. Apress, 2024 (ISBN 9798868802324). Identity threats, PAM, ITDR, JIT access, governance. Chapters selected from the user's table of contents: 6, 8, 10, 13 and 19 full (enterprise identity and account types, PAM, indicators of compromise, six steps, zero trust for identity); 5, 7, 9, 11, 12, 14–16, 18, 20, 21 and appendix A short; introductions, transformation, takeaways, vendor chapters and appendix B not read.
8. Justin Richer, Antonio Sanso. *OAuth 2 in Action*. Manning, 2017. Protocol mechanics and vulnerabilities. A second edition (Richer, Lombardo, Sanso; ISBN 9781633433083) is in MEAP since October 2026, estimated spring 2027 — prefer the MEAP chapters on OAuth 2.1, DPoP and PKCE if available. Chapters selected from the user's copy (PDF bookmarks): 7–10 and 13 full (client, protected resource, authorization server and token vulnerabilities; OAuth is not authentication); 2, 6, 11, 12, 14 and 15 short; code-lab chapters 3–5, 1, 16 and the appendices not read.
9. Phillip J. Windley. *Learning Digital Identity*. O'Reilly, 2023 (ISBN 9781098117689). Identity architectures, federation, trust, privacy, policies. Chapters selected from the user's table of contents: 8, 11, 12, 13 and 21 full (privacy, authentication, access control, federation, identity policies); 4, 6, 7, 9, 10, 14, 16 and 22 short; self-sovereign identity chapters (15, 17–20, 23) and 1–3, 5 not read.

## Priority: low-medium
10. Razi Rais, Christina Morillo, Evan Gilman, Doug Barth. *Zero Trust Networks*, 2nd ed. O'Reilly, 2024. Trusting identities, authorization decisions. Chapters selected from the user's table of contents: 4, 6 and 10 full (authorization decisions, trusting identities, adversarial view); 2, 5, 7, 8, 9 and 11 short; 1, 3, 12 and the appendix not read.

## Considered and not selected
- Prabath Siriwardena, *OpenID Connect in Action* (Manning): announced, MEAP since 2020, no final publication found.
- Aaron Parecki, *OAuth 2.0 Simplified* (2nd ed., 2018): good primer, superseded by RFC 9700 for security guidance; read online instead.
- *Identity-Native Infrastructure Access Management* (O'Reilly, 2023), *Identity Security for Software Development* (O'Reilly, 2025): adjacent topics (infrastructure access, developer identity), low yield for this plugin.
- No other Keycloak-specific book newer than Thorgersen & Silva 2nd ed. was found.
