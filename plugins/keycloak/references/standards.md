# Standards

| Standard | Status (2026-11) | What the plugin takes from it |
|---|---|---|
| OpenID Connect Core 1.0, Discovery, RP-Initiated / Back-Channel / Front-Channel Logout | final | login, ID token validation, logout propagation |
| RFC 6749 / 6750 OAuth 2.0, RFC 7636 PKCE | final | grants, bearer usage, PKCE S256 everywhere |
| [RFC 9700 – OAuth 2.0 Security Best Current Practice (BCP 240, Jan 2025)](https://www.rfc-editor.org/rfc/rfc9700.html) | final | no implicit/password grants, exact redirect matching, refresh-token protection, sender-constraining, mix-up defence |
| [OAuth 2.1 (draft-ietf-oauth-v2-1-16, Sep 2026)](https://www.ietf.org/archive/id/draft-ietf-oauth-v2-1-16.html) | Internet-Draft | direction of travel; not normative |
| RFC 8693 Token Exchange | final | Keycloak standard token exchange (26.2) for down-scoping |
| RFC 7523 JWT bearer grant / client authentication | final | JWT authorization grant (26.6), `private_key_jwt` |
| RFC 9449 DPoP | final | sender-constrained tokens (Keycloak 26.4) |
| RFC 9126 PAR, RFC 9101 JAR | final | FAPI-style clients, ASVS L3 |
| RFC 9068 JWT access-token profile, RFC 7662 introspection | final | resource-server validation |
| [FAPI 2.0 Security Profile (final, Feb 2025)](https://openid.net/specs/fapi-security-profile-2_0-final.html) | final | external API consumers only |
| SCIM 2.0 (RFC 7643/7644) | final | Keycloak SCIM API (supported 26.8) for institution provisioning |
| W3C WebAuthn Level 2/3, FIDO2 | final / CR | passkeys |
| [NIST SP 800-63-4 (B: authentication, C: federation)](https://pages.nist.gov/800-63-4/sp800-63b/aal/) | final | AAL definitions, phishing resistance, reauthentication limits (AAL2: overall ≤ 24 h, idle ≤ 1 h; AAL3: overall ≤ 12 h, idle ≤ 15 min), password rules |
| NIST SP 800-207 / SP 1800-35 Zero Trust | final | identity as the policy anchor |
| [OWASP ASVS 5.0 (2025)](https://github.com/OWASP/ASVS) – V6 Authentication, V7 Session Management, V8 Authorization, V9 Self-contained Tokens, V10 OAuth and OIDC | final | review checklist (`/keycloak-config-review`) |
| OWASP Cheat Sheets: Authentication, Session Management, OAuth 2.0 | living | patterns |
| [CISA/NSA ESF – IAM Recommended Best Practices for Administrators (2023)](https://www.cisa.gov/sites/default/files/2023-12/ESF%20IDENTITY%20AND%20ACCESS%20MANAGEMENT%20RECOMMENDED%20BEST%20PRACTICES%20FOR%20ADMINISTRATORS%20PP-23-0248_508C.pdf) | guidance | admin hardening, identity governance |
| ISO/IEC 27001:2022 Annex A 5.15–5.18, 8.2, 8.5 | final | access control, identity management, authentication information, access rights, privileged access, secure authentication |
| [Bilgi ve İletişim Güvenliği Rehberi 1.1 (Mart 2026)](https://cdn.siberguvenlik.gov.tr/public/docs/bg_rehber.pdf) | binding for public institutions | 3.1.8 log records, 3.1.12 authentication and access management, 3.2.1 authentication, 3.2.2 session management, 3.2.3 authorization |
| KVKK and *Kişisel Veri Güvenliği Rehberi (Teknik ve İdari Tedbirler)* | binding | minimisation, access logs, authorization matrix, retention |
