---
name: keycloak-identity-federation
description: Bringing institution identities into Keycloak for the HKM platform - LDAP/Active Directory user federation versus OIDC/SAML identity brokering per institution, organization domains and identity-first routing, first-broker-login and account linking (trust email, verified email), IdP mappers to groups and attributes, the declarative user profile, SCIM provisioning, Workflows for lifecycle, joiner/mover/leaver handling, and optional e-Devlet or social login for public users. Use when connecting an institution's directory or IdP, designing user attributes, or handling deprovisioning.
---

# Identity Federation and Provisioning

## Choose per institution
| Institution has | Use | Keycloak object | Notes |
|---|---|---|---|
| Its own OIDC/SAML IdP (AD FS, Entra ID, Keycloak, Shibboleth) | **Identity brokering** | OIDC/SAML identity provider linked to the organization, domain `acme.gov.tr`, redirect when email matches | Passwords and MFA stay at the institution; Keycloak gets assertions. Best default. |
| Only LDAP/AD reachable from the platform | **User federation (LDAP)**, as a transition only (HKM D27) | LDAP provider, `READ_ONLY` edit mode, LDAPS, group mapper | Keycloak sees passwords (binds); sync users/groups periodically; LDAP password policy support since 26.6 |
| Nothing usable | **Local accounts** | Organization invitations: the user opens the link and creates the account (HKM D61); required actions (password, WebAuthn) | Institution admin manages group membership via FGAP v2 and never creates accounts |

Rules of thumb: prefer brokering over LDAP; never mix both for the same users; one IdP per institution, linked to its organization; shared IdPs across organizations are possible since 26.8 but make membership ambiguous — avoid unless a ministry-level IdP serves several institutions.

## First login and linking
- First-broker-login flow: create the user when new; **link by email only if the IdP verifies email and the organization domain matches**, and to an existing local account only after re-authentication (HKM D18; 26.8.2 hardening: enable *Trust Email* only for IdPs that verify; CVE-2026-97175). Otherwise require review.
- Organization membership: managed (via IdP) or unmanaged; since 26.8 new IdP links default to *Unmanaged*. Domain-based membership requires email verification since 26.8.2.
- IdP mappers: map the institution's groups/roles to `platform_{tenant}_{role}` only through an allow-list (Advanced Claim to Group / hardcoded group per IdP). Since 26.8.0 IdP mappers cannot grant admin roles unless explicitly enabled — keep it that way.
- Sync mode: `FORCE` for attributes that the institution owns (name, email, department) so movers are updated at next login; group mappings re-evaluated at each login.

## User profile (declarative)
Define exactly the attributes the platform needs, with who may view/edit them:
| Attribute | Source | Editable by user | Used by |
|---|---|---|---|
| `username` (= `preferred_username`) | IdP/LDAP (`sAMAccountName`, `upn` local part) | no (edit username disabled) | every engine principal, Ranger user |
| `email`, `firstName`, `lastName` | IdP | no | UI, notifications |
| `institution` | organization alias | no (admin only) | claims, audit |
| `department`, `title` | IdP | no | optional ABAC in Ranger |
Do **not** store TC Kimlik No, phone or address in the workforce realm unless a documented purpose exists (KVKK minimisation); for `hkm-public`, keep registration fields minimal.

## Provisioning and lifecycle
- **Joiner**: account appears at first brokered login (JIT) or via SCIM (SCIM API supported in 26.8) when the institution pushes users in advance.
- **Mover**: IdP group change → mapper updates groups at next login; for immediate effect revoke sessions (Admin API) and let Ranger usersync pick up memberships.
- **Leaver**: institution disables account → brokered login fails; also disable the Keycloak user (SCIM or workflow) and revoke sessions/offline tokens; Ranger reconciliation removes groups. Dormant accounts are handled by a Keycloak **Workflow** (supported since 26.6) that first notifies, then disables (HKM D22) — BG Rehberi 3.1.12.10 / 3.2.1.12; personal attributes are deleted when retention ends. A deleted username is never reissued (HMAC tombstone, HKM D24).
- Public users (`hkm-public`): self-registration with email verification, e-Devlet brokering only via the official integration agreement (check the current e-Devlet Kapısı integration terms before design); social login is off in both realms (HKM D35).

## Anti-patterns
- Linking accounts by unverified email.
- Writable LDAP federation from Keycloak.
- Letting institution IdP claims assign platform admin groups.
- Keeping leavers enabled because "the IdP will reject them" — offline tokens and service sessions outlive the IdP check.

## Open owner decisions
Still undecided: the inactivity threshold, temporary-account renewal, whether disabled accounts keep group memberships, the organization for vendor and auditor accounts, and the KVKK basis for registration data. When a request depends on one of them, say so, ask whether it has been decided since, and point to `../../references/open-decisions.md`. Give options, never a value presented as decided.

## Examples and references
- `examples/terraform/modules/institution/` (organization, IdP, mappers, groups)
- `references/federation-and-provisioning.md` — merged rules: federation vs brokering vs local accounts, SAML/OIDC brokering, account linking, user attributes, provisioning/SCIM/deprovisioning, legacy migration.
