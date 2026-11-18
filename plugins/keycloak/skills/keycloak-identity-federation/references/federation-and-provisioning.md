# Federation and provisioning

Status: merged from Keycloak docs 26.8, Keycloak – IAM for Modern Applications, Identity standards (NIST SP 800-63C-4, OWASP ASVS 5.0) and Cloud Native Data Security with OAuth (2026-11-17); Solving Identity Management, API Security in Action, Türkiye mevzuatı (BG Rehberi), Identity Attack Vectors, Learning Digital Identity and Zero Trust Networks (2026-11-18); no pending sections.

## Starter rules

1. Broker the institution's own OIDC or SAML IdP for its staff as the one authoritative source per institution, never personal or social accounts, and use read-only LDAP federation only as a transition for institutions without one. Why: credentials and MFA stay with the account owner, Keycloak never sees passwords, the institution can end access in one place when someone leaves, and a second user store per institution multiplies passwords, policies and errors. (plugin v1; Keycloak docs 26.8, p. 3, p. 8; Keycloak IAM ch. 10; Solving IdM ch. 4; BG Rehberi 3.2.1.13; Identity Attack Vectors ch. 10, 13; Learning Digital Identity ch. 13; HKM D27, D35)
2. Link a brokered identity to an existing account only after the user proves ownership: by email only when the institution IdP verifies addresses and sends `email_verified` and the organization domain matches, and to an existing local account only after re-authentication; confirm with the user before creating a new account. Why: unverified linking enables account takeover and silent duplicates, trusted email marks addresses verified (again at every login with sync mode FORCE), and from 26.8.2 auto-linking requires a trusted email. (research: Keycloak 26.8.2 release notes, CVE-2026-97175; Keycloak docs 26.8, p. 8, p. 17; NIST SP 800-63C-4 §3.8; Cloud Native OAuth ch. 14; Solving IdM ch. 10, 18; Learning Digital Identity ch. 13; HKM D18)
3. Map institution groups to platform groups in Keycloak IdP mappers through an explicit allow-list, never inside clients, import through IdP mappers only the attributes for which the institution is authoritative (as listed in its trust agreement), and review every IdP mapper that assigns roles or groups as a privilege change. Why: an IdP must not grant platform roles it does not own, a valid token says nothing about how the institution collected a value it does not own, roles granted through IdP mappers count as direct role mappings, and transformation stays central. (research: Keycloak 26.8.0 upgrade notes on IdP mappers and admin roles; Keycloak docs 26.8, p. 8; Cloud Native OAuth ch. 14; Learning Digital Identity ch. 7)
4. Disable automatically every account that is unused for a set period, belongs to someone who has left, or is no longer tied to a person or business process, triggered by the authoritative source (institution IdP, HR system or SCIM `active=false`); report such accounts, and remove their sessions, offline tokens, consents and pending codes; run a Workflow on `user-authenticated` that notifies the user first and then disables, so that just-in-time accounts with no termination signal can still be reactivated. Why: offline tokens and sessions survive the upstream account, orphaned accounts hold personal data and invite takeover, deprovisioning without automation is forgotten, and each login restarts the inactivity clock so only dormant accounts close. (BG Rehberi 3.1.12.8, 3.1.12.10, 3.2.1.12; Keycloak docs 26.8, p. 10; NIST SP 800-63C-4 §4.6.7; OWASP ASVS 5.0, 7.4.2; Cloud Native OAuth ch. 14; Solving IdM ch. 11, 15; Identity Attack Vectors ch. 11, 13; Learning Digital Identity ch. 6; Zero Trust Networks ch. 4; HKM D22, D43)
5. Keep Keycloak as the single store of identity personal data: define only the default profile attributes plus those a documented purpose needs, release to each client only the attributes listed for it, keep any application copies read-only, and keep activity data such as login locations out of the user profile, held only in the event store. Why: KVKK data minimisation, release must be minimal and visible, Keycloak already stores device data (IP, OS, browser) for audit, and a central store of rich user data is a privacy and attack risk. (research: KVKK Kişisel Veri Güvenliği Rehberi; Keycloak docs 26.8, p. 4; Keycloak IAM ch. 10, 14; NIST SP 800-63C-4 §3.7; Cloud Native OAuth ch. 4; Solving IdM ch. 1, 4; Zero Trust Networks ch. 6)

## Rules by topic

### Federation vs brokering vs local accounts

Source: Keycloak 26.8.2 documentation, Server Admin: User federation, Managing organizations; Keycloak – IAM for Modern Applications, ch. 10; NIST SP 800-63C-4 §2.3, §2.5, §3.3.3, §3.5, §4.3.1; Cloud Native Data Security with OAuth, ch. 14 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 4, 12; API Security in Action, ch. 8; BG Rehberi 3.2.1.13; Identity Attack Vectors, 2nd ed. (Haber, Rolls, 2024), ch. 13, 14; Learning Digital Identity (Windley, 2023), ch. 6, 7, 11, 13, 22 (merged 2026-11-18).

1. Keep each person's account in the realm where they sign in: staff, operators and service accounts in `hkm`, open-data portal users in `hkm-public`. Why: users authenticate only in the realm that holds their account. (Keycloak IAM ch. 10)
2. Keep self-registration off in `hkm`; allow it only in `hkm-public`, with the account inactive until email verification or an IdP login proves it. Why: workforce accounts need an accountable source, and a public sign-up link needs proof before activation. (Keycloak IAM ch. 10; Cloud Native OAuth ch. 14; Identity Attack Vectors ch. 14; Learning Digital Identity ch. 6, 11)
3. Treat Keycloak as a federation proxy that meets the IdP rules towards its clients and the relying-party rules towards institution IdPs, aim for FAL2 on every hop, and report the lowest FAL of the whole path. Why: the connection is only as strong as its weakest hop. (NIST SP 800-63C-4 §3.3.3; Learning Digital Identity ch. 13; HKM D47)
4. Sign a written trust agreement with each institution before its IdP is linked to its organization, keep it in Git next to its Terraform IdP definition, and review it periodically; it covers attributes and purposes, attribute quality, population, the assurance and MFA the platform requires from the IdP, the provisioning model, the platform's session limits, access control, retention, redress and key rotation. Document internal federations between platform teams the same way. Why: FAL2 requires an agreement in place before the first login, even inside one organization, protocols carry trust but cannot create it, and an undocumented federation becomes an unwritten agreement that costs downtime when it breaks. (NIST SP 800-63C-4 §2.3, §3.5, §4.3.1; Learning Digital Identity ch. 13, 22) Retention open.
5. Declare the minimum IAL, AAL and FAL each client needs from the sensitivity of its data, compare it with what the institution IdP asserts, treat a missing value as "none", not as level 1, and check an IdP's service and availability before trusting it. Why: Keycloak cannot see the institution's proofing or authentication, and an identity with unvalidated attributes cannot be tied to an accountable person. (NIST SP 800-63C-4 §2.5; Solving IdM ch. 4, 12)
6. Create every LDAP provider with Import Users on, Edit Mode READ_ONLY and Sync Registrations off, pinned in Terraform at creation. Why: Keycloak derives the default mappers from these settings and does not reconfigure them later, READ_ONLY keeps the directory the source of truth and usernames immutable, and without import users cannot become organization members or be covered by FGAP. (Keycloak docs 26.8, p. 2, p. 3, p. 9; Keycloak IAM ch. 10; HKM D1, D27)
7. Connect only with `ldaps://` URLs, put the directory CA in the Keycloak server truststore, and leave Use Truststore SPI at Always. Why: TLS to LDAP relies on the server truststore. (Keycloak docs 26.8, p. 3)
8. List failover servers space-separated, and only when they are replicas of one directory with identical entry identifiers; tune Connection Timeout to the network. Why: failover is sequential and spends the full timeout on each unreachable URL, independent servers break sync and login, and commas break the list. (Keycloak docs 26.8, p. 3)
9. Run one full synchronization right after creating the provider, then schedule periodic changed-users synchronization. Why: pre-imported users skip import at first login, and changed-users sync is cheaper. (Keycloak docs 26.8, p. 3; Keycloak IAM ch. 10)
10. Map directory groups with the Group Mapper from a defined branch so that they arrive as top-level `platform_{tenant}_{role}` groups, map profile fields with User Attribute mappers, and use neither a Hardcoded Role mapper nor a role mapper for data permissions. Why: Ranger matches flat names, and role mappers grant roles to every federated user. (Keycloak docs 26.8, p. 3; Keycloak IAM ch. 10; API Security in Action ch. 8; HKM D2, D3)
11. Add the MSAD User Account Mapper to every Active Directory provider. Why: it carries disabled and must-change-password state from `userAccountControl` and `pwdLastSet`. (Keycloak docs 26.8, p. 3)
12. Keep at least one local administrator account in the realm or in `master`. Why: a failing provider blocks logins and lookups without falling back to the next provider. (Keycloak docs 26.8, p. 3)
13. Avoid unfiltered user searches in the admin console on large imported directories, and keep Remove invalid users during searches off. Why: every returned user triggers an LDAP lookup, and users missing from the directory then stay read-only and disabled with their audit history instead of being deleted. (Keycloak docs 26.8, p. 3)
14. Do not unlink LDAP users without giving them another sign-in or reset path. Why: LDAP import stores no password, so an unlinked user has none. (Keycloak IAM ch. 10)
15. Bind every transitional LDAP provider with a dedicated, low-privilege read-only account whose credential comes from OpenBao, map only static directory groups, and retrieve memberships through the directory's member-of attribute where it offers one. Why: ordinary users usually cannot search the directory, dynamic search-defined groups need expensive searches to resolve members, and repeated group searches load the directory. (API Security in Action ch. 8; HKM D27, D33)
16. Broker e-Devlet only in `hkm-public`, for portal users whose identity must be proven, never in `hkm`, and offer no social login in either realm. Why: the guide accepts e-Devlet integration as central authentication, and workforce identities come from the institutions. (BG Rehberi 3.2.1.13; Solving IdM ch. 2, 4; HKM D35)
17. Report every local account in `hkm` (no institution IdP link and no LDAP federation link) as an exception with an owner and a reason, and review the list at the access-review interval. Why: local accounts escape the institution's lifecycle control and need individual care. (Identity Attack Vectors ch. 13; HKM D27)
18. Before an institution IdP is linked to its organization, assess it and record the result in its onboarding record: how the institution proofs its staff, which authentication methods and MFA its IdP enforces and how it stores authenticators, its protocol and signature algorithms, who issues its TLS and signing certificates, how it handles its keys, and its access policies; confirm its provenance out of band by fetching the metadata over TLS from the institution's own domain and confirming it with a named contact, rather than trusting signatures alone. Why: federation means accepting another party's authentication, confidence rests on these technical properties, and a valid signature proves that content is intact, not who the issuer is. (Learning Digital Identity ch. 7, 11, 13, 22)

Example:

```hcl
resource "keycloak_ldap_user_federation" "t_acme_ad" {
  realm_id           = keycloak_realm.hkm.id
  name               = "t-acme-ad"
  vendor             = "AD"
  connection_url     = "ldaps://dc01.acme.local:636 ldaps://dc02.acme.local:636"  # replicas, space separated
  users_dn           = "ou=staff,dc=acme,dc=local"
  bind_dn            = "cn=svc-keycloak,ou=service,dc=acme,dc=local"
  bind_credential    = "$${vault.t-acme-ldap-bind}"
  edit_mode          = "READ_ONLY"
  import_enabled     = true
  sync_registrations = false
  use_truststore_spi = "ALWAYS"
}

resource "keycloak_ldap_group_mapper" "t_acme_platform_groups" {
  realm_id                   = keycloak_realm.hkm.id
  ldap_user_federation_id    = keycloak_ldap_user_federation.t_acme_ad.id
  name                       = "platform-groups"
  ldap_groups_dn             = "ou=hkm,ou=groups,dc=acme,dc=local"   # holds platform_t-acme_* groups only
  group_name_ldap_attribute  = "cn"
  group_object_classes       = ["group"]
  membership_ldap_attribute  = "member"
  membership_attribute_type  = "DN"
  memberof_ldap_attribute    = "memberOf"
  user_roles_retrieve_strategy = "GET_GROUPS_FROM_USER_MEMBEROF_ATTRIBUTE"   # static groups, read via memberOf
  mode                       = "READ_ONLY"
  preserve_group_inheritance = false
}

resource "keycloak_ldap_msad_user_account_control_mapper" "t_acme_uac" {
  realm_id                = keycloak_realm.hkm.id
  ldap_user_federation_id = keycloak_ldap_user_federation.t_acme_ad.id
  name                    = "msad-account-state"
}
```

### SAML and OIDC brokering details

Source: Keycloak 26.8.2 documentation, Server Admin: Integrating identity providers, Managing organizations; Keycloak – IAM for Modern Applications, ch. 3, 10; NIST SP 800-63C-4 §2.3, §3.6, §3.11, §3.13, §4.4, §4.9–4.11; OWASP ASVS 5.0, 6.8.2, 6.8.3 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 7, 16; Learning Digital Identity (Windley, 2023), ch. 4, 9, 13 (merged 2026-11-18).

1. Broker each institution IdP with the OpenID Connect v1.0 provider using the authorization code flow, import its settings from the discovery document, set the exact Issuer, and keep Validate Signatures on with Use JWKS URL over HTTPS. Why: back-channel code exchange keeps the assertion out of the browser, issuer and signature checks make the external token trustworthy, and keys must come from an authenticated source that follows the IdP's rotation. (Keycloak docs 26.8, p. 8; Keycloak IAM ch. 3, 10; NIST SP 800-63C-4 §3.6, §4.4, §4.6.1.1, §4.11.1; Learning Digital Identity ch. 9; HKM D47)
2. Use no wildcards in issuers, redirect URIs, allowlists or organization domains. Why: each trusted party must be identified exactly. (NIST SP 800-63C-4 §3.6, §4.4; RFC 9700 §4.4; HKM D31)
3. Authenticate the realm to the institution IdP with a JWT signed by the realm private key. Why: a signed assertion avoids a shared static secret. (Keycloak docs 26.8, p. 8; HKM D12)
4. Use a client secret towards an institution IdP only as a listed per-institution exception when the IdP supports nothing else, keep it in the vault (OpenBao), and resend it with any change to the IdP's token URL. Why: the exception must stay visible and reviewable, and 26.5+ requires the secret again when the token URL changes. (Keycloak docs 26.8, p. 8, p. 17; decision 2026-11-17)
5. Start every institution login from Keycloak, refuse unsolicited IdP-initiated responses, send a `nonce` in every upstream request, and reject any response whose nonce, signature, issuer, time window, audience, identifier (replay) or assurance level fails. Why: when Keycloak starts the transaction an attacker cannot inject an assertion, and every check must pass before Keycloak creates a session. (NIST SP 800-63C-4 §2.3, §3.11.1, §4.9, §4.10; OWASP ASVS 5.0, 6.8.2, 6.8.3; Solving IdM ch. 7; Learning Digital Identity ch. 13)
6. Use Verify essential claim to accept only ID tokens carrying the expected institution identifier. Why: it rejects users of the same IdP who do not belong to the tenant. (Keycloak docs 26.8, p. 8)
7. Attach a post login flow that requires a second factor to every IdP that does not assert MFA. Why: once the login is handed to an external IdP, the browser flow's own second-factor steps do not run. (Keycloak docs 26.8, p. 8; HKM D9)
8. Set Sync Mode Override on every IdP mapper: `import` for values the user may change locally, `force` for attributes the IdP owns. Why: `legacy` and inherited modes can silently keep stale data. (Keycloak docs 26.8, p. 8)
9. Leave Store Tokens and Stored Tokens Readable off unless a client must call the IdP's API, and never embed an institution token in platform tokens. Why: stored tokens extend the IdP's trust to anyone holding the read-token role. (Keycloak docs 26.8, p. 8; Learning Digital Identity ch. 4; HKM D53)
10. Use SAML IdPs only where the institution has no OIDC, and for each one set the Identity Provider Entity ID, agree a persistent NameID rather than the email address, sign authentication requests and send them by HTTP-POST, require signed responses or assertions, take keys from the metadata descriptor URL and agree a dated certificate rollover with the institution, avoid SHA1 algorithms, keep Reject proxy restriction on, and encrypt assertions that carry personal data through the browser. Why: an empty entity ID disables issuer validation, email addresses change and expose personal data, an expired or swapped certificate stops every login, SHA1 is deprecated, and front-channel content ends up in logs, proxies and history. (Keycloak docs 26.8, p. 8; Keycloak IAM ch. 3; NIST SP 800-63C-4 §3.13.3, §4.11.2; Solving IdM ch. 7)
11. Keep client assertion re-use disabled and Client ID as audience off on OIDC IdPs. Why: re-use enables replay, and the Client ID audience is non-standard. (Keycloak docs 26.8, p. 8)
12. Configure the IdP Logout URL and back-channel logout wherever the institution IdP supports them. Why: Keycloak then ends the user's session at the IdP used for login. (Keycloak docs 26.8, p. 8; HKM D41)
13. Review IdP mappers that target organization groups whenever an IdP is unlinked from an organization. Why: those mappings stop working with only a log warning. (Keycloak docs 26.8, p. 2)
14. Keep every Keycloak node on NTP and agree time synchronisation with each institution IdP. Why: assertions are valid only long enough to be delivered, so clock drift makes them arrive expired. (Solving IdM ch. 7, 16)

Example:

```hcl
resource "keycloak_oidc_identity_provider" "t_acme" {
  realm                         = keycloak_realm.hkm.id
  alias                         = "t-acme-oidc"
  issuer                        = "https://login.acme.gov.tr"
  authorization_url             = "https://login.acme.gov.tr/authorize"
  token_url                     = "https://login.acme.gov.tr/token"
  jwks_url                      = "https://login.acme.gov.tr/jwks"
  logout_url                    = "https://login.acme.gov.tr/logout"
  client_id                     = "hkm-broker"
  client_auth_method            = "private_key_jwt"   # realm key signs the assertion
  validate_signature            = true
  pkce_enabled                  = true
  pkce_method                   = "S256"
  trust_email                   = false               # true only after t-acme confirms email_verified
  sync_mode                     = "IMPORT"
  store_token                   = false
  backchannel_supported         = true
  hide_on_login_page            = true                # chosen by organization domain routing
  first_broker_login_flow_alias = "hkm-first-broker-login"
  post_broker_login_flow_alias  = "hkm-idp-second-factor"
}
```

### Account linking and first login

Source: Keycloak 26.8.2 documentation, Server Admin: Integrating identity providers, Managing organizations; Keycloak – IAM for Modern Applications, ch. 10; NIST SP 800-63C-4 §2.3, §3.4, §3.8, §4.9; OWASP ASVS 5.0, 6.8.1 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 7, 10, 18; Learning Digital Identity (Windley, 2023), ch. 6, 13, 14 (merged 2026-11-18).

1. Key every brokered account on IdP alias plus upstream subject, never on subject alone and never on a value that contains a username, email or employee number. Why: subjects from different IdPs can collide, and personal data in a federated identifier is forbidden at FAL2. (NIST SP 800-63C-4 §2.3, §3.4, §4.9; OWASP ASVS 5.0, 6.8.1; Solving IdM ch. 7; Learning Digital Identity ch. 14)
2. Give every brokered provider a dedicated first broker login flow that collects missing data or forces the credential setup the platform needs, and create accounts just in time there. Why: the brokered account is created and linked at first login, so that is the only point to enforce it, and `sub` stays stable. (Keycloak docs 26.8, p. 8; Keycloak IAM ch. 10; Learning Digital Identity ch. 6, 13; HKM D43)
3. In that flow link only through Verify Existing Account By Email or By Re-authentication, mark Create User If Unique REQUIRED where duplicates must be impossible, and notify the user whenever a link is added or removed. Why: linking outside an authenticated session of the existing account lets an attacker attach their identity to someone else's account. (Keycloak docs 26.8, p. 8; NIST SP 800-63C-4 §3.8, §3.8.1; Solving IdM ch. 10, 18; Learning Digital Identity ch. 13; HKM D18)
4. Never use the Automatically Set Existing User (AutoLink) step in `hkm-public` or any realm where users choose their own username or email; use it only behind Detect Existing Broker User, both required, for IdPs whose accounts are pre-provisioned (read-only LDAP or SCIM). Why: AutoLink links without verification, and in the pre-provisioned case only accounts that already exist may log in. (Keycloak docs 26.8, p. 8; HKM D43)
5. When accounts are resolved by attributes, request enough attributes to match exactly one account and design the match so it can never attach a foreign identity. Why: a wrong match hands one person's account to another. (NIST SP 800-63C-4 §3.8.2)
6. Mark a provider Account Linking Only when users should link it from the account console but not choose it on the login page. Why: it is then hidden from the login page. (Keycloak IAM ch. 10)
7. In realms created before Organizations were enabled, duplicate the browser and first broker login flows, add the organization steps, and only then bind them. Why: only newly created realms get the updated flows automatically. (Keycloak docs 26.8, p. 2)
8. Let users see and remove identity provider links in the Account Console, and give a user another sign-in path before removing a link that is their only one. Why: a linked identity may be compromised or no longer wanted, and an account with no login left is orphaned. (Solving IdM ch. 18)

Example:

```hcl
resource "keycloak_authentication_flow" "hkm_first_broker_login" {
  realm_id = keycloak_realm.hkm.id
  alias    = "hkm-first-broker-login"   # copy of "first broker login", no AutoLink step
}

resource "keycloak_authentication_execution" "hkm_fbl_create_unique" {
  realm_id          = keycloak_realm.hkm.id
  parent_flow_alias = keycloak_authentication_flow.hkm_first_broker_login.alias
  authenticator     = "idp-create-user-if-unique"
  requirement       = "REQUIRED"
}
```

### User attributes and profile

Source: Keycloak 26.8.2 documentation, Server Admin: Managing users and the user profile, Mitigating security threats; Keycloak – IAM for Modern Applications, ch. 10; NIST SP 800-63C-4 §3.3.2, §3.7, §4.6.1.1 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 4, 14, 18; Learning Digital Identity (Windley, 2023), ch. 6, 8, 10, 11 (merged 2026-11-18).

1. Declare every attribute the platform uses in the realm user profile and keep Unmanaged Attributes at Disabled (at most Admin can edit). Why: undefined attributes are then ignored, and unmanaged attributes let attackers store arbitrary data. (Keycloak docs 26.8, p. 4, p. 13; Cloud Native OAuth ch. 4)
2. Put a `length` validator on every user-editable attribute and a `multivalued` validator on multivalued ones, and avoid costly regular expressions. Why: otherwise values may reach 2048 characters and counts are unbounded, and ReDoS patterns exhaust CPU. (Keycloak docs 26.8, p. 4, p. 13)
3. Validate names with `person-name-prohibited-characters` and usernames with `username-prohibited-characters` plus `up-username-not-idn-homograph`. Why: they block script injection and IDN homograph usernames. (Keycloak docs 26.8, p. 4)
4. Collect or confirm missing user data with required actions (Verify Email, Update Profile, and the enabled `VerifyProfile`) at next login rather than having administrators enter it. Why: the owner confirms the data before authentication completes, and non-compliant profiles get fixed. (Keycloak docs 26.8, p. 4; Keycloak IAM ch. 10)
5. Publish, per client, the list of attributes Keycloak releases to it; for platform clients the platform decides the list, and anything requested beyond it is not released. Why: attribute release must be minimal and visible to users. (NIST SP 800-63C-4 §3.3.2, §3.7, §4.6.1.1)
6. Control self-service in the account console through `account` client roles, removing `manage-account` where users must not change their data. Why: every new user receives `manage-account` by default. (Keycloak IAM ch. 10)
7. Keep an attribute ownership map in Git next to the user profile: for each attribute its owner (institution IdP, Keycloak profile or application), who may view and edit it, how it reaches applications (claim, UserInfo, SCIM or event) and how fast a change must propagate; let users correct what they own, give IdP-owned attributes sync mode `force`, and keep attributes that drive access (department, job level, group membership) editable only by administrators or the IdP. Why: users and support must know where each value is changed, privacy law gives users correction rights, and access attributes belong to the IdP owner, not the user. (Solving IdM ch. 14; Learning Digital Identity ch. 6, 8)
8. Keep the login identifier, the display name and the contact email as separate profile attributes. Why: names and addresses change or are reassigned, while the identifier must not. (Solving IdM ch. 4, 18; Learning Digital Identity ch. 10)
9. Verify every email address used for login, recovery, organization membership or notices, imported and migrated ones included: Verify Email for local accounts, Trust Email only for IdPs that verify addresses. Why: an unverified address can match an access rule or route recovery to an attacker. (Solving IdM ch. 4; Learning Digital Identity ch. 11)
10. Ask at registration only for the attributes the first use needs, request further ones when a feature needs them by marking them required only for the client scope that uses them together with the `VerifyProfile` required action, and combine IdP-supplied with user-entered values. Why: long sign-up forms deter users, and data asked for later has a visible purpose. (Solving IdM ch. 4, 18; Learning Digital Identity ch. 11)
11. In `hkm`, make email optional only for an institution whose staff have no address or may not use it as an identifier, assign their organization membership explicitly, and give them recovery through the institution IdP or the help desk. Why: domain-based membership and email recovery then have no channel. (Solving IdM ch. 18; HKM D48)

Example (length value illustrative):

```json
{
  "unmanagedAttributePolicy": "DISABLED",
  "attributes": [
    {
      "name": "institution",
      "permissions": { "view": ["admin"], "edit": ["admin"] },
      "validations": { "length": { "max": 64 } }
    },
    {
      "name": "department",
      "permissions": { "view": ["admin", "user"], "edit": ["admin"] },
      "annotations": { "owner": "t-acme IdP (sync mode force)" }
    },
    {
      "name": "phone_contact",
      "required": { "scopes": ["ckan-contact"] },
      "permissions": { "view": ["admin", "user"], "edit": ["admin", "user"] },
      "validations": { "length": { "max": 32 } }
    }
  ]
}
```

### Provisioning, SCIM and deprovisioning

Source: Keycloak 26.8.2 documentation, Server Admin: Workflows and SCIM, Managing users and the user profile; NIST SP 800-63C-4 §3.9, §3.10, §4.6.3, §4.6.5; Cloud Native Data Security with OAuth, ch. 4, 11, 14 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 2, 4, 15, 17, 18; Identity Attack Vectors, 2nd ed., ch. 6, 11; Learning Digital Identity, ch. 6, 10, 13; Zero Trust Networks, 2nd ed. (Rais, Morillo, Gilman, Barth, 2024), ch. 6 (merged 2026-11-18).

1. Record per institution the provisioning model (just in time at first broker login, pre-provisioning through SCIM, or ephemeral) and the component that owns each life cycle event (provisioning, authorization, authentication, enforcement, sessions, logout, recovery, deprovisioning); for each relationship type (organization, workforce user, `hkm-public` user, service account such as `svc-spark-etl`) plan discovery, creation, propagation, use, update and termination in every subsystem that holds a copy of the identity (Keycloak, Ranger, OpenBao, Superset, Airflow); create accounts only by automation from one authoritative source per population, and bind every pre-provisioned account to a federated identifier. Why: a session may only start from an account provisioned the agreed way, automation gives consistent and traceable accounts, and an event or subsystem with no owner becomes a gap, such as accounts that are created but never closed. (NIST SP 800-63C-4 §3.9, §4.6.3; Solving IdM ch. 2, 4; Identity Attack Vectors ch. 6; Learning Digital Identity ch. 6; Zero Trust Networks ch. 6; HKM D43)
2. Prefer the standard SCIM API for migration, administration and pre-provisioning, write an explicit attribute mapping per institution, and accept it only under the institution's trust agreement, over mutually authenticated channels, limited to the needed attributes and population. Why: a standard API works from any stack, its common schema rarely matches both sides, and provisioning exposes users who may never log in. (Cloud Native OAuth ch. 4; NIST SP 800-63C-4 §3.10, §4.6.5; Solving IdM ch. 4; Learning Digital Identity ch. 13)
3. Enable SCIM on the server and in `hkm` only, and give the provisioning caller a confidential service-account client using `client-jwt`, the narrowest of `manage-users`, `view-users`, `query-users`, `query-groups` as both service-account roles and role scope mappings, Full Scope Allowed off, and an Audience mapper equal to the public SCIM base URL. Why: tokens without that exact `aud` get 401, and the API runs under the Admin REST API permission model. (Keycloak docs 26.8, p. 10; HKM D12, D43)
4. Manage administrator accounts and admin-role groups only through Terraform or the Admin REST API. Why: SCIM refuses writes to them with 403. (Keycloak docs 26.8, p. 10)
5. Decommission users centrally in Keycloak with `disable-user`, `revoke-role`, `leave-group` or `unlink-user` steps (for example on removal from `platform_t-acme_analyst`), ending sessions, consents and tokens and unbinding the federated identifier on a termination signal; run `delete-user` only as a later retention step. Why: disabled accounts keep the audit trail, and personal data then has one removal point when retention ends. (Keycloak docs 26.8, p. 4, p. 10; NIST SP 800-63C-4 §4.6.5; Cloud Native OAuth ch. 14; Solving IdM ch. 2, 15, 17; Identity Attack Vectors ch. 11; HKM D22)
6. Base workflow conditions that grant roles or groups only on attributes whose Who can edit is Admin, with Unmanaged Attributes not Enabled. Why: a self-registering `hkm-public` user could otherwise grant themselves the role. (Keycloak docs 26.8, p. 10)
7. Do not change the step chain of a workflow that has active scheduled executions, and do not delete such a workflow unless its pending steps may be dropped. Why: the edit is blocked and deletion silently discards pending steps. (Keycloak docs 26.8, p. 10)
8. Set the full public hostname before creating workflows that use the `invite-user` step. Why: without a frontend URL or full `--hostname` the workflow is rejected at creation. (Keycloak docs 26.8, p. 10)
9. Make bulk user updates idempotent and rehearse them before production. Why: a rerun after an interruption must create no duplicates. (Cloud Native OAuth ch. 11)
10. Give every contractor, intern or partner account an admin-only expiry attribute, have its sponsor approve renewal at intervals set by its privileges, and disable it through a scheduled Workflow when it is not renewed. Why: temporary accounts outside the normal joiner and leaver process outlive the engagement. (Solving IdM ch. 18; HKM D22)
11. Keep one account when a person changes status (contractor to staff or back) and change groups and attributes instead of creating a second account. Why: two accounts for one person cause ambiguity or unauthorized access. (Solving IdM ch. 18)
12. Never reissue the username of a deleted account: record a keyed hash (HMAC-SHA256) of each deleted username as a tombstone, check new usernames against active, disabled and tombstoned accounts, and keep each tombstone longer than any backup that still holds the deleted account's data. Why: a new owner of the same name could reach the previous owner's data in SSO applications or ask for it to be restored. Keycloak has no reserved-name list, so the tombstone needs a custom event listener and user profile validator, still to be checked against the Keycloak docs. Do not tombstone email addresses: on the platform an email address is a contact attribute, not an identifier, and a list of hashed former addresses would keep a history that HKM D28 forbids. (Solving IdM ch. 4, 15, 17; Learning Digital Identity ch. 10; HKM D1, D24, D28; decision 2026-11-18)
13. Decide per realm whether a disabled account may be reactivated, and reactivate one only after re-verifying that the requester is its original owner (institution IdP or help-desk re-verification). Why: handing an old account to someone else is a breach. (Solving IdM ch. 15, 17; HKM D48)
14. Propagate every identity change (disable, group membership, attribute) to every system that holds a copy: run a scheduled reconciliation that compares Keycloak group memberships and disabled users with Ranger's synced users and groups and with application user tables, applies removals first and raises a ticket for every difference, and after a membership change such as removal from `platform_t-acme_analyst` confirm that downstream access followed. Why: the platform's systems become consistent only eventually, partial failures need recovery rather than one transaction, and a change is useless until every consumer sees it. (Learning Digital Identity ch. 6; HKM D4)
15. Before sending an organization invitation for a workforce account that no institution IdP vouches for, have the person's official identity document checked, ideally in person and vouched for by a known colleague, and never treat possession of an email address or trust on first use as the binding of the account to the person. Why: an introduction that rests only on an emailed link can be intercepted, and every later recovery can only be as strong as this first binding. (Zero Trust Networks ch. 6; HKM D61)

Example (step ids and durations are placeholders to check against the Workflows documentation):

```yaml
# hkm workflow: dormant workforce accounts (kept in Git)
name: hkm-dormant-accounts
on: user-authenticated
concurrency:
  restart-in-progress: true
steps:
  - uses: <notify-step-id>
    after: <notice period>
  - uses: disable-user
    after: <inactivity period>
```

```yaml
# hkm workflow: temporary accounts (step ids and durations to check against the Workflows documentation)
name: hkm-contractor-expiry
on: <scheduled trigger>
if: user attribute contract_end < now and not renewed   # contract_end: Who can edit = admin
steps:
  - uses: <notify-step-id>        # to the sponsor of the platform_t-acme_analyst member
  - uses: disable-user
    after: <grace period>
```

```yaml
# jobs/identity-reconcile.yaml (schedule and layout illustrative)
schedule: "<cron>"
checks:
  - source: keycloak hkm group platform_t-acme_analyst (Admin REST API members)
    target: ranger usersync group platform_t-acme_analyst
  - source: keycloak hkm disabled users
    target: [superset users (active), airflow users (active)]
on_difference: { order: removals-first, action: open-ticket }
```

### Legacy user migration

Source: Solving Identity Management in Modern Applications, 2nd ed. (Wilson, Hingnikar, 2022), ch. 1, 4 (merged 2026-11-18).

1. Plan any move of users from a legacy store as its own work item covering credentials, duplicate merging, uptime and a cutoff date. Why: no identity standard covers migration, and it is a common source of scope creep. (Solving IdM ch. 1, 4)
2. For a bulk import without usable password hashes, import only accounts with verified email addresses, announce the move in advance, and send each user a personal, expiring set-password link through an execute-actions email. Why: the link must be safe for the data's sensitivity and must not look like phishing, and administrators never set the password. (Solving IdM ch. 4; HKM D8)
3. For a gradual migration, set a cutoff date, bulk-move or abandon the remaining accounts at the cutoff, tombstone the usernames of accounts not moved, and disable each legacy record as soon as it has moved. Why: the legacy store can then be retired, names are not reused, and data is not edited in two places. (Solving IdM ch. 4; HKM D24)
4. Allow email-based reset for accounts not yet migrated only when the legacy store cannot contain recycled email addresses. Why: a reassigned address would hand the account to its new owner. (Solving IdM ch. 4)
