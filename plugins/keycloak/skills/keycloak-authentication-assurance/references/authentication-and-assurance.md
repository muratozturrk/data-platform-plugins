# Authentication and assurance

Status: merged from Keycloak docs 26.8, Keycloak – IAM for Modern Applications, Identity standards (NIST SP 800-63B-4, 800-63C-4, OWASP ASVS 5.0, CISA/NSA ESF IAM) and Cloud Native Data Security with OAuth (2026-11-17); Solving Identity Management, API Security in Action and Türkiye mevzuatı (BG Rehberi, KVKK Rehberi), Identity Attack Vectors, OAuth 2 in Action, Learning Digital Identity and Zero Trust Networks (2026-11-18); no pending sections.

## Starter rules

1. Give administrators and operators separate administrative accounts, used only for administrative work, that sign in only with passkeys, registered through the WebAuthn Register Passwordless required action with User Verification `required` and Discoverable Credential `required`, two passkeys per administrator, and Acceptable AAGUIDs with `Direct` attestation where the hardware is managed; use device-bound, non-exportable keys wherever AAL3 is claimed. Why: their sessions control every realm and client, OTP and push can be phished, loginless use needs user verification and resident keys, attestation `none` can zero the AAGUID, and synced passkeys cannot reach AAL3. (research: NIST SP 800-63B-4 AAL3; Keycloak docs 26.8, p. 7; Keycloak IAM ch. 11; NIST SP 800-63B-4 §2.3.2, App. B.2; CISA/NSA ESF IAM, Evaluate Assurance Requirements; Cloud Native OAuth ch. 1, 14; Solving IdM ch. 12, 19; BG Rehberi 3.1.12.5, 3.1.12.11; Identity Attack Vectors ch. 6, 8, 11, 12, 14, App. A; Learning Digital Identity ch. 11; Zero Trust Networks ch. 10; HKM D9, D23, D37, D48)
2. Require MFA for every workforce login in `hkm`, inside or outside the institution network, with passkeys as the target factor for every user and TOTP allowed only until the user has enrolled a passkey. Why: agency staff need a phishing-resistant authenticator, AAL2 verifiers must offer one, and the guide's MFA for external access is then always met. (research: NIST SP 800-63B-4 AAL2; NIST SP 800-63B-4 §2.2.2; Keycloak docs 26.8, p. 7; Solving IdM ch. 12, 19; BG Rehberi 3.1.12.15; Identity Attack Vectors ch. 7, 10, 13; Learning Digital Identity ch. 11; Zero Trust Networks ch. 2, 9, 10; HKM D9)
3. Set SSO Session Idle and SSO Session Max explicitly in every realm: `hkm` 30 min and 10 h; administrators (`master` and the `hkm` admin console client) 15 min and 12 h; change them only through a reviewed Terraform change. Why: either timeout must end the parent session that every client session depends on, AAL2 allows at most 1 h inactivity and 24 h overall, AAL3 at most 15 min and 12 h, and the guide allows the limits to change only by authorised change. (research: NIST SP 800-63B-4 session management; Keycloak docs 26.8, p. 5; Keycloak IAM ch. 12; NIST SP 800-63B-4 §2.2.3, §2.3.3, §5.2; OWASP ASVS 5.0, 7.3.1, 7.3.2; Solving IdM ch. 2, 11; BG Rehberi 3.1.12.6, 3.2.2.3; Identity Attack Vectors ch. 19; HKM D10, D17, D38)
4. Map ACR names to levels of authentication at realm level (`silver` = 1, `gold` = 2), document which methods each level requires, with `silver` in `hkm` meaning password plus a second factor (password-only `silver` exists only in `hkm-public`) and `gold` always requiring a phishing-resistant passkey, and agree that mapping with the owners of the applications, make each client request its level through `acr_values` and check `acr` (and `auth_time` where freshness matters) in the ID token, and send `acr_values` through PAR or a request object where possible. Why: step-up must be verifiable downstream, a level value means nothing unless both sides define it, only an origin-bound authenticator cannot be replayed by a phishing site, and the browser can rewrite `acr_values` or `claims` in the URL. (research: OWASP ASVS 5.0, 10.3.4; Keycloak docs 26.8, p. 7; Keycloak IAM ch. 11; NIST SP 800-63B-4 §5.2; Cloud Native OAuth ch. 6, 14; Solving IdM ch. 6, 11, 12; API Security in Action ch. 7; Learning Digital Identity ch. 11; HKM D9; decision 2026-11-18)
5. Never enable the resource-owner password grant: keep Direct Access Grants off on every client, also for testing. Why: it exposes credentials to clients, defeats MFA, and every enabled grant adds attack surface. (research: RFC 9700 §2.4; OWASP ASVS 5.0, 10.4.4; Keycloak docs 26.8, p. 11; Keycloak IAM ch. 3, 14; HKM D45)
6. Record every successful and failed authentication of users and clients in every realm (`LOGIN`, `LOGIN_ERROR`, `CODE_TO_TOKEN`, `CLIENT_LOGIN` and their error variants). Why: the guide requires a record of every attempt, and failed attempts feed brute-force detection. (API Security in Action ch. 3; BG Rehberi 3.1.12.3, 3.2.1.3)

## Rules by topic

### Authentication flows and factors

Source: Keycloak 26.8.2 documentation, Server Admin: Configuring authentication; Keycloak – IAM for Modern Applications, ch. 11; NIST SP 800-63B-4 §2, §3.1–3.2; OWASP ASVS 5.0 V6; CISA/NSA ESF IAM, Implementing MFA; Cloud Native Data Security with OAuth, ch. 14 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 12, 14, 17, 18; BG Rehberi 3.1.12.2, 3.1.12.14, 3.2.1.2, 3.2.1.10, 3.2.1.13; KVKK Rehberi 3.1; Identity Attack Vectors, 2nd ed. (Haber, Rolls, 2024), ch. 6, 8, 10, 11, 13, 14, 16, 19; Learning Digital Identity (Windley, 2023), ch. 4, 6, 9, 11; Zero Trust Networks, 2nd ed. (Rais, Morillo, Gilman, Barth, 2024), ch. 2, 6, 8, 9 (merged 2026-11-18).

1. Keep all authentication logic in central Keycloak flows, let no platform application keep its own login or password store, and show every client of a realm the same login theme and MFA ceremony; clients only start a code flow, adding `acr_values`, `prompt` or `max_age` when needed. Record an application that cannot use Keycloak as an exception with an owner, an end date and compensating controls from a documented risk analysis, and place it behind an authenticating gateway or proxy that logs access. Why: methods can change without touching clients, no application can skip MFA or step-up, a consistent ceremony lets users recognise the real login, central authentication with a recorded exception path is a guide requirement, and a system outside the authentication model keeps its own unmanaged accounts. (Cloud Native OAuth ch. 14; CISA/NSA ESF IAM, Implementing MFA; BG Rehberi 3.1.12.14, 3.2.1.13; Identity Attack Vectors ch. 16, 19; Learning Digital Identity ch. 4; Zero Trust Networks ch. 6, 8, 9)
2. Never edit built-in flows: duplicate them in Terraform, change requirements on the copy, bind it, and test corner cases such as a user whose second factor was deleted. Why: the untouched original stays available as a rollback target, and a wrong requirement silently turns two-factor authentication off. (Keycloak docs 26.8, p. 7; Keycloak IAM ch. 11)
3. Review every requirement from top to bottom, including the registration and reset-credentials flows: keep the cookie step ALTERNATIVE first and the forms subflow ALTERNATIVE after it, and mark both steps of an identifier-first login REQUIRED. Why: one wrong requirement silently skips or forces a factor, and a weak reset flow bypasses a strong login. (Keycloak IAM ch. 11)
4. Enforce the second factor in `hkm` with a REQUIRED subflow or a registration required action, and deny login (`Condition - sub-flow executed` plus `Deny Access`) or force enrolment when a user has none. Why: the default conditional OTP succeeds silently for users without a second-factor credential. (Keycloak docs 26.8, p. 7; Keycloak IAM ch. 11; OWASP ASVS 5.0, 6.3.3; HKM D9)
5. Treat a password-only login as AAL1, and allow it only in `hkm-public` for low-risk actions; require two factors of different types, or one multi-factor authenticator, for AAL2, never count two codes reaching the same phone as two factors, and never count location, network or time of day as a factor at all. Why: a password plus a session cookie is not two factors, one stolen device defeats both codes, and location and time are context signals that are not secure enough to stand as a factor. (NIST SP 800-63B-4 §2.2.1; Cloud Native OAuth ch. 14; Learning Digital Identity ch. 11; Zero Trust Networks ch. 2, 6; HKM D9)
6. Never use email as an authentication factor, and do not use SMS or voice codes anywhere, neither as a factor nor for recovery. Why: email can be read with the password alone, and SMS and voice are restricted authenticators that can be intercepted. (NIST SP 800-63B-4 §3.1.3.1, §3.2.9; OWASP ASVS 5.0, 6.3.6, 6.6.1; Solving IdM ch. 14; Identity Attack Vectors ch. 14; Learning Digital Identity ch. 11; Zero Trust Networks ch. 6; HKM D64)
7. Do not use approve-only push; require the user to transfer a value between channels and cap the number of prompts. Why: approve-only push invites MFA-fatigue attacks. (NIST SP 800-63B-4 §3.1.3; Identity Attack Vectors ch. 11, 14; Learning Digital Identity ch. 11)
8. Where TOTP is still allowed, use TOTP rather than HOTP with a key of at least 112 bits, a 30-second period, Reusable code off, a look-around window of 1, failure rate limiting, Keycloak nodes on NTP, and only settings that users' apps support. Why: HOTP codes stay valid until used, each extra window step widens acceptance by 60 s, a code must be accepted only once, and clock drift or unsupported settings lock users out. (Keycloak docs 26.8, p. 7; Keycloak IAM ch. 11; NIST SP 800-63B-4 §3.1.4.1, §3.1.4.2; OWASP ASVS 5.0, 6.5.5; HKM D9)
9. Make OTPs and recovery codes single use and random (CSPRNG, at least 20 bits of entropy, recovery codes at least 6 digits), store recovery codes hashed and salted, and invalidate out-of-band codes after 10 minutes. Why: they are bearer secrets that otherwise stay reusable after a leak. (NIST SP 800-63B-4 §3.1.2, §3.1.3.2; OWASP ASVS 5.0, 6.5.1, 6.5.3, 6.5.4)
10. Use biometrics only as the user verification of a passkey (never voice) and always offer a non-biometric option. Why: biometric traits are not secrets and matching is probabilistic. (NIST SP 800-63B-4 §3.2.3; Solving IdM ch. 12, 18; Zero Trust Networks ch. 6; HKM D56)
11. Keep an inventory of each user's authenticators, let users and administrators remove a lost or stolen one immediately and end the sessions opened with it, and re-verify the user's identity (institution IdP or help desk) before a replacement is bound. Why: a reported loss must stop the authenticator at once, a stolen unlocked device may still hold a session, and a caller who only claims a lost phone must not get a new factor. (NIST SP 800-63B-4 §3.2.1; CISA/NSA ESF IAM, Implementing MFA; Solving IdM ch. 12, 17; HKM D48)
12. In `hkm`, ask for the identifier first, resolve the organization from the email domain, and send the user straight to that organization's IdP without a chooser. Why: users of different institutions need different methods without seeing other institutions' options. (Cloud Native OAuth ch. 14; Solving IdM ch. 18; Learning Digital Identity ch. 6)
13. Keep `sub`, `preferred_username` and the claims contract identical whatever method, IdP or factor a user signs in with. Why: engines and Ranger identify users and their history by these values. (Cloud Native OAuth ch. 14; HKM D1)
14. Start registration with `prompt=create` and password reset with the `/forgot-credentials` path, never by linking to `login-actions` or `broker` endpoints. Why: only these entry points are supported. (Keycloak docs 26.8, p. 7)
15. Pilot MFA and step-up with users who represent the population, test failure paths (lost device, no enrolled factor, rejected code) as well as success, and write a recovery for each failure before enforcing. Why: usability problems found after rollout turn into help-desk load and workarounds. (Solving IdM ch. 12)
16. Give each person exactly one account per realm and each workload its own client service account, and never create shared accounts, administrator accounts and `master` emergency accounts included. Why: every action must trace back to one person or one system, and a shared account breaks the audit trail. (BG Rehberi 3.1.12.2, 3.2.1.2; KVKK Rehberi 3.1, p. 20-21; Identity Attack Vectors ch. 6, 8, 10, 13; decision 2026-11-18)
17. Use X.509 client certificate login only for a documented high-security case, and where it is used validate the full certificate path to a pinned trusted CA and check revocation through CRL or OCSP before accepting the certificate. Why: certificates are costly to manage and their public key becomes a correlatable identifier, an unvalidated chain lets any certificate authenticate, and an intact certificate may still carry a compromised key. (BG Rehberi 3.2.1.10; Learning Digital Identity ch. 9, 11)
18. Before adopting or changing an authentication method, rank the nine system properties for the platform (practicality, security level, locational transparency, integrability, privacy, reliability, auditability, manageability, federation support), score each option against them, and keep the comparison with the decision record. Why: a ranked property list makes the evaluation repeatable and explains the choice later. (Learning Digital Identity ch. 11)

Example:

```hcl
resource "keycloak_authentication_flow" "hkm_browser" {
  realm_id = keycloak_realm.hkm.id
  alias    = "hkm-browser"   # copy of the built-in "browser"; the original stays untouched
}

resource "keycloak_authentication_subflow" "hkm_browser_mfa" {
  realm_id          = keycloak_realm.hkm.id
  parent_flow_alias = keycloak_authentication_flow.hkm_browser.alias
  alias             = "hkm-browser-second-factor"
  requirement       = "REQUIRED"   # not CONDITIONAL
}

resource "keycloak_authentication_bindings" "hkm" {
  realm_id     = keycloak_realm.hkm.id
  browser_flow = keycloak_authentication_flow.hkm_browser.alias
}

resource "keycloak_realm" "hkm" {
  realm = "hkm"
  otp_policy {
    type              = "totp"
    algorithm         = "HmacSHA1"   # change only to what users' authenticator apps support
    digits            = 6
    period            = 30
    look_ahead_window = 1
    code_reusable     = false
  }
}
```

### Passkeys, WebAuthn and phishing resistance

Source: Keycloak 26.8.2 documentation, Server Admin: Configuring authentication; Keycloak – IAM for Modern Applications, ch. 11; NIST SP 800-63B-4 §2.2.2, §2.3.2, §3.1.7, §3.2.5, App. B; Cloud Native Data Security with OAuth, ch. 14 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 12; Learning Digital Identity, ch. 11; Zero Trust Networks, 2nd ed., ch. 6 (merged 2026-11-18).

1. Prefer passkeys to OTP as the second factor, and count only cryptographic authenticators bound to the verifier (WebAuthn origin binding) as phishing-resistant; typed OTP and out-of-band codes never qualify. Why: WebAuthn keeps no shared secret on the server, and any code the user types can be relayed by a phishing page. (Keycloak IAM ch. 11; NIST SP 800-63B-4 §3.2.5; Cloud Native OAuth ch. 14; Learning Digital Identity ch. 11)
2. Keep User Verification `required`, check the UV flag in every response, and let a passkey count as a complete login (the browser flow's `Condition - credential` skips 2FA after it) only on that basis; treat a response without UV as one factor. Why: only user verification makes a passkey multi-factor. (Keycloak docs 26.8, p. 7; NIST SP 800-63B-4 §3.1.7.2, App. B.3; Zero Trust Networks ch. 6; HKM D37)
3. Claim AAL3 only for a hardware-held, non-exportable key with user action on every login, never for a syncable passkey. Why: syncing makes the key exportable. (NIST SP 800-63B-4 §2.3.2, App. B.2; OWASP ASVS 5.0, 6.3.3; Zero Trust Networks ch. 6; HKM D23)
4. Use the Backup Eligible/Backup State flags and attestation only to restrict synced keys for workforce users, never to block `hkm-public` users. Why: demanding attestation from the public pushes them to weaker, phishable methods. (NIST SP 800-63B-4 App. B.3)
5. Require authentication with an existing method before a user registers a new passkey or OTP device, never with a password alone, and remove replaced passwords and OTP devices. Why: credential change is the account-takeover path, and password-only re-enrolment defeats MFA. (Cloud Native OAuth ch. 14; CISA/NSA ESF IAM, Implementing MFA)
6. Include RS256 in the passwordless policy's signature algorithms where Windows Hello must work. Why: that platform authenticator requires RS256. (Keycloak docs 26.8, p. 7)
7. Before choosing hardware security keys, budget for their durability and for replacing lost or broken keys, and keep spare keys so a user is not locked out while waiting. Why: replacement cost and delay can be substantial. (Solving IdM ch. 12)

Example:

```hcl
resource "keycloak_realm" "hkm" {
  realm = "hkm"
  web_authn_passwordless_policy {
    relying_party_entity_name         = "HKM"
    relying_party_id                  = "sso.hkm.example"
    user_verification_requirement     = "required"
    require_resident_key              = "Yes"
    attestation_conveyance_preference = "direct"
    signature_algorithms              = ["ES256", "RS256"]
  }
}
```

### Step-up authentication and levels of assurance

Source: Keycloak 26.8.2 documentation, Server Admin: Configuring authentication, Managing OpenID Connect and SAML clients; Keycloak – IAM for Modern Applications, ch. 4, 11; NIST SP 800-63B-4 §2, §5.1.2, §5.2; NIST SP 800-63C-4 §2.5; OWASP ASVS 5.0, 6.8.4, 7.5.1, 7.5.2, 10.3.4; Cloud Native Data Security with OAuth, ch. 6, 14 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 2, 8, 9, 11, 12, 13; API Security in Action, ch. 7; BG Rehberi 3.2.1.10; Identity Attack Vectors, 2nd ed., ch. 5, 12, 18, 19; OAuth 2 in Action, ch. 13, 14; Zero Trust Networks, 2nd ed., ch. 2, 4, 5, 6, 10 (merged 2026-11-18).

1. Map every ACR value to an AAL and never claim more than the weakest step of the flow that produced it. Why: the level is only as strong as its weakest factor. (NIST SP 800-63B-4 §2.1–2.3; OAuth 2 in Action ch. 13)
2. Request a higher level (for example `gold`) only for actions that need it, such as an administrator changing a role mapping, through a second code flow when an API refuses a request, instead of raising the level for every login or issuing high-privilege tokens up front. Why: the guide requires re-authentication or stronger authentication before sensitive operations, stronger credentials are asked only where needed, and a stolen normal token cannot perform high-privilege operations. (Keycloak IAM ch. 11; Cloud Native OAuth ch. 6, 14; Solving IdM ch. 2, 12; BG Rehberi 3.2.1.10; Identity Attack Vectors ch. 5, 12, 18; Zero Trust Networks ch. 2, 6, 10)
3. Do not treat a valid access or refresh token as proof that the user is present; let each relying party check `auth_time` and `acr` before sensitive actions and request `max_age` when it needs a fresh login. Why: tokens outlive the authentication, and the relying party decides how fresh is fresh enough. (NIST SP 800-63B-4 §5.1.2, §5.2; OWASP ASVS 5.0, 10.3.4; Solving IdM ch. 8, 9, 11; API Security in Action ch. 7; OAuth 2 in Action ch. 13; Identity Attack Vectors ch. 19)
4. Build step-up subflows in ascending level order, put only the minimum authenticators in the first level, make every level include all methods of the level below, and set Max Age 0 on the highest level so that elevated assurance lapses by expiry rather than through a separate step-down. Why: the first level always runs at first login, Max Age 0 forces fresh proof for each request, and short privileged sessions limit hijacking and mistakes. (Keycloak docs 26.8, p. 7; Solving IdM ch. 12)
5. Use a client policy with the ACR condition and the authentication flow selector executor when a level needs a different method rather than an added one. Why: level-of-authentication conditions assume each level is a superset of the one below. (Keycloak docs 26.8, p. 7)
6. Enforce a client's minimum assurance with Minimum ACR Value, not Default ACR Values. Why: a client can lower a default by sending its own `acr_values`. (Keycloak docs 26.8, p. 11)
7. When an upstream IdP signals no `acr`, `amr` or AAL, treat the login as password-only and step up in Keycloak. Why: the relying party cannot see how the upstream user authenticated. (NIST SP 800-63C-4 §2.5; OWASP ASVS 5.0, 6.8.4)
8. Require full reauthentication before a user changes email, phone, MFA or recovery data, and before they view and end their other sessions. Why: a hijacked session must not be able to take over the account. (OWASP ASVS 5.0, 7.5.1, 7.5.2)
9. Tell users briefly, before they start an action, that it will ask for a stronger login, and do not promise a single login for everything when clients need different levels. Why: an unexpected second prompt confuses users. (Solving IdM ch. 12)
10. Make logout end the whole Keycloak session whatever level it reached, and say so in the user guidance. Why: users need to know that a stepped-up session does not survive logout. (Solving IdM ch. 13)
11. Base every government-facing login integration (e-Devlet brokering into `hkm-public`, institution IdPs brokered into `hkm`) on a named, published OIDC assurance profile, record which profile it follows, and plan the integration for a long service life. Why: profiles narrow OAuth's many options for interoperability and security, and government systems keep a protocol for many years. (OAuth 2 in Action ch. 14)
12. When an OpenSearch detection rule flags a user (a return after long inactivity, logins whose locations cannot both be true in the time between them, a burst of failed attempts), answer by requiring a fresh `gold` login for sensitive clients or by ending the user's sessions, never by silently allowing; never let a location or a score decide access on its own, and do not show users a live risk score. Why: lowered trust should cost more proof rather than flip access on or off, location and learned scores are unreliable alone, and a visible score tells an attacker whether they are gaining trust. (Zero Trust Networks ch. 2, 4, 5, 6; HKM D2, D49)

Example:

```hcl
resource "keycloak_realm" "hkm" {
  realm      = "hkm"
  attributes = { "acr.loa.map" = jsonencode({ silver = 1, gold = 2 }) }
}

resource "keycloak_openid_client" "superset" {
  realm_id     = keycloak_realm.hkm.id
  client_id    = "superset"
  extra_config = { "minimum.acr.value" = "silver" }   # silver in hkm = password + second factor; admin views redirect with acr_values=gold
}
```

### Sessions, timeouts and logout

Source: Keycloak 26.8.2 documentation, Server Admin: Managing user sessions, Configuring authentication, Managing OIDC and SAML clients, Upgrading Guide; Keycloak – IAM for Modern Applications, ch. 12, 14; NIST SP 800-63B-4 §5; OWASP ASVS 5.0 V7; Cloud Native Data Security with OAuth, ch. 2, 13, 14 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 2, 9, 11, 13, 17, 18; API Security in Action, ch. 3, 4, 5, 9; BG Rehberi 3.2.2.1–3.2.2.5; KVKK Rehberi 3.1; Learning Digital Identity, ch. 11, 13 (merged 2026-11-18).

1. Keep a session table per realm, assurance level and client: every layer a login creates (application session, Keycloak SSO session, institution IdP session), the inactivity and absolute limit of each, the concurrent-session limit, and for each termination cause (logout, timeout, administrator sign-out, disabling, restart, cleared cookies) which layers it ends; justify any deviation from NIST SP 800-63B-4. Why: no single timeout fits every application, each layer runs its own timers, and an undocumented layer stays alive after the user thinks the session is gone. (OWASP ASVS 5.0, 7.1.1–7.1.3; NIST SP 800-63B-4 §5.2; Solving IdM ch. 2, 9, 13)
2. Keep Client Session Idle and Max, realm-wide or per client, equal to or shorter than the realm SSO values, and override them per client only to shorten. Why: application sessions must not outlive the login that created them, and since 26.5 longer values fail validation. (Keycloak docs 26.8, p. 5, p. 11, p. 17; Keycloak IAM ch. 12, 14; HKM D10)
3. Let activity reset only the inactivity timer, let reauthentication reset both timers, and count as Keycloak activity only requests that reach Keycloak (logins and token refreshes), never clicks inside an application. Why: periodic reauthentication confirms the user is still there, and Keycloak does not see in-application activity. (NIST SP 800-63B-4 §5.2; Solving IdM ch. 9; HKM D55)
4. Keep Remember Me off in every realm, and never let a session carry more assurance than the login that created it. Why: positive Remember Me timeouts extend idle and max, and a remembered browser must not stand in for authentication. (Keycloak docs 26.8, p. 5; NIST SP 800-63B-4 §5.1; HKM D62)
5. Limit administrators to one concurrent session with the User session count limiter, added as a required step at the end of the browser flow (inside an alternative subflow, not at top level), direct grant, reset credentials and post broker login flows; leave workforce users unlimited but let them see and end their sessions in the account console of `hkm` and `hkm-public`. Why: Keycloak does not keep these configurations consistent, a top-level step would count SSO re-entry, and users must be able to spot a foreign session. (Keycloak docs 26.8, p. 7; Cloud Native OAuth ch. 14; BG Rehberi 3.2.2.5; HKM D63)
6. Investigate users with many parallel user sessions. Why: one browser holds one user session with many client sessions, so parallel sessions come only from other devices, browsers or cleared cookies. (Keycloak IAM ch. 12)
7. Issue a new random session secret from a cryptographically secure generator at every authentication and reauthentication (at least 128 bits for reference tokens), accept only identifiers the server issued, keep session identifiers and tokens out of URLs, error messages and logs, end the old secret and clear server-side session state, client storage and cookies at logout, and make server-side applications such as `superset` and `airflow` rotate their own session ID after the OIDC callback; verify session tokens only on a trusted backend. Why: this prevents session fixation, guessing, takeover through leaked identifiers and forged client-side tokens, and an application session planted before login would otherwise inherit the user's login. (NIST SP 800-63B-4 §5.1; OWASP ASVS 5.0, 7.2.1–7.2.4; API Security in Action ch. 4, 5; BG Rehberi 3.2.2.1, 3.2.2.2, 3.2.2.3; Learning Digital Identity ch. 11)
8. Serve Keycloak and every application only over HTTPS, refuse plain HTTP on API endpoints instead of redirecting, and redirect browser hosts to HTTPS with HSTS; make application session cookies Secure, HttpOnly, opaque, free of personal data and without a `Domain` attribute, named with the `__Host-` prefix (which implies `Path=/` and no `Domain`) for the application session cookie, and a restrictive, separate `Path` only for token-handler cookies that cannot use that prefix, `SameSite=Strict` where possible and `Lax` only where the OIDC redirect requires it, and no `Max-Age` or `Expires`, with idle and absolute lifetimes enforced on the server; this binds the applications, because Keycloak's own cookies carry no `__Host-` prefix and are set with SameSite=None. Why: a stolen cookie or a credential sniffed on plain HTTP is a stolen session, these attributes keep the bearer secret away from scripts, cross-site requests and clear text, and browsers may restore persistent cookies after the session should have ended. (Keycloak IAM ch. 12; NIST SP 800-63B-4 §5.1, §5.1.1; Cloud Native OAuth ch. 13; API Security in Action ch. 3, 4; BG Rehberi 3.2.2.4; HKM D44; decision 2026-11-18; research: Keycloak 26.8.2 documentation and source, 2026-11-18)
9. Make logout and expiry stop all further use, including of self-contained tokens, through short access-token lifetimes, refresh-token revocation and the realm not-before time, and use back-channel logout instead of iframe or third-party-cookie session checks. Why: a JWT otherwise stays valid until it expires, and browsers no longer send third-party cookies reliably. (OWASP ASVS 5.0, 7.4.1; Cloud Native OAuth ch. 2, 14; Solving IdM ch. 13; HKM D10)
10. End all of a user's sessions when the account is disabled or deleted, when the password or authentication methods change, and when the user's group memberships or roles change; sign out a user disabled for cause at once instead of only disabling them, and let administrators end sessions per user or realm-wide. Why: old sessions must not keep old rights, leavers and compromised credentials must lose access at once, and a disabled account alone stops access only when an application next contacts Keycloak. (OWASP ASVS 5.0, 7.4.2, 7.4.3, 7.4.5; Cloud Native OAuth ch. 14; Solving IdM ch. 13; BG Rehberi 3.2.2.3; KVKK Rehberi 3.1, p. 21)
11. Set Keycloak's SSO lifetimes independently of institution IdP sessions, and propagate logout to an institution IdP by back-channel logout where it supports it. Why: an upstream session is outside the platform's control. (Cloud Native OAuth ch. 14; Solving IdM ch. 13; Learning Digital Identity ch. 13; HKM D41)
12. Give every authenticated page of every application a visible logout that is itself CSRF-protected, ends the server-side session and expires the cookie; never change state on a GET request; and require a session-bound anti-CSRF value, compared in constant time, on every state-changing request, keeping the framework's CSRF protection on in Superset and Airflow. Why: users must be able to end a session and a surviving cookie copy must stop working, cookies and SameSite alone do not stop CSRF, and an early-exit comparison leaks the value by timing. (NIST SP 800-63B-4 §5.1; API Security in Action ch. 4; BG Rehberi 3.2.2.3)
13. Serve every web UI with a strict Content Security Policy (`default-src 'none'`, scripts only from self, no `unsafe-inline`, an explicit `connect-src` list) and restrict framing of Keycloak login pages to the same origin. Why: it blocks foreign scripts and exfiltration, and framing enables clickjacking of login and consent screens. (Cloud Native OAuth ch. 13; API Security in Action ch. 5)
14. Keep User-Initiated Action Lifespan short, and set Admin-Initiated Action Lifespan only as long as onboarding emails need. Why: action links act as credentials until they expire. (Keycloak docs 26.8, p. 5; Solving IdM ch. 17; API Security in Action ch. 9)
15. In `hkm`, make every application logout an RP-initiated logout that ends the Keycloak SSO session and lets Keycloak notify the other clients of that session by back-channel logout, and tell users that logging out of one platform application logs them out of all. Why: workforce policy expects one logout to end all access, users forget per-application logouts, and a global logout can interrupt work open elsewhere. (Solving IdM ch. 11, 13; HKM D10)
16. Test logout early and across every client that shares one SSO session, including a back-channel delivery that fails. Why: multi-party logout usually takes longer to design and test than login. (Solving IdM ch. 13)
17. For shared workstations and kiosks, require an individual login for each session, give their clients a shorter Client Session Idle than the realm, keep Remember Me off, and use browser sessions that are discarded at sign-out. Why: users walk away without logging out, and temporary browser data can leak to the next user. (Solving IdM ch. 18; HKM D62)

Example:

```hcl
resource "keycloak_realm" "hkm" {
  realm                       = "hkm"
  sso_session_idle_timeout    = "30m"
  sso_session_max_lifespan    = "10h"
  client_session_idle_timeout = "30m"   # never above the SSO values
  client_session_max_lifespan = "10h"
  remember_me                 = false

  security_defenses {
    headers {
      content_security_policy = "frame-src 'self'; frame-ancestors 'self'; object-src 'none';"
      x_frame_options         = "SAMEORIGIN"
    }
  }
}

resource "keycloak_openid_client" "superset_kiosk" {
  realm_id                            = keycloak_realm.hkm.id
  client_id                           = "superset-kiosk"
  access_type                         = "CONFIDENTIAL"
  client_authenticator_type           = "client-jwt"
  standard_flow_enabled               = true
  pkce_code_challenge_method          = "S256"
  client_session_idle_timeout         = "5m"    # shorter than the hkm SSO idle of 30m
  backchannel_logout_url              = "https://superset-kiosk.hkm.example/backchannel-logout"
  backchannel_logout_session_required = true
  valid_post_logout_redirect_uris     = ["https://superset-kiosk.hkm.example/signed-out"]
}
```

```python
# superset_config.py: the application session after the Keycloak login
SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"      # Lax because the OIDC callback is a cross-site redirect (HKM D44)
SESSION_COOKIE_DOMAIN = None         # host-only cookie for superset.hkm.example
WTF_CSRF_ENABLED = True              # framework CSRF protection stays on
# lifetime enforced on the server, never longer than the hkm SSO session (30 min idle, 10 h max)
```

### Brute force, passwords and recovery

Source: Keycloak 26.8.2 documentation, Server Admin: Mitigating security threats, Configuring authentication; Keycloak – IAM for Modern Applications, ch. 11, 14; NIST SP 800-63B-4 §3.1.1, §3.2.2; OWASP ASVS 5.0, 6.1–6.4; Cloud Native Data Security with OAuth, ch. 14 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 2, 4, 12, 14, 17; API Security in Action, ch. 3; BG Rehberi 3.1.12.3, 3.1.12.4, 3.1.12.16, 3.2.1.1, 3.2.1.4–3.2.1.6, 3.2.1.8–3.2.1.11; KVKK Rehberi 3.1; Identity Attack Vectors, 2nd ed., ch. 10, 11, 12, 13, 14, 21; Learning Digital Identity, ch. 11, 14, 16; Zero Trust Networks, 2nd ed., ch. 4, 6 (merged 2026-11-18).

1. Enable brute force detection in `master`, `hkm` and `hkm-public` in temporary lockout mode with increasing waits, add IP-based rate limiting at the ingress, implement a permanent lock as an administrator disabling the user so that clearing a temporary lockout never re-enables that user, and document the anti-automation design. Why: detection is off by default, automatic permanent lockout lets attackers lock known accounts, and a permanent lock must survive the end of a temporary one. (Keycloak docs 26.8, p. 13; Keycloak IAM ch. 14; NIST SP 800-63B-4 §3.2.2; OWASP ASVS 5.0, 6.1.1, 6.3.1; Solving IdM ch. 14, 17; API Security in Action ch. 3; BG Rehberi 3.1.12.3, 3.2.1.1, 3.2.1.9; KVKK Rehberi 3.1, p. 20-21; Identity Attack Vectors ch. 10, 11; HKM D30)
2. Count second-factor failures too: set Maximum Secondary Authentication Failures to a non-zero value of at most 100. Why: it is off by default and protects OTP once the password is known. (Keycloak docs 26.8, p. 13; NIST SP 800-63B-4 §3.2.2)
3. Keep Failure Reset Time longer than Max Wait. Why: otherwise the effective wait never reaches the configured maximum. (Keycloak docs 26.8, p. 13)
4. Set the password policy when the realm is created: at least 15 characters when the password is the only factor and at least 8 only where MFA is always enforced, a maximum of at least 64 verified without truncation so that long passphrases fit, no composition rules, hints or security questions, not the username or email, and a denylist; require the user to confirm a new password by entering it twice, and push existing users to the policy with the Update Password required action. Why: length, not composition, drives resistance to guessing, and a new policy applies only to new passwords. (Keycloak docs 26.8, p. 7, p. 13; Keycloak IAM ch. 11, 14; NIST SP 800-63B-4 §3.1.1.1, §3.1.1.2, App. A.3; OWASP ASVS 5.0, 6.2.1, 6.2.4, 6.2.5, 6.2.8, 6.2.9, 6.2.11, 6.2.12, 6.4.2, 6.4.3; Solving IdM ch. 12, 14; BG Rehberi 3.2.1.8; KVKK Rehberi 3.1, p. 20-21; Identity Attack Vectors ch. 10; HKM D6)
5. Set a long maximum password age (365 days suggested) for local passwords and force a change on evidence of compromise or a breached-password hit. Why: BG Rehberi requires a maximum validity period, while NIST makes compromise, not the calendar, the real trigger. (BG Rehberi 3.2.1.8; NIST SP 800-63B-4 §3.1.1.2; Solving IdM ch. 14; Identity Attack Vectors ch. 10; Learning Digital Identity ch. 11; Zero Trust Networks ch. 4; HKM D5)
6. Fill the denylist with breached, dictionary and context words (`hkm`, `hkm-public`, tenant aliases such as `t-acme`, the username and email), at least the 3000 most common passwords, tell the user why a password was rejected, replace the file atomically (write a temporary file, then rename), and pre-compute a `.bloom` file for large lists. Why: the denylist stops the first guesses, a half-written file can be read during reload, and large plaintext lists slow startup. (NIST SP 800-63B-4 §3.1.1.2; OWASP ASVS 5.0, 6.2.4, 6.2.11, 6.2.12; Keycloak docs 26.8, p. 7; Solving IdM ch. 14, 17; Identity Attack Vectors ch. 10, 21)
7. Allow password managers, autofill and paste on login and password-change forms. Why: managers make long unique passwords practical. (NIST SP 800-63B-4 §3.1.1.2; Learning Digital Identity ch. 11; Zero Trust Networks ch. 6)
8. Keep Keycloak's default password hashing (Argon2; PBKDF2-SHA512 under FIPS) and its default iteration count (`-1`), never lower the cost for speed, and accept credentials only over TLS. Why: the hash is salted and cost-tuned against offline guessing, higher values cost CPU, and a changed algorithm rehashes only at the next login. (Keycloak docs 26.8, p. 7; Keycloak IAM ch. 11; NIST SP 800-63B-4 §3.1.1.2; API Security in Action ch. 3; BG Rehberi 3.1.12.16, 3.2.1.4; Learning Digital Identity ch. 11; Zero Trust Networks ch. 6)
9. Configure Maximum Authentication Age on the Update Password required action, not in the password policy. Why: the policy variant may be deprecated or removed. (Keycloak docs 26.8, p. 7)
10. Never let administrators type a user's password; send an execute-actions email so the user sets the first password at first login, and never send a password by email. Why: the administrator must not know the user's password, and the first password must be changed on first use. (Keycloak IAM ch. 11; OWASP ASVS 5.0, 6.4.6; Solving IdM ch. 4; BG Rehberi 3.2.1.4, 3.2.1.5; Identity Attack Vectors ch. 10; Learning Digital Identity ch. 16; HKM D8)
11. Make every recovery path (forgotten password, lost passkey, expired password, help desk) at least as strong as the primary login and as the original identity binding: back it with a factor of a different type than the lost one, proven through a channel registered before the loss, never reveal the current password, and never let password reset bypass the second factor. Offer the forgot-password link only in `hkm-public`, where email is verified, add a second factor to that reset, make its link unguessable, single use and short-lived, keep the current password valid until the link is used, and confirm every completed reset by notice; workforce users recover through their institution IdP or help-desk re-verification. Why: recovery is the usual route around strong login, a mailbox alone is weak proof, and a reset request must not lock out the real owner. (Keycloak IAM ch. 11; OWASP ASVS 5.0, 6.4.2, 6.4.3; Cloud Native OAuth ch. 14; Solving IdM ch. 2, 14, 17; BG Rehberi 3.2.1.10, 3.2.1.11; Identity Attack Vectors ch. 10; Learning Digital Identity ch. 11, 14; Zero Trust Networks ch. 6; HKM D7, D48)
12. Let every user register a primary and at least one backup method: two passkeys for administrators, and recovery codes only as an alternative second factor next to a passkey or OTP for `hkm-public` users (with email). Why: a second method keeps the account usable when one is lost or compromised, and recovery codes back up a lost device without being a primary factor. (Keycloak docs 26.8, p. 7; CISA/NSA ESF IAM, Implementing MFA; Solving IdM ch. 12, 14, 17; HKM D48)
13. Rate-limit at the ingress source addresses that fail logins across many accounts, but first find systems that still log in with stored, outdated user passwords and exempt the known NAT egress addresses of institutions, keeping the per-user temporary lockout for them. Why: shared NAT addresses and stale stored passwords cause false alarms that would block a whole institution. (Solving IdM ch. 17; HKM D30)
14. Do not build credential reset into platform applications: link `hkm-public` applications to Keycloak's `/forgot-credentials` entry and send `hkm` users to their institution IdP's reset. Why: applications that delegate authentication hold no credentials, and a second reset path escapes the controls of the first. (Solving IdM ch. 14; HKM D48)
15. Prefer self-service recovery; where the help desk must re-verify a user, follow a written procedure that never asks for secrets the user may reuse elsewhere, accepts an official identity document in person, and records every step; never change an email address, a second factor or an access-driving attribute on the strength of an emailed request alone, and give no help-desk or institution-administrator role the right to reset or remove an administrator's credentials. Why: remote help-desk questions expose data that can be replayed at other sites, attackers talk help desks into removing MFA or enrolling their own device, and falsified details succeed when nobody validates them. (Solving IdM ch. 14, 17; Identity Attack Vectors ch. 12, 14; HKM D48, D57)
16. Notify the user of every credential, email or profile change, send the notice for an email change to the previous address as well, and tell the user what to do if they did not make the change. Why: notices reveal mistakes and give early warning of account takeover. (Solving IdM ch. 14, 17)
17. Give the same answer, with no telling difference in content or timing, for an unknown user, a wrong password and a locked account in login, reset and recovery forms. Why: different answers let attackers enumerate accounts and confirm success with automated tools. (BG Rehberi 3.2.1.9)
18. Remove or change every default account and password in Keycloak and every component it depends on before production, including the `master` bootstrap admin; keep only the named personal break-glass accounts (at least two operators, each carrying the HKM D65 controls), monitor them, and rotate each password in OpenBao after every use. Why: default credentials are a common entry point and are forbidden on any asset, and personal emergency accounts keep every emergency action traceable to one person. (BG Rehberi 3.1.12.4, 3.2.1.6; Identity Attack Vectors ch. 6, 10, 13; HKM D32, D65; decision 2026-11-18)

Example (lockout numbers are the 26.8.2 defaults; HKM values still to be set):

```hcl
resource "keycloak_realm" "hkm_public" {
  realm                  = "hkm-public"
  reset_password_allowed = true
  verify_email           = true
  password_policy        = "length(15) and maxLength(64) and notUsername and notEmail and passwordBlacklist(hkm-denylist.txt) and forceExpiredPasswordChange(365)"

  security_defenses {
    brute_force_detection {
      permanent_lockout                = false
      max_login_failures               = 30
      wait_increment_seconds           = 60
      quick_login_check_milli_seconds  = 1000
      minimum_quick_login_wait_seconds = 60
      max_failure_wait_seconds         = 900
      failure_reset_time_seconds       = 43200   # 12 h, above max wait
    }
  }
}
```
