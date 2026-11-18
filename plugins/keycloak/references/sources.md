# Sources behind v1 (checked 2026-11-15)

## Keycloak project
- Release posts: [26.8.2 (2026-11-05)](https://www.keycloak.org/2026/11/keycloak-2682-released) · [26.8.0 (2026-10-01)](https://www.keycloak.org/2026/10/keycloak-2680-released) · [26.7.5](https://www.keycloak.org/2026/09/keycloak-2675-released) · [26.7.0](https://www.keycloak.org/2026/07/keycloak-2670-released) · [26.6.0](https://www.keycloak.org/2026/04/keycloak-2660-released) · [26.5.0](https://www.keycloak.org/2026/01/keycloak-2650-released) · [26.2.0](https://www.keycloak.org/2025/04/keycloak-2620-released)
- Release notes and upgrading guide: [26.8.0 release notes](https://www.keycloak.org/docs/26.8.0/release_notes/) · [Upgrading Keycloak 26.8.0](https://www.keycloak.org/docs/26.8.0/upgrading/)
- Organizations: [announcement](https://www.keycloak.org/2024/06/announcement-keycloak-organizations) · [organization groups (2026-04)](https://www.keycloak.org/2026/04/org-groups)
- [Fine-grained admin permissions v2 (26.2)](https://www.keycloak.org/2025/05/fgap-kc-26-2) · [Standard token exchange (26.2)](https://www.keycloak.org/2025/05/standard-token-exchange-kc-26-2) · [DPoP supported (26.4)](https://www.keycloak.org/2025/10/dpop-support-26-4) · [Passkeys (26.4)](https://www.keycloak.org/2025/09/passkeys-support-26-4)
- High availability: [sizing CPU and memory](https://www.keycloak.org/high-availability/single-cluster/concepts-memory-and-cpu-sizing) · [26.4 benchmark](https://www.keycloak.org/2025/10/keycloak-benchmark) · [multi-cluster with the Operator](https://www.keycloak.org/high-availability/multi-cluster/deploy-keycloak-kubernetes)
- Operator: [basic deployment](https://www.keycloak.org/operator/basic-deployment) · advanced configuration, rolling updates, realm import (keycloak/keycloak `docs/guides/operator`, main branch 2026-11-13)
- Server guides used: database (supported PostgreSQL 14–18), hostname, reverse proxy, management interface, features, bootstrap-admin recovery, supported configurations (keycloak/keycloak `docs/guides/server`)
- [Realm configuration management tools survey (2024-09)](https://www.keycloak.org/2024/09/realm-config-management-tools-survey-results) · [Keycloak adopts the Terraform provider (2024-12)](https://www.keycloak.org/2024/12/terraform-provider-adoption)
- Terraform provider: keycloak/terraform-provider-keycloak v5.10.0 docs (`organization`, `openid_client`, `openid_group_membership_protocol_mapper`, `realm`, `kubernetes_identity_provider`)

## Secondary analyses (used with care; facts cross-checked with release posts where possible)
- [Keycloak 26.8 upgrade checklist (WZ-IT)](https://wz-it.com/en/blog/keycloak-26-8-upgrade-guide/)
- [Keycloak Advisory Watch: fixes 26.7.4–26.8.0](https://buttondown.com/keycloak-advisory-watch/archive/keycloak-security-fixes-2674-to-2680-the-catch-up/)
- [How many realms can one Keycloak cluster handle (Skycloak, 26.6)](https://skycloak.io/blog/keycloak-realm-scaling-how-many-realms-per-cluster/)
- [CNCF: prepare your application landscape for zero trust with Keycloak 26.2](https://www.cncf.io/blog/2025/04/24/prepare-your-application-landscape-for-zero-trust-with-keycloak-26-2/)

## Platform components
- Trino 484 documentation: `security/oauth2`, `security/jwt`, `security/user-mapping` (offline copy `trino-docs-html.zip`)
- [Airflow Keycloak provider 0.12.0 changelog](https://airflow.apache.org/docs/apache-airflow-providers-keycloak/stable/changelog.html) · [configuration reference](https://airflow.apache.org/docs/apache-airflow-providers-keycloak/stable/configurations-ref.html)
- [strimzi-kafka-oauth](https://github.com/strimzi/strimzi-kafka-oauth) · [Streams for Apache Kafka 3.1 – KafkaListenerAuthenticationOAuth](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.1/html/streams_for_apache_kafka_api_reference/type-kafkalistenerauthenticationoauth-reference)
- [OpenBao: Keycloak OIDC provider configuration](https://openbao.org/docs/auth/jwt/oidc-providers/keycloak/)
- [MinIO AIStor: Keycloak identity provider](https://docs.min.io/aistor/administration/iam/identity/keycloak-identity/)
- [OpenMetadata custom OIDC SSO](https://docs.open-metadata.org/v2.1.x-SNAPSHOT/how-to-guides/admin-guide/sso/custom-oidc)
- [Argo CD Keycloak integration](https://argo-cd.readthedocs.io/en/stable/operator-manual/user-management/keycloak/)

## Turkish context
- [Bilgi ve İletişim Güvenliği Rehberi, sürüm 1.1 (Siber Güvenlik Başkanlığı, 01.03.2026)](https://cdn.siberguvenlik.gov.tr/public/docs/bg_rehber.pdf) — sections 3.1.8, 3.1.12, 3.2.1, 3.2.2, 3.2.3
- [TÜBİTAK BİLGEM YTE – Kimlik Doğrulama Altyapılarının Kullanımı (2023)](https://bilgem.tubitak.gov.tr/wp-content/uploads/sites/8/kimlik_dogrulama_altyapilarinin_kullanimi.pdf) — CAS vs Keycloak comparison for public institutions
