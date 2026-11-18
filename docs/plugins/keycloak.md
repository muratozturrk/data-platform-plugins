# keycloak

> Keycloak 26.8'i HKM açık kaynak veri platformunun kimlik sağlayıcısı (IdP) olarak tasarlamak, kurmak ve işletmek için skill, agent ve örnekler: çok kurum için realm ve organization tasarımı, Operator ve CloudNativePG ile Kubernetes kurulumu, LDAP/AD federasyonu ve kurum IdP'lerine brokering, kimlik doğrulama güvencesi (passkey, MFA, step-up), OAuth 2.0/OIDC istemci profili, Ranger/Trino/Superset/Airflow/Kafka'nın kullandığı claims sözleşmesi, FGAP v2 ile yetki devri, Terraform ile configuration as code, OpenSearch/immudb denetimi, KVKK ve yükseltmeler.

| | |
| --- | --- |
| Sürüm | 0.1.0 |
| Kategori | platform |
| Kaynak | `plugins/keycloak/` |
| Kaynak materyal | Keycloak 26.x dokümanları ve sürüm notları (güncel: 26.8.2, 2026-11-05), platform bileşenlerinin entegrasyon dokümanları, RFC 9700, OAuth 2.1 taslağı, NIST SP 800-63-4, OWASP ASVS 5.0, Bilgi ve İletişim Güvenliği Rehberi 1.1, KVKK, kitap özetleri (`references/sources.md`, `references/standards.md`, `references/books.md`) |

## 1. Ne işe yarar?

Keycloak, platformdaki tüm kullanıcıların ve iş yüklerinin kimliğini doğrulayan ve token üreten bileşendir. Bu plugin, Keycloak'ı HKM platformunda tek kimlik sağlayıcı olarak kurmak için gereken kararları ve yapılandırmayı içerir: kurumların nasıl modelleneceği, kurum kullanıcılarının nereden geleceği, girişin ne kadar güçlü olacağı, token'larda hangi claim'lerin bulunacağı ve diğer bileşenlerin bu kimliği nasıl kullanacağı.

Plugin şu platform kararlarını varsayar:
- **Realm yapısı:** Çalışanlar için tek realm `hkm`, içinde **kurum başına bir organization** (alias = tenant id, ör. `t-acme`). Açık veri portalı kullanıcıları için `hkm-public`. `master` yalnızca kurulum ve break-glass için. Kuruma ayrı realm yalnızca yazılı bir izolasyon gereksinimi varsa açılır.
- **Keycloak kimliği doğrular, veri erişimine Ranger karar verir.** Secret erişimine OpenBao karar verir. Keycloak Authorization Services yalnızca Airflow'un Keycloak auth manager'ında uygulama yetkisi için kullanılır.
- **Gruplar düz ve ortak adlı:** `platform_{kurum}_{rol}`, Ranger'da da aynı adla. `groups` claim'inde yol (path) olmadan gönderilir. `preferred_username` her yerde principal adıdır ve değiştirilemez.
- **Yapılandırma koddur:** Kurum başına Terraform modülü, Git'ten uygulanır. Üretimde konsoldan değişiklik yalnızca break-glass'tır ve 24 saat içinde Terraform'a geri yazılır.
- **Olaylar ve admin olayları** OpenSearch'e (arama) ve immudb'ye (kurcalama kanıtı) gider; Keycloak'ta tutulan kişisel veri en aza indirilir.

Plugin, `apache-ranger` ile birlikte çalışacak şekilde yazıldı: grup ve kullanıcı adları ortaktır, Ranger'daki `sync_groups_keycloak.py` grupları buradan alır. Tenant manifest'i `platform-engineering` plugin'inden gelir.

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install keycloak@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/keycloak`

Gereksinimler:
- Skill, agent ve komutlar için ek bir şey gerekmez.
- `examples/automation/*.py` için Python 3 ve `pyyaml`; testler için `pytest`. Script'ler çevrimdışı çalışır (realm export, token veya manifest dosyası alır).
- Terraform örnekleri için Terraform veya OpenTofu ve `keycloak/keycloak` provider v5.x.
- Kurulum örnekleri için Kubernetes, Keycloak Operator ve CloudNativePG.

## 3. İçerik

### Skill'ler

| Skill | Ne zaman devreye girer | İçerdiği konular |
| --- | --- | --- |
| `keycloak-architecture-tenancy` | IdP tasarımına başlarken, yeni bir kullanıcı kitlesi eklerken, bir kurumun ayrı realm'e ihtiyacı olup olmadığına karar verirken | Realm / organization / grup seçimi, kimlik alanları (çalışanlar, platform operatörleri, portal kullanıcıları, makineler), `master` realm, adlandırma, izolasyon garantileri ve sınırları, Keycloak ile Ranger/OpenBao/uygulama arasındaki sınır |
| `keycloak-deployment-kubernetes` | Kurulum, boyutlandırma, kurulum incelemesi | Operator ve Keycloak CR (`k8s.keycloak.org/v2beta1`), provider ve temalı optimize imaj, CloudNativePG ile PostgreSQL, hostname/proxy/TLS, management arayüzü (health, metrics), giriş ve token oranından boyutlandırma, zonlar arası tek küme HA, multi-cluster v2, rolling ve recreate güncelleme, network policy |
| `keycloak-identity-federation` | Bir kurumun dizinini veya IdP'sini bağlamak, kullanıcı özniteliklerini tasarlamak, hesap kapatmayı ele almak | LDAP/AD federasyonu ile OIDC/SAML brokering karşılaştırması, organization domain'leri ve identity-first yönlendirme, first-broker-login ve hesap bağlama, IdP mapper'ları, declarative user profile, SCIM, Workflows, işe giriş/görev değişikliği/ayrılış, portal kullanıcıları için e-Devlet veya sosyal giriş |
| `keycloak-authentication-assurance` | Giriş akışı, MFA politikası, oturum süreleri tasarlamak, denetimde kimlik doğrulama sorusu yanıtlamak | Authentication flow'lar, passkey/WebAuthn, OTP, ACR/LoA ile koşullu ve step-up doğrulama, brute-force koruması, yerel hesap parola politikası, SSO ve client oturum süreleri, NIST SP 800-63B-4 AAL, ASVS 5.0 ve BG Rehberi eşlemesi |
| `keycloak-clients-and-tokens` | İstemci kaydetmek, token incelemek, bir iş yükünün nasıl kimlik doğrulayacağına karar vermek | İstemci tipleri (backend'li web uygulaması, BFF ile SPA, CLI, resource server, servis/job, otomasyon), PKCE S256, `private_key_jwt`, Kubernetes service account token'ıyla federated client authentication, secret rotasyonu, DPoP, scope ve audience, token ömürleri, refresh token rotasyonu, token exchange (RFC 8693), client policy'ler, back-channel logout |
| `keycloak-claims-and-authorization` | Grup/rol tasarlamak, claim eklemek, token incelemek, kuruma Keycloak'ta yönetici yetkisi vermek | Platform claims sözleşmesi (`sub`, `preferred_username`, `groups`, `organization`, `acr`, `aud`), Ranger ile ortak grup ve rol modeli, Keycloak'ın ve Ranger/OpenBao'nun neye karar verdiği, ABAC öznitelikleri, FGAP v2 ve organization admin rolleriyle yetki devri, impersonation kontrolleri, 26.8 group claim değişikliği |
| `keycloak-platform-integrations` | Bir bileşeni Keycloak'a bağlamak, giriş veya claim uyuşmazlığını çözmek | Trino, Superset, Airflow 3, Strimzi Kafka, Ranger, OpenBao, MinIO AIStor/ObjectScale, OpenMetadata, Grafana, OpenSearch Dashboards, Nessie ve Iceberg REST istemcileri, CKAN, WSO2, Kubernetes API ve Argo CD, Spark/Flink job'ları, Flowable için tarifler |
| `keycloak-config-as-code` | Keycloak değişikliklerini otomatikleştirmek, kurumu CI ile almak, Terraform değişikliği incelemek | `keycloak/keycloak` provider v5.x ile Terraform/OpenTofu, Tenant manifest'inden kurum modülü, realm taban modülü, OpenBao'da secret (write-only argümanlar), realm import yalnızca bootstrap için, `KeycloakOIDCClient` CR (preview), keycloak-config-cli alternatifi, drift tespiti, ortamlar arası terfi, `kc-admin-api` scope'lu yönetim istemcisi |
| `keycloak-audit-compliance` | Denetim tasarlamak, denetçiye cevap vermek, KVKK talebi hazırlamak, erişim gözden geçirme yapmak | Kullanıcı ve admin olayları, event listener ve JSON log'ların OpenSearch'e gönderimi, immudb ile kurcalama kanıtı, saklama, KVKK (en aza indirme, silme talepleri), erişim gözden geçirme, BG Rehberi 1.1, NIST SP 800-63-4, ASVS 5.0 ve ISO/IEC 27001 kontrol eşlemesi |
| `keycloak-operations-upgrades` | Üretimde işletmek, yükseltmek, sorun gidermek | SLI/SLO ve Prometheus alarmları, arıza modları (veritabanı, cache, IdP kesintisi, anahtar rotasyonu, saat kayması, hesap kilitlenmesi), yedekleme/geri yükleme ve DR tatbikatı, imzalama anahtarı ve client secret rotasyonu, CVE takibi, patch ile minor yükseltme farkı, 26.8 kırıcı değişiklik listesi, Keycloak Benchmark ile yük testi, runbook'lar |

Her skill'in `references/` klasöründe kitap özetlerinden derlenmiş ayrıntılı bir kural dosyası var (ör. `tenancy-and-realm-design.md`, `oauth-oidc-profile.md`, `integration-matrix.md`).

### Agent'lar

| Agent | Model | Ne yapar | Ne zaman kullanılır |
| --- | --- | --- | --- |
| `keycloak-platform-architect` | opus | Realm/organization topolojisini, kurum başına federasyonu, güvence seviyelerini, claims sözleşmesini, kurulum ve HA biçimini ve Ranger/OpenBao sınırını tasarlar ve inceler. | Yeni kullanıcı kitlesi, olağandışı gereksinimli yeni kurum, realm'lerde, claims sözleşmesinde veya kimlik doğrulama politikasında her değişiklik |
| `keycloak-integration-engineer` | sonnet | Bir bileşeni Keycloak'a bağlar: Terraform'da istemci tanımı, bileşen ayarı, OpenBao/ESO ile secret, kullanılan claim'ler, logout ve bir doğrulama testi. | Bir bileşenin girişini veya token doğrulamasını eklerken ya da hata ayıklarken |
| `keycloak-config-reviewer` | sonnet | Terraform diff'leri, realm export'ları, Keycloak CR ve imaj değişiklikleri, tema/provider güncellemelerini güvenlik, güvence, claims sözleşmesi kararlılığı, HA ve platform kuralları açısından denetler. Bulguları BLOCK/FIX/NIT önem derecesiyle verir. | Keycloak yapılandırmasına veya kurulumuna dokunan her PR'da |

### Komutlar

Komutların argüman ipucu yok; bağlamı serbest metinle yazın.

| Komut | Argüman | Ne yapar |
| --- | --- | --- |
| `/keycloak-platform-design` | Kullanıcı kitleleri, kurumlar, ortamlar, bileşenler | Kimlik mimarisini (kitleler, realm ve organization'lar, kurum başına federasyon, güvence, claims sözleşmesi, kurulum/HA, entegrasyonlar, kanıt) kararlar ve açık sorularla birlikte tasarım dokümanı olarak üretir veya günceller. |
| `/keycloak-tenant-onboard` | Kurumun Tenant manifest'i | Kurumu alır: organization, doğrulanmış domain'ler, IdP veya LDAP federasyonu, platform gruplarına IdP mapper'ları, platform grupları, FGAP v2 ile kurum yöneticisi yetkisi, Terraform değişkenleri, doğrulama ve geri alma. |
| `/keycloak-client-register` | Bileşen veya job tanımı | Bir bileşeni istemci olarak kaydeder: istemci tipi, Terraform dosyası, scope/audience, istemci kimlik doğrulaması (`private_key_jwt` veya federated Kubernetes service account), secret yönetimi, bileşen ayar parçası, logout ve doğrulama testi. |
| `/keycloak-config-review` | Terraform diff, realm export, Keycloak CR veya imaj | Platform kurallarına göre bulgu tablosu (önem derecesi ve düzeltmeyle) üretir. |
| `/keycloak-access-review` | Kurum id'si veya `all`, dönem | Periyodik erişim gözden geçirme: grup üyelikleri, admin ve FGAP yetkileri, servis hesabı rolleri, eski hesaplar. Kurum yöneticileri için onay tablosu, ayrıcalıklı erişim ve makine kimliği tabloları üretir; kararları Terraform/SCIM ile uygular ve kanıtı saklar. |
| `/keycloak-upgrade-plan` | Mevcut ve hedef sürüm | Aradaki her sürümün sürüm notlarını ve yükseltme rehberini okur, platform yapılandırmasına göre kırıcı değişiklikleri listeler, imajı, Operator'ü, Terraform provider'ı ve `keycloak_version` sabitlemesini günceller; üretim kopyasında prova, rolling ya da kesintili yöntem seçimi, doğrulama ve geri alma. |

### Örnekler (examples/)

| Dosya | Ne yapar | Ek bağımlılık |
| --- | --- | --- |
| `deploy/Dockerfile` | Provider ve temalarla optimize Keycloak imajı | Docker |
| `deploy/keycloak-cr.yaml` | Keycloak CR v2beta1 ve PodDisruptionBudget | Keycloak Operator |
| `deploy/cnpg-cluster.yaml` | CloudNativePG PostgreSQL kümesi | CloudNativePG |
| `terraform/` | Realm tabanı (`realm.tf`), claims sözleşmesi scope'ları (`scopes.tf`), web uygulaması, Airflow, Kafka ve federated Kubernetes kimlik bilgili iş yükü istemcileri (`clients/`), kurum modülü (`modules/institution/`: organization, IdP, izinli grup mapper'ları, FGAP v2 yetki devri) | Terraform/OpenTofu, `keycloak/keycloak` v5.x |
| `integrations/` | Trino, Superset, Airflow 3, Strimzi Kafka, OpenBao, MinIO, Grafana ve Kubernetes API için ayar parçaları | İlgili bileşen |
| `automation/tenant_to_tfvars.py` | Tenant manifest'lerinden `institutions.auto.tfvars.json` üretir | `pyyaml` |
| `automation/claims_contract_check.py` | Bir access token'ı claims sözleşmesine göre kontrol eder (imza doğrulamaz). Kural ihlalinde çıkış kodu 1. | — |
| `automation/realm_audit.py` | Realm export'unu platform kurallarına göre denetler; BLOCK/FIX/INFO bulguları, BLOCK varsa çıkış kodu 1 | — |
| `tests/` | Otomasyon script'leri için pytest (qa-test-strategy taksonomisinde T-iam-010…032) | `pytest`, `pyyaml` |

## 4. Hızlı başlangıç

1. Plugin'i kurun (bölüm 2).
2. Tasarım isteyin: `/keycloak-platform-design 3 kurum (acme, globex, initech), çalışanlar kurum AD'sinden, açık veri portalı kullanıcıları, Trino + Superset + Airflow + Kafka, dev/stg/prod`.
3. Her kurumun Tenant manifest'inden Terraform değişkenlerini üretin: `python examples/automation/tenant_to_tfvars.py tenants/*.yaml > examples/terraform/institutions.auto.tfvars.json`, sonra `tofu validate` ve `tofu plan`.
4. Mevcut bir realm'i denetleyin: `kc.sh export --realm hkm --file hkm.json --users skip` ve `python examples/automation/realm_audit.py hkm.json --profile workforce`.
5. Bileşenleri tek tek bağlayın: `/keycloak-client-register Trino 4xx, web UI + CLI + servis hesapları`.

## 5. Kullanım senaryoları

**Yeni kurumu almak**
- Durum: `t-initech` kurumu platforma katılıyor; kullanıcıları kurumun kendi Entra ID / ADFS'inden gelecek.
- Ne yazarsınız: "/keycloak-tenant-onboard tenants/t-initech.yaml — IdP OIDC, domain `initech.gov.tr`, roller analyst/engineer/admin."
- Ne olur: `keycloak-identity-federation` ve `keycloak-config-as-code` skill'leri kullanılır; `modules/institution/` temel alınır.
- Çıktı: Organization ve domain, IdP tanımı, izinli grup mapper'ları, `platform_t-initech_*` grupları, kurum yöneticisi için FGAP v2 yetkisi, tfvars, doğrulama ve geri alma adımları.

**Bileşeni Keycloak'a bağlamak**
- Durum: Superset kullanıcıları Keycloak ile girecek, rolleri gruptan gelecek.
- Ne yazarsınız: "Superset 6'yı Keycloak'a bağla; `platform_*_analyst` Gamma, `platform_*_admin` Admin rolüne eşlensin."
- Ne olur: `keycloak-integration-engineer` devreye girer; `keycloak-platform-integrations` skill'i ve `integrations/superset/superset_config_oauth.py` kullanılır.
- Çıktı: Terraform istemci tanımı, `superset_config.py` parçası, secret yönetimi, logout ve doğrulama testi.

**Token'da claim sorunu**
- Durum: Trino kullanıcıyı tanıyor ama Ranger politikaları uygulanmıyor.
- Ne yazarsınız: "Bu token Trino'da çalışıyor ama Ranger grupları eşleşmiyor: `python claims_contract_check.py --token ... --audience trino` çıktısı ekte."
- Ne olur: `keycloak-claims-and-authorization` skill'i kullanılır; `groups` claim'inde yol olup olmadığı, grupların üst seviyede olup olmadığı (26.8 değişikliği) ve `principal-field` kontrol edilir.
- Çıktı: Kök neden, düzeltilecek mapper veya scope ve Terraform değişikliği.

**MFA ve step-up politikası**
- Durum: PII'ye erişen kullanıcılar için daha güçlü giriş isteniyor.
- Ne yazarsınız: "Çalışanlar için passkey veya OTP zorunlu olsun; PII okuyucu rolü aktive edilirken step-up istensin. AAL ve BG Rehberi eşlemesini ver."
- Ne olur: `keycloak-authentication-assurance` skill'i kullanılır.
- Çıktı: Authentication flow tasarımı, ACR/LoA eşlemesi, oturum süreleri, denetim için kontrol eşlemesi.

**Periyodik erişim gözden geçirme**
- Durum: Çeyrek sonu erişim gözden geçirmesi yapılacak.
- Ne yazarsınız: "/keycloak-access-review t-acme, 2026 Q4."
- Ne olur: Komut önce açık sahip kararlarını (pasiflik eşiği, gözden geçirme sıklığı) sorar; ardından hesap envanterini, ayrıcalıklı yetkileri ve makine kimliklerini toplar.
- Çıktı: Kurum yöneticisi için onay tablosu, ayrıcalıklı erişim ve makine kimliği tabloları, uygulanacak Terraform/SCIM değişiklikleri.

## 6. Dosya yapısı

```text
plugins/keycloak/
├── .claude-plugin/plugin.json
├── README.md
├── agents/
│   ├── keycloak-platform-architect.md     # mimari ve platform kararları
│   ├── keycloak-integration-engineer.md   # bileşen bağlama
│   └── keycloak-config-reviewer.md        # PR inceleme kontrol listesi
├── commands/                              # 6 komut
├── skills/                                # 10 skill, her birinde references/
├── examples/
│   ├── deploy/                            # Dockerfile, Keycloak CR, CNPG
│   ├── terraform/                         # realm, scope'lar, istemciler, kurum modülü
│   ├── integrations/                      # bileşen ayar parçaları
│   ├── automation/                        # tenant_to_tfvars, claims_contract_check, realm_audit
│   └── tests/
└── references/
    ├── sources.md, standards.md, books.md
    └── open-decisions.md                  # henüz karara bağlanmamış konular
```

Kurum modeli için önce `keycloak-architecture-tenancy` ve `terraform/modules/institution/`; claim sorunları için `keycloak-claims-and-authorization`; bileşen ayarı için `integration-matrix.md`.

## 7. İpuçları ve sınırlamalar

- **Örnekler canlı ortamda test edilmedi.** Yalnızca otomasyon script'lerinin pytest testleri çalıştırıldı (16 test geçti). Terraform için `tofu fmt` temiz, ama `tofu validate` çalıştırılmadı; pipeline'ınızda önce onu çalıştırın. Entegrasyon parçaları yalnızca sözdizimi olarak kontrol edildi; sürüme bağlı yerler "verify" ile işaretli.
- **Açık sahip kararları.** KVKK hukuki dayanağı, ilgili kişi hakları süreci, ihlal bildirimi, saklama süreleri, pasif hesap eşiği, geçici hesap yenileme aralığı gibi konular henüz karara bağlanmadı (`references/open-decisions.md`). Plugin bunlar için sayı veya hukuki görüş yazmaz; seçenek sunar ve kararın verilip verilmediğini sorar.
- **Keycloak olayları süre verilmezse sonsuza kadar saklar.** Saklama süresi karara bağlanana kadar bunu bilinçli olarak yönetin.
- **26.8'de grup claim'i değişti.** Yolsuz grup adları yalnızca üst seviye gruplara çözülür; platform gruplarını üst seviyede tutun.
- **Minor yükseltme kesinti ister.** Patch sürümleri Operator ile rolling güncellenebilir; minor yükseltmede tam kapatma gerekir. Patch'lerde de sürüm notlarını okuyun (26.8.2 Admin API audience, protocol mapper yetkileri, SAML ve token exchange kurallarını değiştirdi).
- **`claims_contract_check.py` imzayı doğrulamaz;** yalnızca claim'lerin biçimini ve içeriğini kontrol eder.
- Host adları (`sso.hkm.example`, `*.hkm.example`), `t-acme` ve registry yolları örnek değerlerdir.
- README'deki `trino` plugin'i bu marketplace'te yok; Trino tarafı `keycloak-platform-integrations` ve `integrations/trino/` altında.

## 8. Bakım

- Plugin `keycloak-workspace` çalışma alanında paketlenip (`dist/keycloak-<sürüm>.zip`) bu repoya eklenir. Güncellemede zip'i `plugins/keycloak/` üzerine açın. `build-platform-plugins.ps1` ve `build-platform-plugins_linux.sh` plugin'i `platform` kategorisiyle `marketplace.json`'a ekler.
- Yeni bir Keycloak sürümünde: `keycloak-operations-upgrades`'teki yükseltme listesini, imaj ve Operator etiketlerini, Terraform provider sürümünü ve `references/sources.md`'yi güncelleyin; `plugin.json` sürümünü artırın.
- Testler: `python -m pytest plugins/keycloak/examples/tests`
- Terraform: `cd plugins/keycloak/examples/terraform && tofu init && tofu validate`
- İlgili dokümanlar: [apache-ranger.md](apache-ranger.md) (veri erişim kararları, grup eşitleme), [apache-superset.md](apache-superset.md) (OAuth ve rol eşleme), [project-nessie.md](project-nessie.md) (OIDC), [qa-test-strategy.md](qa-test-strategy.md) (T-iam testleri), [data-governance.md](data-governance.md) (KVKK).
