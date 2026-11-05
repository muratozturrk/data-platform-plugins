# project-nessie

> Project Nessie 0.108.x'i HKM platformunun Iceberg kataloğu olarak kurmak, güvenceye almak ve işletmek için skill, agent ve örnekler: Git benzeri branch/tag/commit modeli, Iceberg REST uç noktası (Nessie Catalog) ile S3 imzalama ve credential vending, Helm ile üretim kurulumu, Keycloak OIDC ve Ranger ile birlikte çalışan CEL yetkilendirmesi, motor entegrasyonları, branch tabanlı iş akışları, GC ve KVKK silme prosedürü, Apache Polaris'ten geçiş.

| | |
| --- | --- |
| Sürüm | 0.1.0 |
| Kategori | data |
| Kaynak | `plugins/project-nessie/` |
| Kaynak materyal | projectnessie.org dokümanları ve 0.106–0.108 sürüm notları, Nessie Helm chart, Trino/ClickHouse/Iceberg dokümanları, Iceberg ve Polaris kitapları (`skills/nessie-catalog-fundamentals/references/sources.md`) |

## 1. Ne işe yarar?

Project Nessie, Iceberg tabloları için Git mantığıyla çalışan bir katalog. Repository içinde branch'ler ve tag'ler var; her değişiklik atomik bir commit. Birden fazla tabloya yapılan değişiklik tek commit'te yayınlanabilir, geçmiş sorgulanabilir ve bir branch önceki bir hash'e geri alınabilir. Nessie iki API sunar: motorların ve CLI'ın kullandığı yerel REST API v2 (`/api/v2`) ve object-store güvenliğini motorlara ileten Iceberg REST uç noktası (`/iceberg/{ref}/`, "Nessie Catalog").

Plugin, HKM platformunda **Nessie'nin seçilen katalog** olduğunu varsayar. Platform kuralları skill ve agent'lara gömülü:
- Her ortam için bir Nessie repository'si; kurum başına `t-{kurum}` branch'i ya da `main` üzerinde kurum başına namespace.
- `main` korumalı. Motorlar kurum veya WAP branch'lerine yazar, `main`'e yalnızca promotion job merge eder.
- Object-store erişimi Nessie Catalog üzerinden S3 imzalama ya da kısa ömürlü credential ile verilir; uzun ömürlü paylaşılan anahtar yasak.
- Yetkilendirme iki katmanlı: Nessie CEL kuralları metadata'yı (branch, path), Ranger satır/kolon erişimini korur. İkisi aynı `Policy` kaynağından üretilir.
- KVKK silme işlemi bir prosedürdür: satır silme → yeniden yazma → `expire_snapshots` → tüm referanslarda Nessie GC → doğrulama → OpenBao'da crypto-shredding → immudb kanıt kaydı.

Aynı marketplace'teki `apache-polaris` plugin'i referans ve geçiş kaynağı olarak duruyor. Yeni işlerde ikisini birlikte "aktif" rehber olarak kullanmayın. Genel Iceberg konuları için `apache-iceberg`, kurum/tenant modeli ve kontrol düzlemi için `platform-engineering` plugin'ine bakın.

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install project-nessie@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/project-nessie`

Gereksinimler:
- Skill, agent ve komutlar için ek bir şey gerekmez.
- `examples/*.py` script'leri için Python 3 ve `httpx`; `test_branch_protection.py` için ayrıca `pytest`. Script'ler çalışan bir Nessie sunucusu ve Keycloak token'ları ister (ortam değişkenleriyle verilir).
- Helm ve Kubernetes örnekleri için bir Kubernetes kümesi, PostgreSQL ve S3 uyumlu object store (ObjectScale, Ceph RGW veya MinIO AIStor).

## 3. İçerik

### Skill'ler

| Skill | Ne zaman devreye girer | İçerdiği konular |
| --- | --- | --- |
| `nessie-catalog-fundamentals` | Nessie kavramlarını anlatmak, katalog yerleşimi tasarlamak, merge çakışmasını açıklamak | Repository, referanslar (branch, tag, detached hash), commit ve operasyonlar (`Put`/`Delete`/`Unchanged`), content key ve namespace, content type'lar, merge/transplant, çakışma tespiti, katalog seviyesinde time travel; Nessie / Polaris / Hive Metastore / Lakekeeper karşılaştırması |
| `nessie-iceberg-rest-catalog` | Nessie Catalog'u ya da bir motoru Iceberg REST üzerinden yapılandırmak | `/iceberg/{ref}/` uç noktası, warehouse ve bucket ayarları, S3 request signing (`CATALOG_S3_SIGN`) ile vended credentials farkı, Keycloak OAuth2, Spark/Trino/ClickHouse/PyIceberg istemci ayarları, 0.108.1 ve 0.108.2 güvenlik düzeltmeleri |
| `nessie-deployment` | Nessie'yi kurmak, boyutlandırmak, yükseltmek, sorun gidermek | Helm chart ve Quarkus ayarları, version store seçimi (üretimde JDBC/PostgreSQL), HA (stateless replica'lar), portlar ve probe'lar, OIDC, OpenTelemetry/Prometheus, boyutlandırma başlangıç değerleri, yedekleme, yükseltme, export/import |
| `nessie-authentication-authorization` | Nessie güvenlik ayarı yazmak veya incelemek | Keycloak OIDC, CEL kural değişkenleri ve `op` listesi, kurum bazlı platform kural seti, `main` koruması, S3 imzalama kuralı, Ranger ile görev paylaşımı tablosu, `AccessChecker` SPI |
| `nessie-engine-integrations` | Bir motoru Nessie'ye bağlamak veya motor hatası çözmek | Trino (`nessie` yerel tipi ile `rest` tipi), Spark extension'ları ve branch/tag SQL'i, Flink, ClickHouse `DataLakeCatalog`, dbt-trino, PyIceberg, sürüm uyumluluk matrisi |
| `nessie-branching-workflows` | Pipeline promotion, release ve kurtarma süreçleri tasarlamak | Referans yerleşimi (`main`, `t-*`, `wap-*`, `backfill-*`, `exp-*`, `rel-*`), write-audit-publish adımları, release tag'leri, hash'e geri alma ile rollback, çok tablolu transaction |
| `nessie-gc-and-retention` | Saklama süresi planlamak, GC çalıştırmak, silmeyi kanıtlamak | `nessie-gc` identify/expire/sweep, referans bazlı cut-off, Iceberg bakım prosedürleriyle sıra, CronJob, KVKK silme prosedürünün 7 adımı |
| `nessie-cli-api-and-migration` | Nessie'yi kontrol düzleminden otomatikleştirmek, geçiş planlamak | `nessie-cli` komutları, REST API v2 uç noktaları, `nessie-quarkus-cli` export/import, Polaris/HMS/diğer REST kataloglardan `register_table` ile geçiş, cut-over ve rollback |

### Agent'lar

| Agent | Model | Ne yapar | Ne zaman kullanılır |
| --- | --- | --- | --- |
| `nessie-platform-architect` | opus | Repository/branch/namespace yerleşimi, Nessie Catalog ve object-store güvenliği, Helm kurulumu, CEL kuralları ve Ranger karşılıkları, motor ayarları, GC/silme ve Polaris geçişi tasarlar. Platform kurallarını uygular. | Nessie ile ilgili her tasarım, kurulum, güvenlik, iş akışı veya geçiş işinde |
| `nessie-config-reviewer` | sonnet | Sunucu ayarı, Helm values, yetki kuralları, motor katalog ayarları, branch yerleşimi ve GC job'larını 7 başlıkta denetler: güvenlik, version store, yerleşim, motorlar, GC, gözlemlenebilirlik, değişiklik güvenliği. Bulguları BLOCK/FIX/NIT önem derecesiyle tablo halinde verir. | Nessie veya motor katalog ayarına dokunan her PR'da |

### Komutlar

| Komut | Argüman | Ne yapar |
| --- | --- | --- |
| `/design-nessie-platform` | Bağlam: kurumlar, ortamlar, object store'lar, motorlar, izolasyon katmanları | 7 adımda tasarım yapar (referanslar, warehouse'lar, kurulum, yetkilendirme, motorlar, GC, Polaris geçişi). Çıktı: yerleşim tablosu, `helm-values.yaml`, `application.properties`, `authorization-rules.properties`, motor katalog ayarları, GC CronJob, yedekleme, yükseltme planı ve fitness function'lar. |
| `/review-nessie-config` | İncelenecek dosyalar | `nessie-config-reviewer` kontrol listesini uygular; bulgu tablosu ve karar döner. |

### Örnekler (examples/)

| Dosya | Ne yapar | Ek bağımlılık |
| --- | --- | --- |
| `helm-values.yaml` | Üretim Nessie: 2 replica, JDBC/PostgreSQL, Keycloak OIDC, yetkilendirme, OTel, ObjectScale için S3 warehouse'ları | Helm, Kubernetes |
| `application.properties` | Aynı ayarların Quarkus karşılığı (referans veya `advancedConfig` için) | — |
| `authorization-rules.properties` | Platform CEL kural seti: admin, kontrol düzlemi, kurum okuma/yazma, S3 imzalama, `main` promotion | — |
| `engines/trino-t_acme_lake.properties` | Trino, yerel `nessie` tipi (platform varsayılanı) | Trino |
| `engines/trino-t_acme_lake-rest.properties` | Trino, REST + vended credentials | Trino |
| `engines/spark-defaults.conf`, `engines/flink-catalog.sql`, `engines/clickhouse-catalog.sql`, `engines/pyiceberg.yaml` | Diğer motorlar için katalog ayarları | İlgili motor |
| `k8s/nessie-gc-cronjob.yaml` | Gece çalışan GC: cut-off politikaları ve `--defer-deletes` | Kubernetes, `nessie-gc` imajı |
| `wap_promote.py` | REST API v2 ile write-audit-publish: WAP branch'i açar, kalite denetimini çağırır, merge eder, isteğe bağlı release tag'i atar, branch'i siler | `httpx` |
| `migrate_from_polaris.py` | Polaris kataloğundaki tabloları metadata konumuyla bir Nessie branch'ine kaydeder; dosyalar taşınmaz | `httpx` |
| `test_branch_protection.py` | Fitness testi: kurum rolü `main`'e commit atamaz, başka kurumun branch'ini göremez, release tag'lerini okuyabilir | `httpx`, `pytest` |

## 4. Hızlı başlangıç

1. Plugin'i kurun (bölüm 2).
2. Ortamınızı tarif ederek tasarım isteyin: `/design-nessie-platform 3 kurum (acme, globex, initech), dev/stg/prod, ObjectScale, Trino + Spark + ClickHouse, pool tier`.
3. Çıkan dosyaları `examples/` altındaki karşılıklarıyla karşılaştırın ve kendi hostname, bucket ve Keycloak realm değerlerinizi girin.
4. Ayarları inceleme için verin: `/review-nessie-config helm-values.yaml authorization-rules.properties engines/`.
5. Staging'de `test_branch_protection.py`'yi iki kurum token'ıyla çalıştırın; GC'yi ilk seferde `--defer-deletes` ile çalıştırıp live-set'i inceleyin.

## 5. Kullanım senaryoları

**Kurum başına izolasyon tasarımı**
- Durum: Platforma yeni bir kurum ekleniyor; ayrı branch mı, ayrı namespace mi olacağına karar verilecek.
- Ne yazarsınız: "Yeni kurum `t-initech` için Nessie yerleşimini tasarla. Pool tier, ObjectScale bucket'ı `s3://hkm-lake/t-initech/`. CEL kurallarını ve Trino katalog dosyasını da üret."
- Ne olur: `nessie-platform-architect` devreye girer; `nessie-catalog-fundamentals`, `nessie-authentication-authorization` ve `nessie-engine-integrations` skill'lerini kullanır.
- Çıktı: Referans/namespace tablosu, kuruma özel CEL kuralları ve Ranger karşılıkları, `trino-t_initech_lake.properties`.

**Write-audit-publish akışı**
- Durum: Gece yüklemesi yarım kalırsa raporlar tutarsız veri gösteriyor.
- Ne yazarsınız: "`sales_daily` pipeline'ı için WAP akışı kur: Spark yazsın, dbt testleri WAP branch'inde koşsun, başarılıysa `t-acme`'ye merge edip `rel-` tag'i atsın."
- Ne olur: `nessie-branching-workflows` skill'i kullanılır; `wap_promote.py` temel alınır.
- Çıktı: Adım adım akış, Spark SQL komutları (`CREATE BRANCH ... IN nessie`, `MERGE BRANCH ...`), çakışma ve rollback davranışı.

**Trino tabloları göremiyor**
- Durum: Trino `Cannot obtain metadata` hatası veriyor ya da tablolar görünmüyor.
- Ne yazarsınız: "/review-nessie-config etc/catalog/t_acme_lake.properties — Trino 'Cannot obtain metadata' hatası veriyor."
- Ne olur: Reviewer motor ayarlarını kontrol eder: `rest` tipinin `/api/v2` yerine `/iceberg/{ref}/`'e gidip gitmediği, `vended-credentials-enabled`, token kaynağı, motorlar arası katalog adı tutarlılığı.
- Çıktı: BLOCK/FIX/NIT bulgu tablosu ve düzeltmeler.

**KVKK silme talebi**
- Durum: Bir ilgili kişinin verisi göl katmanından tamamen silinmeli ve bu kanıtlanmalı.
- Ne yazarsınız: "Müşteri 12345 için KVKK silme prosedürünü çıkar. Veri `t-acme` branch'inde, `rel-` tag'leri ve bir `exp-` branch'i var."
- Ne olur: `nessie-gc-and-retention` skill'i kullanılır. Eski tag ve branch'lerin silinmiş veriyi hâlâ tuttuğu, motor tarafındaki `expire_snapshots`'ın tek başına yetmediği vurgulanır.
- Çıktı: 7 adımlı prosedür, GC cut-off ayarları, doğrulama ve crypto-shredding/kanıt kaydı adımları.

**Polaris'ten geçiş**
- Durum: Mevcut tablolar Apache Polaris'te; Nessie'ye geçilecek.
- Ne yazarsınız: "Polaris kataloğu `analytics`'i Nessie'nin `t-acme` branch'ine taşı. Cut-over ve rollback planı ver."
- Ne olur: `nessie-cli-api-and-migration` skill'i ve `migrate_from_polaris.py` kullanılır; Polaris tarafı için `apache-polaris` plugin'ine yönlendirilir.
- Çıktı: Envanter, yazma dondurma, `register_table` ile kayıt, doğrulama, cut-over ve rollback adımları.

## 6. Dosya yapısı

```text
plugins/project-nessie/
├── .claude-plugin/plugin.json
├── README.md
├── agents/
│   ├── nessie-platform-architect.md     # tasarım ve platform kuralları
│   └── nessie-config-reviewer.md        # PR inceleme kontrol listesi
├── commands/
│   ├── design-nessie-platform.md
│   └── review-nessie-config.md
├── skills/
│   ├── nessie-catalog-fundamentals/     # + references/sources.md (tüm kaynaklar)
│   ├── nessie-iceberg-rest-catalog/
│   ├── nessie-deployment/
│   ├── nessie-authentication-authorization/
│   ├── nessie-engine-integrations/
│   ├── nessie-branching-workflows/
│   ├── nessie-gc-and-retention/
│   └── nessie-cli-api-and-migration/
└── examples/
    ├── helm-values.yaml, application.properties, authorization-rules.properties
    ├── engines/                         # Trino (native/REST), Spark, Flink, ClickHouse, PyIceberg
    ├── k8s/nessie-gc-cronjob.yaml
    └── wap_promote.py, migrate_from_polaris.py, test_branch_protection.py
```

Kurulum için önce `examples/helm-values.yaml` ve `nessie-deployment`; güvenlik için `authorization-rules.properties` ve `nessie-authentication-authorization`; kaynak doğrulama için `references/sources.md`.

## 7. İpuçları ve sınırlamalar

- **Örnekler canlı bir sunucuda test edilmedi.** Python script'leri yalnızca sözdizimi açısından kontrol edildi. Hostname'ler (`nessie.platform.svc`, `objectscale.hkm.internal`) ve kurum adları (`t-acme`, `t-globex`) örnek değerlerdir.
- **Sürüm bilgileri 2026 ortası itibarıyla.** Nessie sık sürüm çıkarıyor ve S3 imzalama yolunda güvenlik düzeltmeleri geliyor. Kullanacağınız sürümü sabitleyin (en az 0.108.1) ve yükseltmeden önce sürüm notlarını okuyun. Uyumluluk matrisindeki Trino 484, Iceberg 1.11.x ve ClickHouse 25.x değerlerini kendi ortamınızda doğrulayın.
- **Nessie verinin kendisini korumaz.** CEL kuralları yalnızca metadata'yı korur. Satır/kolon güvenliği Ranger'da, dosya erişimi object-store politikası ve imzalama/vending'dedir. Eski hash'ler GC çalışana kadar okunabilir kalır.
- **Trino S3 imzalama yapamaz.** REST yolunda vended credentials ve dışarıdan verilen OAuth2 token gerekir; platform varsayılanı yerel `nessie` tipidir.
- **STS desteği store'a göre değişir.** İmzalama ve vending için object store'un STS/assume-role desteği gerekir; ObjectScale, Ceph RGW ve AIStor'da staging'de ayrıca doğrulayın.
- **CEL kuralları örnektir.** `authorization-rules.properties`'teki rol adı ayrıştırma ifadeleri (`substring`, `split`) Keycloak grup adı biçimine (`platform_t-{kurum}_{rol}`) bağlı. Farklı bir adlandırmada kuralları yeniden yazın ve iki kurumlu test matrisiyle deneyin.
- **Release tag adı.** `branching-workflows` `rel-{YYYYMMDD}` biçimini önerir; `wap_promote.py --release` ise `rel-{run_id}` üretir. Kendi kuralınıza göre birini seçin.
- GC'yi cut-off değişikliğinden sonra mutlaka önce dry-run / `--defer-deletes` ile çalıştırın. GC servis hesabına WORM (Object Lock) arşiv bucket'larında silme yetkisi vermeyin.

## 8. Bakım

- Plugin bu repoda elle yazılmıştır; upstream'den çekilmez. `build-platform-plugins.ps1` ve `build-platform-plugins_linux.sh` onu `data` kategorisiyle `marketplace.json`'a ekler.
- Yeni bir Nessie sürümünde: `nessie-engine-integrations`'taki uyumluluk matrisini, agent'taki "Ground truth" sürüm listesini, `helm-values.yaml` ve `nessie-gc-cronjob.yaml`'daki imaj etiketlerini ve `references/sources.md`'yi güncelleyin; `plugin.json` sürümünü artırın.
- Script kontrolü: `python -c "import ast,sys;[ast.parse(open(f,encoding='utf-8').read()) for f in sys.argv[1:]]" plugins/project-nessie/examples/*.py`
- Fitness testi (staging): `NESSIE_URI=... TOKEN_T_ACME_ENGINEER=... TOKEN_T_ACME_ANALYST=... TOKEN_SVC_PROMOTION=... pytest plugins/project-nessie/examples/test_branch_protection.py`
- İlgili dokümanlar: [apache-polaris.md](apache-polaris.md) (geçiş kaynağı), [openmetadata.md](openmetadata.md) (lineage), [data-governance.md](data-governance.md) (KVKK).
