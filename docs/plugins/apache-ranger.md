# apache-ranger

> Apache Ranger'ı (en az 2.8.0, hedef 2.9.0) HKM platformunun erişim politikası ve denetim (audit) katmanı olarak kurmak, politikalarını tasarlamak ve işletmek için skill, agent ve örnekler: mimari ve politika modeli, resmi Helm chart'ı olmadan Kubernetes kurulumu, Keycloak/LDAP kimlik eşitleme, çok kurumlu RBAC+ABAC politika tasarımı, Trino/Kafka/Spark plugin'leri, plugin'i olmayan motorlar için yerel kural üretimi, OpenSearch/immudb denetimi, policy-as-code ve yükseltmeler.

| | |
| --- | --- |
| Sürüm | 0.2.0 |
| Kategori | data |
| Kaynak | `plugins/apache-ranger/` |
| Kaynak materyal | Ranger dokümanları ve sürüm notları, Trino/Strimzi/Kyuubi dokümanları, NIST ABAC, ISO 27001, KVKK, kitap özetleri (`references/sources.md`, `references/standards.md`, `references/books.md`) |

## 1. Ne işe yarar?

Apache Ranger, veri motorları için merkezi erişim politikası ve denetim sistemidir. Politikalar Ranger Admin'de tutulur; her motorun içindeki plugin politikaları periyodik olarak indirir, önbelleğe alır, kararı motorun içinde verir ve her kararı denetim kaydı olarak gönderir. Kaynak politikalarının yanında tag tabanlı politikalar, maskeleme, satır filtresi, deny/exception kalemleri ve güvenlik bölgeleri (security zone) vardır.

Plugin, HKM platformunda **Ranger'ın tek erişim kararı kaynağı** olduğunu varsayar. Platform kuralları agent ve skill'lere gömülü:
- **Politikanın kaynağı Git, Ranger çalışma zamanıdır.** İnsanlar `Policy` manifest'lerini düzenler; kontrol düzlemi bunları REST API v2 ile Ranger politikalarına derler. Üretimde arayüzden yapılan değişiklik drift sayılır.
- **Motor kümesi başına bir Ranger servisi, kurum başına bir güvenlik bölgesi.** Paylaşılan veri setleri `shared` bölgesinde, kurum başına satır filtresiyle durur.
- **Kullanıcı değil grup.** Politikalar Keycloak kaynaklı `platform_{kurum}_{rol}` gruplarını ve Ranger rollerini kullanır; kullanıcı seviyesinde yetki yalnızca servis hesaplarına verilir.
- **Varsayılan deny, hassasiyet için tag politikası.** Kaynak politikaları erişim verir; PII, amaç ve sınıflandırma tag'leri erişimi engeller ya da maskeler. Maske ve satır filtresi her istemci yolunda aynıdır.
- **Her karar bir kez ve değiştirilemez biçimde denetlenir.** Plugin denetimleri OpenSearch'e (arama) ve immudb'ye (kurcalama kanıtı) gider.
- **Plugin'ler kapalı başarısız olur (fail closed).** Admin'e ulaşılamazsa önbellekteki politika kullanılır; önbellek yaşı izlenir, boş önbellek erişimi reddeder.

Trino, Kafka ve Spark SQL (Kyuubi AuthZ üzerinden) plugin ile korunur. Plugin'i olmayan motorlar (ClickHouse, Superset, Nessie, CKAN, WSO2) için aynı politika modelinden yerel kurallar üretilir: ClickHouse row policy, Superset RLS, Nessie CEL. Yetkilendirme modeli Azure RBAC/ABAC kavramlarını izler: rol tanımı, kapsam, rol ataması, koşul, korkuluk olarak deny, süreli yetki yükseltme, erişim gözden geçirme.

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install apache-ranger@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/apache-ranger`

Gereksinimler:
- Skill, agent ve komutlar için ek bir şey gerekmez.
- `examples/automation/*.py` için Python 3, `pyyaml` ve `httpx`. `compile_policies.py` çevrimdışı çalışır; `apply_policies.py`, `sync_groups_keycloak.py` ve `sync_tags_openmetadata.py` çalışan Ranger, Keycloak ve OpenMetadata ister.
- `examples/tests/` için `pytest`. `test_compile_manifest.py` dışındaki testler ayrıca `trino`, `kafka-python` ve canlı Ranger/Trino/Kafka/OpenSearch ister.
- Kurulum örnekleri için bir Kubernetes kümesi ve PostgreSQL; yerel deneme için `examples/deploy/docker-compose.yml`.

## 3. İçerik

### Skill'ler

| Skill | Ne zaman devreye girer | İçerdiği konular |
| --- | --- | --- |
| `ranger-architecture-policy-model` | Ranger davranışını açıklamak, bir kuralın nereye ait olduğuna karar vermek | Admin, usersync, tagsync, plugin'ler ve indir/önbellek/uygula/denetle döngüsü; service-def ve servisler; access, masking ve row-filter politikaları; allow/deny ve exception kalemleri; koşullar, geçerlilik takvimi, öncelik, roller, delegate admin, etiketler; tag politikaları, güvenlik bölgeleri, değerlendirme sırası; PAP/PDP/PEP/PIP görünümü, OPA ve motorların kendi ACL'leriyle karşılaştırma |
| `ranger-deployment-kubernetes` | Ranger'ı kurmak, boyutlandırmak, yeniden yapılandırmak | Resmi chart olmadığında imaj üretme ve sürüm sabitleme, PostgreSQL, `install.properties` ve `ranger-admin-site`, TLS, HA Admin replica'ları, tek usersync, OpenBao + External Secrets ile secret'lar, denetim deposu seçimi, boyutlandırma, probe'lar, yedekleme, ortam yerleşimi |
| `ranger-identity-usersync` | Kimlik entegrasyonu tasarlamak, "grup bulunamadı / politika uygulanmadı" sorunu çözmek | usersync kaynakları (LDAP/AD, Unix, dosya), Keycloak'a federe LDAP'tan eşitleme, kontrol düzleminden REST ile grup gönderme, grup adlandırma ve rol modeli, ABAC için kullanıcı öznitelikleri, servis hesapları, Admin UI girişi, motorların kimliği plugin'e nasıl ilettiği |
| `ranger-policy-design-abac-masking` | Politika setini tasarlamak veya yeniden düzenlemek | Katmanlı politika seti (taban, bölge kaynak politikaları, paylaşılan veride satır filtresi, tag ile maske ve deny, amaç sınırlaması rolleri), RBAC ile ABAC dengesi ve politika sayısı kontrolü, her istemcide aynı kalan maske/filtre kalıpları, süreli ve break-glass erişim, impersonation, adlandırma, karar tabloları, anti-pattern'ler |
| `ranger-engine-plugins` | Bir motoru Ranger'a bağlamak, koruma kapsamını ve boşlukları değerlendirmek | Trino (yerleşik `ranger` access control, kaynaklar ve erişim tipleri, maske/filtre, impersonation), Strimzi üzerinde Kafka (`RangerKafkaAuthorizer`, imaj katmanı, KRaft, topic önekli politikalar), Kyuubi AuthZ ile Spark SQL (Iceberg desteği, sınırlar), plugin'i olmayan motorlar için yerel kural derleme, OpenMetadata'dan tag |
| `ranger-audit-compliance` | Denetim hattını tasarlamak, denetçiye cevap vermek, erişim incelemek | Plugin access audit ve Admin audit, denetim hedefleri ve 2.7 paketleme değişikliği, spool ile kayıp önleme, audit filtreleri, OpenSearch + immudb hattı, saklama, KVKK/GDPR kanıt raporları, erişim gözden geçirme, alarmlar |
| `ranger-api-policy-as-code` | Ranger otomasyonu kurmak veya incelemek | REST API v2 (servis, service-def, politika, rol, bölge, tag, kullanıcı/grup), Python istemcisi, export/import, manifest'ten idempotent derleme, sabit GUID ve etiketler, Git'e karşı drift tespiti, CI doğrulama ve karar tablosu testleri, OpenMetadata'dan tag gönderimi, güvenli yayın ve geri alma |
| `ranger-operations-upgrades` | İzleme kurmak, runbook yazmak, yükseltme planlamak | SLI'lar (politika indirme, önbellek yaşı, usersync ve audit gecikmesi, karar gecikmesi), HA ve arıza modları, büyük politika setlerinde performans, yedekleme/geri yükleme, güvenlik duyuruları, 2.6→2.7→2.8 yükseltmesi (audit modül ayrımı, JWT plugin kimlik doğrulaması), uyumluluk matrisi, runbook'lar |

Skill'lerin çoğunda `references/` altında kitap özetlerinden derlenmiş kural dosyaları var. Yetkilendirme modelinin ayrıntısı: `skills/ranger-policy-design-abac-masking/references/azure-style-rbac-abac.md`.

### Agent'lar

| Agent | Model | Ne yapar | Ne zaman kullanılır |
| --- | --- | --- | --- |
| `ranger-platform-engineer` | opus | Admin/usersync/tagsync topolojisini, PostgreSQL'i, Keycloak/LDAP eşitlemesini, kurum başına servis ve bölgeleri, Trino/Kafka/Spark plugin yayınını, OpenSearch/immudb denetim hattını, HA ve yükseltmeyi tasarlar. Platform kurallarını uygular. | Ranger kurulumu, entegrasyonu, işletimi veya mimarisiyle ilgili her işte |
| `ranger-policy-author` | sonnet | `Policy` manifest'lerinden ya da düz gereksinimlerden kaynak, tag, maske ve satır filtresi politikaları yazar; REST API v2 JSON'u ve plugin'i olmayan motorlar için yerel karşılıkları üretir. | Politika yazarken, incelerken veya "neden erişemiyor" sorusunda |
| `ranger-config-reviewer` | sonnet | `install.properties`, usersync ayarı, Trino/Kafka/Kyuubi plugin XML'leri, Kubernetes manifest'leri, denetim hedefleri ve politika export'larını güvenlik, HA, fail-closed, denetim bütünlüğü ve platform kuralları açısından denetler. Bulguları BLOCK/FIX/NIT önem derecesiyle tablo halinde verir. | Ranger'a veya bir motorun Ranger plugin ayarına dokunan her PR'da |

### Komutlar

| Komut | Argüman | Ne yapar |
| --- | --- | --- |
| `/ranger-platform-design` | Bağlam: kurumlar, motorlar, izolasyon katmanları, uyum gereksinimleri | 7 adımlı tasarım: motor başına plugin mi yerel kural mı, topoloji, kimlik, servis/bölge/tag/rol modeli, plugin ayarları ve yayın sırası, denetim hattı, otomasyon ve yükseltme. Çıktı: Mermaid topoloji, karar tablosu, `install.properties`, usersync ve plugin XML'leri, bölge/servis/rol planı, test ve izleme listesi. |
| `/ranger-policy-compile` | Gereksinimler veya `Policy` manifest'leri | Özne/kaynak/eylem/koşul modelini çıkarır; tag mı, kaynak mı, satır filtresi mi, maske mi kararını verir; servis ve bölge başına REST API v2 JSON'u, ClickHouse/Superset/Nessie karşılıklarını, karar tablosunu ve test vakalarını üretir. |
| `/ranger-service-onboard` | Motor veya küme tanımı | Bir motoru Ranger'a alır: service-def kontrolü, servis oluşturma, plugin imajı ve ayarı, super user'lar, taban politikalar ve kurum bölgeleri, audit-only deneme, sonra zorunlu uygulama. Geri alma adımlarını da verir. |
| `/ranger-audit-review` | Dönem, kurum veya veri seti; varsa export edilmiş denetim verisi | Access ve Admin denetimlerini özetler: deny desenleri, PII tag'li kaynaklara olağandışı erişim, policy id `-1` ile izin verilen olaylar, Git dışı politika değişiklikleri, 90 gündür kullanılmayan politikalar, süresiz ayrıcalıklı atamalar, plugin'siz yolların erişimleri. KVKK'ya hazır erişim raporu ve düzeltme listesi üretir. |
| `/ranger-config-review` | İncelenecek dosyalar | `ranger-config-reviewer` kontrol listesini uygular; bulgu tablosu ve karar döner. |

### Örnekler (examples/)

| Dosya | Ne yapar | Ek bağımlılık |
| --- | --- | --- |
| `deploy/install.properties`, `deploy/usersync-install.properties` | Admin (PostgreSQL, OpenSearch audit) ve LDAP usersync ayarları | — |
| `deploy/k8s-ranger.yaml` | Şema kurulum Job'ı, Admin Deployment/Service, tek usersync, ExternalSecret | Kubernetes |
| `deploy/docker-compose.yml` | Testler için yerel Ranger | Docker |
| `plugins/trino/` | `access-control.properties`, `ranger-trino-security.xml`, `ranger-trino-audit.xml` | Trino |
| `plugins/kafka/` | Strimzi custom authorizer CR, plugin XML'leri, imaj katmanı `Dockerfile` | Strimzi |
| `plugins/spark/` | Kyuubi AuthZ için `spark-defaults.conf` ve `ranger-spark-security.xml` | Spark, Kyuubi |
| `policies/policy-manifest.yaml` | Örnek platform manifest'i: Institution, RoleDefinition (`pii-reader`, `restricted-reader` dahil), kurum/alan/staging/veri seti kapsamında RoleAssignment; `agreement`, `validity`, `eligible` atama ve aktivasyon alanları | — |
| `policies/zones-and-services.json`, `baseline-trino.json`, `tag-policies.json` | Servis ve bölge tanımları, taban politika, tag maskeleri (PII tipine göre maske, `RESTRICTED` kolon maskesi, tablo seviyesinde deny) | — |
| `policies/decision-table.csv` | Beklenen izin/ret/maske kararları | — |
| `automation/compile_policies.py` | Manifest → servis ve bölge başına Ranger JSON (sabit GUID, etiket). Kullanıcı principal'ı, staging dışında insan yazma yetkisi, anlaşmasız yabancı grup, 8 saati aşan aktivasyon ve 3 ayı aşan okuyucu uygunluğu reddedilir. | `pyyaml` |
| `automation/apply_policies.py` | REST ile plan/apply/drift | `httpx`, Ranger |
| `automation/sync_groups_keycloak.py`, `sync_tags_openmetadata.py` | Keycloak gruplarını ve OpenMetadata tag'lerini Ranger'a taşır | `httpx`, Keycloak, OpenMetadata |
| `automation/ci-ranger-policies.yml` | Derle → doğrula → test → plan → uygula CI akışı | CI |
| `tests/test_compile_manifest.py` | Derleyicinin çevrimdışı testi | `pytest`, `pyyaml` |
| `tests/test_decision_table.py` | Karar tablosunu Trino interactive ve ETL üzerinden doğrular | `trino`, canlı Trino + Ranger |
| `tests/test_kafka_access.py` | OAUTHBEARER ile Kafka erişim testi | `kafka-python`, canlı Kafka |
| `tests/test_audit_and_propagation.py` | Denetim kaydının oluştuğunu ve yetki geri alımının süresi içinde uygulandığını doğrular | `trino`, `httpx`, OpenSearch |

## 4. Hızlı başlangıç

1. Plugin'i kurun (bölüm 2).
2. Ortamı tarif ederek tasarım isteyin: `/ranger-platform-design 3 kurum (acme, globex, initech), Trino interactive + ETL, Strimzi Kafka, Spark/Kyuubi, ClickHouse ve Superset plugin'siz, KVKK`.
3. `examples/policies/policy-manifest.yaml`'ı kendi kurumlarınıza göre düzenleyip derleyin: `GIT_SHA=$(git rev-parse --short HEAD) python examples/automation/compile_policies.py manifests/*.yaml > desired-policies.json`.
4. Ayarları inceletin: `/ranger-config-review install.properties plugins/trino/`.
5. Bir motoru önce audit-only modda alın (`/ranger-service-onboard`), karar tablosuyla karşılaştırın, sonra zorunlu uygulamaya geçin.

## 5. Kullanım senaryoları

**Yeni kurumun politika seti**
- Durum: Platforma `t-initech` kurumu ekleniyor; analistler okuyacak, mühendisler staging'e yazacak, PII kolonları maskelenecek.
- Ne yazarsınız: "`t-initech` için policy manifest'i yaz: analyst okur, engineer staging'e yazar, PII kolonları hash ile maskelensin, pii-reader rolü 8 saatlik aktivasyonla açılsın. Ranger JSON'unu ve karar tablosunu üret."
- Ne olur: `/ranger-policy-compile` veya `ranger-policy-author` devreye girer; `ranger-policy-design-abac-masking` skill'i kullanılır.
- Çıktı: Manifest, servis ve bölge başına JSON, ClickHouse/Superset karşılıkları, karar tablosu ve test vakaları.

**Kurumlar arası veri paylaşımı**
- Durum: `t-acme`, `t-globex`'in belirli bir tablosunu anlaşma kapsamında okuyacak.
- Ne yazarsınız: "`t-globex.sales` tablosunu `t-acme` analistlerine `agreement: AGR-2026-04` ile aç, yalnızca `region='marmara'` satırları görünsün."
- Ne olur: Paylaşım yalnızca `trino-interactive` servisine, anlaşmadan üretilen satır filtresiyle verilir; anlaşmasız yabancı grup derleyicide reddedilir.
- Çıktı: Satır filtresi politikası, geçerlilik süresi, karar tablosu satırları.

**Kafka'yı Ranger'a almak**
- Durum: Strimzi Kafka şu an ACL ile korunuyor.
- Ne yazarsınız: "/ranger-service-onboard Strimzi Kafka 3.9, KRaft, OAUTHBEARER, topic adları `{kurum}.{alan}.{ad}`."
- Ne olur: `ranger-engine-plugins` skill'i kullanılır; `examples/plugins/kafka/` temel alınır.
- Çıktı: Servis JSON'u, authorizer CR'ı, imaj katmanı, topic önekli taban politikalar, deneme kontrol listesi ve geri alma adımları.

**Erişim denetimi raporu**
- Durum: Denetçi son çeyrekte PII verisine kimin eriştiğini soruyor.
- Ne yazarsınız: "/ranger-audit-review 2026 Q3, kurum t-acme, PII tag'li kaynaklar."
- Ne olur: Komut OpenSearch `ranger-audit-*` indeksini ve Admin denetimlerini tarar; plugin'siz yolları "Ranger tarafından uygulanmıyor" diye işaretler.
- Çıktı: Kurum/grup/kaynak bazında özet, PII erişimleri, anomaliler, hijyen bulguları ve düzeltme listesi.

**2.8'e yükseltme**
- Durum: Ranger 2.6 çalışıyor, 2.8'e geçilecek.
- Ne yazarsınız: "Ranger 2.6'dan 2.8'e yükseltme planı çıkar; Trino 48x, Kafka 3.9 ve Kyuubi AuthZ uyumluluğunu kontrol et."
- Ne olur: `ranger-operations-upgrades` skill'i kullanılır: audit modül ayrımı, JWT plugin kimlik doğrulaması, service-def güncellemeleri.
- Çıktı: DB kopyasında prova, sıra, uyumluluk matrisi, doğrulama ve geri alma adımları.

## 6. Dosya yapısı

```text
plugins/apache-ranger/
├── .claude-plugin/plugin.json
├── README.md
├── agents/
│   ├── ranger-platform-engineer.md      # tasarım ve platform kuralları
│   ├── ranger-policy-author.md          # politika yazımı
│   └── ranger-config-reviewer.md        # PR inceleme kontrol listesi
├── commands/
│   ├── ranger-platform-design.md
│   ├── ranger-policy-compile.md
│   ├── ranger-service-onboard.md
│   ├── ranger-audit-review.md
│   └── ranger-config-review.md
├── skills/                               # 8 skill, çoğunda references/ kural dosyaları
├── examples/
│   ├── deploy/                           # install.properties, k8s, docker-compose
│   ├── plugins/{trino,kafka,spark}/
│   ├── policies/                         # manifest, servis/bölge, tag politikaları, karar tablosu
│   ├── automation/                       # compile/apply/sync script'leri, CI akışı
│   └── tests/
└── references/                           # sources.md, standards.md, books.md
```

Politika modeli için önce `policies/policy-manifest.yaml` ve `azure-style-rbac-abac.md`; kurulum için `deploy/` ve `ranger-deployment-kubernetes`; kaynak doğrulama için `references/sources.md`.

## 7. İpuçları ve sınırlamalar

- **Örneklerin çoğu canlı ortamda test edilmedi.** Yalnızca `test_compile_manifest.py` çevrimdışı çalıştırıldı (14 test geçti). Karar tablosu, Kafka, denetim/yayılma testleri ve deploy manifest'leri canlı Ranger, Trino ve Kafka ister.
- **Laboratuvarda doğrulanması gerekenler:** bölge kapsamlı tag politikaları, `TAG._type` / `TAG.type` / `TAG.level` koşul ifadeleri, kalem bazında `validitySchedules`, Spark'ta `<catalog>_<schema>` veritabanı eşlemesi.
- **Resmi Helm chart yok.** İmajlar `dev-support/ranger-docker` ile platform tarafından üretilip sürümü sabitlenir.
- **2.7'den itibaren plugin paketlerinde yalnızca Solr ve HDFS denetim kütüphaneleri var.** OpenSearch/Elasticsearch, Kafka veya Log4j hedefi için ilgili modülü imaja ayrıca ekleyin.
- **Spark için Apache Ranger plugin'i yok.** Spark SQL, Apache Kyuubi'nin Ranger destekli AuthZ eklentisiyle korunur. Kontrol driver'daki SQL optimizer'da yapılır; RDD/Dataset API, doğrudan dosya okuma (`spark.read.parquet("s3://…")`) ve serbest kod bunu atlar. Asıl sınır kurum başına object-store kimlik bilgileridir, AuthZ ek savunma katmanıdır.
- **Plugin'siz motorlar Ranger tarafından korunmaz.** ClickHouse, Superset, Nessie ve CKAN için üretilen yerel kurallar ayrı ayrı test edilmeli; denetim raporlarında bu yollar ayrıca işaretlenir.
- **Erişim kararı Ranger ile OPA arasında bölünmez.** OPA yalnızca saklama/yaşam döngüsü politikası için kullanılır.
- README'deki `trino` plugin'i bu marketplace'te yok; Trino tarafı `ranger-engine-plugins` skill'inde ve `examples/plugins/trino/` altında.

## 8. Bakım

- Plugin bu repoda elle yazılmıştır; upstream'den çekilmez. `build-platform-plugins.ps1` ve `build-platform-plugins_linux.sh` onu `data` kategorisiyle `marketplace.json`'a ekler.
- Yeni bir Ranger sürümünde: agent'taki "Ground truth" sürüm listesini, `ranger-operations-upgrades`'teki yükseltme adımlarını ve uyumluluk matrisini, imaj etiketlerini ve `references/sources.md`'yi güncelleyin; `plugin.json` sürümünü artırın.
- Çevrimdışı test: `python -m pytest plugins/apache-ranger/examples/tests/test_compile_manifest.py`
- Canlı testler (staging): `RANGER_URL=... TRINO_HOST=... TRINO_ETL_HOST=... KAFKA_BOOTSTRAP=... OPENSEARCH_URL=... pytest plugins/apache-ranger/examples/tests`
- İlgili dokümanlar: [keycloak.md](keycloak.md) (kimlik ve gruplar), [project-nessie.md](project-nessie.md) (aynı politikadan CEL kuralları), [apache-superset.md](apache-superset.md) (impersonation ve RLS), [qa-test-strategy.md](qa-test-strategy.md) (izolasyon ve maske testleri), [data-governance.md](data-governance.md) (KVKK).
