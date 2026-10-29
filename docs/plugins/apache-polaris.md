# apache-polaris

> Apache Polaris (Iceberg REST catalog) için skill, agent ve çalıştırılabilir Docker lab'ları: katalog temelleri ve credential vending, erişim kontrolü (principal, principal role, catalog role, privilege), Management ve Iceberg REST API'leri, dış kataloglarla federation ve migration, OSS kurulumu, yapılandırma ve `polaris` CLI, Spark, Snowflake ve Dremio entegrasyonları.

| | |
| --- | --- |
| Sürüm | 0.1.0 |
| Kategori | data |
| Kaynak | `plugins/apache-polaris/` |
| Kaynak materyal | İç notlar ve Polaris 1.8.0 üzerinde ölçülmüş lab'lar, plugin'e konu adlarıyla yeniden düzenlenmiş haliyle |

## 1. Ne işe yarar?

Apache Polaris, Iceberg tablolarının metadata işaretçisini tutan bir REST katalog. Spark, Trino, Dremio, Snowflake ve PyIceberg aynı katalogdan aynı tabloları okuyup yazar. Erişim kuralları katalogda durur. Motorlara uzun ömürlü bucket anahtarı yerine tabloya özel, kısa ömürlü storage credential'ı verilir (credential vending).

Plugin, Polaris'i kurmak, kataloglarını ve rollerini tasarlamak, API'sini otomasyondan çağırmak ve motorları bağlamak için gereken bilgiyi toplar. Bilgilerin çoğu belgelerden aktarılmadı, gerçek bir Polaris 1.8.0 sunucusunda **ölçüldü**. Örnekler:
- Yeni bir katalogun `catalog_admin` rolü tablo oluşturamıyor, çünkü `CATALOG_MANAGE_CONTENT` yetkisi yok.
- `NAMESPACE_LIST` yetkisi tabloları listelemiyor.
- Grant iptali `DELETE` ile değil `POST` ile yapılıyor.
- `"spec"` alanıyla oluşturulan tablo hata vermeden partition'sız oluşuyor.

Bu sonuçlar skill'lerde tablo halinde var.

Genel Iceberg konuları bu plugin'de yalnızca kısaca anlatılır ve `apache-iceberg` plugin'ine yönlendirilir: tablo formatı, partitioning, bakım, genel katalog seçimi. Bu plugin Polaris'e özgü olana odaklanır.

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install apache-polaris@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/apache-polaris`

Gereksinimler:
- Skill ve agent'lar için ek bir şey gerekmez.
- Lab'ları çalıştırmak için Docker ve Docker Compose. Her stack kendi Polaris, SeaweedFS (S3 + STS) ve Python test runner'ını getirir. Bazı stack'ler ayrıca Spark, Trino, Dremio, Gravitino, Lakekeeper, PostgreSQL ve Prometheus/OpenTelemetry kullanır.
- Windows'ta Git Bash ile `docker compose exec` komutunun önüne `MSYS_NO_PATHCONV=1` ekleyin.

## 3. İçerik

### Skill'ler

| Skill | Ne zaman devreye girer | İçerdiği konular |
| --- | --- | --- |
| `polaris-catalog-fundamentals` | Polaris'i anlatmak/seçmek, ilk motoru bağlamak | Polaris'in yeri, metadata pointer ve commit kontrolü, REST handshake (token, `config?warehouse=`, vended credentials), katalog tipleri, REST istemci ekosistemi, roadmap ve 1.8.0'daki gerçek durumu |
| `polaris-access-control` | RBAC tasarımı, 401/403 ve `AccessDenied` hataları, credential rotation, erişim denetimi | Katalog izolasyonu, principal yaşam döngüsü (reset/rotate), 25 privilege ve ölçülmüş yetki × işlem matrisi, namespace kalıtımı, token scope'ları, YAML ile roles-as-code, 7 kurallı denetim |
| `polaris-rest-api` | curl/Python otomasyonu, 400/404/405/409/501 hataları | Management API (catalog, principal, rol, grant, `entityVersion`), Iceberg REST API (namespace, tablo, commit, rename, register, metrics, notifications, multi-table transaction, view), 54 isteğin düzeltilmiş halleri |
| `polaris-catalog-federation` | Başka bir REST katalogu Polaris üzerinden açmak, Polaris'e geçiş | EXTERNAL katalog ve `connectionConfigInfo`, federation feature flag'leri, uzak tablolarda RBAC ve vending, kademeli migration (önce federation, sonra `register`), Nessie/Gravitino/Lakekeeper/Glue |
| `polaris-deployment` | Polaris'i Docker Compose ile kurmak, quickstart'tan kalıcı kuruluma geçmek | İmajlar ve portlar (8181/8182), in-memory ve PostgreSQL persistence, admin-tool ile bootstrap, S3/STS storage, ilk katalog/principal/rol, metrics, trace, log, readiness kontrolleri |
| `polaris-configuration-and-cli` | Gündelik yönetim ve ince ayar | `polaris` CLI (auth, profiller, 1.8.0'daki doğru komut biçimleri), realm'ler, feature flag'ler ve realm override, `polaris.config.*`, rate limiting, token broker |
| `polaris-spark` | Spark'ı Polaris'e bağlamak, Spark hatalarını çözmek | SparkCatalog/RESTCatalog ayarları, vended credentials ve S3FileIO, çoklu katalog, DataFrameWriterV2 ve SparkSQL, time travel, Structured Streaming, `CALL ... system.*` bakım prosedürleri, RBAC'ın Spark hatası olarak görünümü |
| `polaris-snowflake` | Snowflake'in Polaris tablolarını okuması | External volume ve catalog integration (`CATALOG_SOURCE = POLARIS`), `CREATE ICEBERG TABLE`, refresh, Polaris tarafında salt-okur servis principal'ı, internete açık Polaris için üretim ayarları |
| `polaris-dremio` | Dremio'yu Polaris üzerinde motor olarak kullanmak | Iceberg REST Catalog kaynağı ve Dremio OSS sınırı, `Source.Namespace.Table` adlandırma, metadata fonksiyonları, `AT SNAPSHOT`, CTAS, `COPY INTO`, `OPTIMIZE` |

### Agent'lar

| Agent | Model | Ne yapar | Ne zaman kullanılır |
| --- | --- | --- | --- |
| `polaris-platform-architect` | opus | Gereksinimlerden kurulum (persistence, bootstrap, storage/STS), katalog yerleşimi, erişim modeli (YAML), federation/migration planı ve motor bağlantıları tasarlar. Config, provisioning adımları ve doğrulama adımları döner. | Polaris kurulumu planlanırken; yeni ekip veya motor eklenirken; Hive/Glue/Nessie'den geçişte |
| `polaris-config-reviewer` | sonnet | Sunucu config'i, storage config, RBAC modeli, REST/CLI script'leri ve motor ayarlarını inceler. Bulduğu sorunlar: fazla geniş yetkiler, eksik data privilege, yanlış istek gövdeleri, kalıcı olmayan persistence, config'teki secret'lar, eksik vending. | Config veya script yazıldıktan sonra; Polaris 4xx döndüğünde; motor tablo okuyamadığında |

### Komutlar

| Komut | Argüman | Ne yapar |
| --- | --- | --- |
| `/design-polaris-platform` | Gereksinim açıklaması | Tasarımı architect agent'a yaptırır. İlgili `examples/` dosyalarından başlayıp projeye uyarlar, ardından reviewer'dan geçirir ve kararları, dosyaları, açık soruları özetler. |
| `/review-polaris-config` | Dosya yolları, config/script veya bir semptom (HTTP durumu, motor hatası) | İncelemeyi reviewer'a yaptırır. Yapısal değişiklik gereken bulguları architect'e devreder. Bulguları önem sırasıyla, düzeltmeleriyle verir. |

### Lab'lar (skills/*/examples/)

| Stack | Skill | Compose project | İçerik |
| --- | --- | --- | --- |
| `catalog-basics`, `rest-ecosystem` | fundamentals | `polaris-catalog-basics`, `polaris-rest-ecosystem` | Metadata pointer, katalog mekaniği, REST spec; Trino/PyIceberg/Polars ile ekosistem ve roadmap kontrolü |
| `access-control` | access-control | `polaris-access-control` | 5 paket, 39 test: izolasyon, principal'lar, privilege matrisi, principal role'ler, roles-as-code |
| `rest-api` | rest-api | `polaris-rest-api` | 3 paket, 10 test, 54 istek |
| `federation` | federation | `polaris-federation` | Federation, migration, Gravitino, Lakekeeper |
| `quickstart-and-persistence`, `observability` | deployment | `polaris-deploy`, `polaris-ops` | Kurulum ve persistence; metrics, trace, log, readiness |
| `config-and-cli` | configuration-and-cli | `polaris-config` | CLI, realm'ler, hardening |
| `spark` | spark | `polaris-spark` | Bağlantı, DataFrame, SparkSQL, streaming, bakım |
| `snowflake-standin` | snowflake | `polaris-snowflake` | Servis principal'ı ve üretim ayarları (Polaris tarafı) |
| `dremio` | dremio | `polaris-dremio` | Bağlantı, sorgular, CTAS/COPY INTO, bakım |

## 4. Hızlı başlangıç

1. Plugin'i kurun (bölüm 2).
2. Tasarım için: `/design-polaris-platform 3 ekip, finans verisi ayrı tutulacak, Spark ve Dremio kullanılacak, PostgreSQL ile kalıcı kurulum`.
3. Davranışı kendi makinenizde görmek için bir lab çalıştırın:

   ```bash
   cd plugins/apache-polaris/skills/polaris-access-control/examples/access-control
   docker compose -p polaris-access-control up -d --build
   docker compose -p polaris-access-control exec runner python -m pytest -v
   docker compose -p polaris-access-control down -v
   ```

4. Var olan config ve script'leri kontrol ettirin: `/review-polaris-config infra/polaris/ scripts/provision_polaris.py`.

## 5. Kullanım senaryoları

**Ekipler için katalog ve rol tasarımı**
- Durum: pazarlama, finans ve uyum ekipleri tek bir Polaris'i paylaşacak; finans verisi hassas.
- Ne yazarsınız: "Polaris'te pazarlama, finans ve compliance için katalog ve rol modeli tasarla; analistler sadece okusun, finans verisine sadece compliance erişsin"
- Ne olur: `polaris-platform-architect` ekip başına katalog önerir; hassas katalog ayrı bucket'ta durur. Grant'ler namespace düzeyinde, data ve metadata yetkileri ayrı tutulur. Hassas katalog için allow-list tanımlanır.
- Çıktı: `rbac_model.yaml` biçiminde erişim modeli, sırasıyla uygulanacak provisioning adımları ve doğrulama için "erişebilmeli / erişememeli" listesi.

**Spark'ta 403 veya AccessDenied**
- Durum: Spark job'u tabloyu görüyor ama okurken storage `AccessDenied` alıyor.
- Ne yazarsınız: `/review-polaris-config spark-defaults.conf` ve hata mesajı
- Ne olur: reviewer, `polaris-spark` ve `polaris-access-control` tablolarına bakarak eksik `X-Iceberg-Access-Delegation` header'ını veya `TABLE_READ_DATA` eksikliğini bulur.
- Çıktı: dosya:satır bazında bulgu ve düzeltilmiş ayar ya da grant.

**REST API ile otomasyon**
- Durum: CI'da her ortam için katalog, namespace ve tablo oluşturan bir script yazılacak.
- Ne yazarsınız: "Polaris'te katalog, namespace ve günlük partition'lı tablo oluşturan bir Python script'i yaz"
- Ne olur: `polaris-rest-api` skill'i devreye girer. `storageConfigInfo` ve `partition-spec` kullanılır, `entityVersion` okunup gönderilir, token isteğinde `scope` bulunur.
- Çıktı: `spec_bodies.py` örneğine dayanan, doğru gövdelerle çalışan bir script.

**Eski katalogdan Polaris'e geçiş**
- Durum: tablolar Nessie veya başka bir REST katalogda; yazıcılar hemen taşınamıyor.
- Ne yazarsınız: "mevcut REST katalogdaki tabloları kesinti olmadan Polaris'e taşıma planı çıkar"
- Ne olur: `polaris-catalog-federation` skill'i devreye girer. Plan şöyle: önce eski katalog federation ile Polaris'ten okunur, sonra tablolar veri kopyalamadan `register` ile iç kataloga alınır, en son eski yazıcılar durdurulur.
- Çıktı: sıralı migration planı ve her adımın doğrulama kontrolleri.

## 6. Dosya yapısı

```text
plugins/apache-polaris/
├── .claude-plugin/plugin.json
├── agents/
│   ├── polaris-platform-architect.md
│   └── polaris-config-reviewer.md
├── commands/
│   ├── design-polaris-platform.md
│   └── review-polaris-config.md
└── skills/<skill>/
    ├── SKILL.md                 # özet, karar tablosu, gotcha'lar, anti-pattern'ler
    ├── references/*.md          # ayrıntılı API/config, ölçülmüş sonuçlar, sorun giderme tabloları
    └── examples/<stack>/        # docker-compose.yml, runner, config/, common/, test paketleri, README.md
```

Hızlı yanıt için `SKILL.md` dosyasındaki karar tablosuna bakın. Ayrıntılı davranış ve hata tabloları `references/` altında. Davranışı doğrulamak için ilgili `examples/<stack>/README.md` dosyasındaki komutları kullanın. `polaris-rest-api/examples/rest-api/common/spec_bodies.py` doğru istek gövdeleri için hazır bir başvuru dosyası.

## 7. İpuçları ve sınırlamalar

- **Sürüm:** ölçümler Polaris 1.8.0 ile yapıldı. Compose dosyalarında `apache/polaris:latest` ve `chrislusf/seaweedfs` sabitlenmemiş, yeni bir imaj sonuçları değiştirebilir. Davranışa güvenmeden önce kendi sürümünüzde ilgili testi çalıştırın.
- **Bu plugin'e alınırken lab'lar çalıştırılmadı.** Kaynakta temiz bir ortamda geçmişlerdi. Plugin'e taşınırken çoğuna yalnızca isim ve yorum değişikliği yapıldı. İki yerde kod değişti ve hiç çalıştırılmadı: `catalog-basics` stack'indeki `metadata_pointer` paketi yeniden yazıldı, `observability` runner imajı da yeni.
- **Compose project adı önemli:** `polaris-deploy`, `polaris-ops`, `polaris-config` ve `polaris-snowflake` stack'lerindeki testler container'ları project etiketiyle buluyor. Bu stack'leri README'deki `-p` adıyla başlatın.
- **Test sırası:** `rest-ecosystem` altındaki `roadmap_check` testleri sırayla çalışmalı. `rest-ecosystem` her çalıştırmadan önce temiz bir stack (`down -v`) ister. `rest-api` altındaki `test_deletes_last` paket olarak çalıştırılmalı.
- **Doğrulanmayanlar:**
  - Snowflake SQL'i hiç çalıştırılmadı; yalnızca Polaris tarafı test edildi.
  - Nessie ve AWS Glue federation'ı denenmedi.
  - Dremio reflections denenmedi.
  - Roadmap'teki bazı özellikler yalnızca 1.8.0 jar'larında görüldü, davranışları denenmedi.
- **Spark lab'ı:** ana testler `1.1.0-incubating-SNAPSHOT` imajında, S3 vending testleri 1.8.0'da koşuyor.
- **Sürüme göre değişen davranışlar:** `credentialRotationRequired` bir 1.8.0 stack'inde principal'ı rotate edene kadar engelledi, başka bir stack'te engellemedi. Lakekeeper federation'ı brotli hatasıyla 500 döndü. Bunları kendi sürümünüzde kontrol edin.
- **Secret'lar:** lab'lardaki `root` / `s3cr3t` ve STS imza anahtarları yalnızca yerel test içindir. Gerçek kurulumda secret'lar environment değişkenlerinde veya bir secret manager'da tutulmalı.

## 8. Bakım

- Yeni bir Polaris sürümü çıktığında, ilgili stack'in imaj etiketini değiştirip testleri çalıştırın. Değişen sonuçlar için skill'in `references/` tablolarını güncelleyin. Privilege semantiği değişirse `access-control` altındaki `test_privilege_matrix_matches_measured_reference` testi bunu yakalar.
- Lab çalıştırma kalıbı her stack için aynı:

  ```bash
  docker compose -p <project> up -d --build
  docker compose -p <project> exec runner python -m pytest -v
  docker compose -p <project> down -v
  ```

- Plugin yapısını doğrulamak: repo kökünde `claude plugin validate .`.
- Sürüm `.claude-plugin/plugin.json` ve `.claude-plugin/marketplace.json` içinde. Kategori iki build script'te tanımlı (`apache-polaris:data`).
- İlgili doküman: genel Iceberg konuları için `apache-iceberg` plugin'i (tablo formatı, katalog, bakım, operasyon skill'leri).
