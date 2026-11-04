# openmetadata

> OpenMetadata'yı on-prem bir veri platformunun yönetişim kataloğu olarak kurmak, platform bileşenlerine bağlamak, eksiklerini kapatmak, KVKK uyumunu desteklemek ve yönetişim olgunluğunu kanıtla ölçmek için skill, agent ve komutlar.

| | |
| --- | --- |
| Sürüm | 0.1.0 |
| Kategori | governance |
| Kaynak | `plugins/openmetadata/` |
| Kaynak materyal | OpenMetadata 1.13.3 (commit `255f6694`) üzerinde yapılmış, kod seviyesinde kanıtlı yetenek ve boşluk araştırması; DataHub 1.7.0 ve Apache Atlas 2.5.0 karşılaştırması; KVKK mevzuatının resmî metinlerden doğrulanmış eşlemesi |

## 1. Ne işe yarar?

OpenMetadata açık kaynak bir veri kataloğu ve yönetişim aracıdır. Tabloları, panoları ve boru hatlarını tek yerde listeler, aralarındaki soyağacını (lineage) çizer, sahiplik, sözlük ve etiketleri tutar. Bu plugin, OpenMetadata'yı bir veri platformuna yerleştiren ekibin sorduğu soruları yanıtlar. Hangi bileşen için hazır bağlayıcı (connector) var? Soyağacı nasıl oluşuyor? Bir eksik nasıl kapatılır? KVKK yükümlülüklerinin hangisi katalogda karşılanabiliyor?

Plugin'in dayanağı belgeler değil, OpenMetadata 1.13.3'ün kaynak kodudur. Skill'lerdeki iddiaların çoğu `dosya:satır` referansıyla kanıtlanır, her iddianın kanıt tipi de belirtilir (`K` kod, `D` resmî doküman, `İ` issue/yol haritası, `B` üretici beyanı). Bir özellik kodda bulunamadığında "yok" denmez. Bunun yerine "1.13.3'te bulunamadı" yazılır ve nerede arandığı belirtilir.

Plugin yazılırken araştırmadaki bazı iddialar kaynak koda karşı yeniden kontrol edildi, yanlış ya da eksik çıkanlar düzeltildi. Örnekler:
- Takım (Team) ve servis varlıkları özel alan (custom property) taşıyamıyor. Bu yüzden veri işleyen ve yurt dışı aktarım bilgisi başka varlıklara konuyor.
- Cron ile zamanlanan bir yönetişim iş akışı tetikleyicisi var. Eksik olan, görev başına son tarih ve eskalasyon.
- Trino bağlayıcısı Iceberg tablolarını tanıyor, ama ayrı bir Iceberg bağlayıcısı yok.

Her düzeltme ilgili skill'in `references/sources.md` dosyasında kayıtlı.

`data-governance` plugin'i yönetişimi genel olarak (sözleşme, erişim, kalite) anlatır. Bu plugin ise yalnızca OpenMetadata'ya özgü olanı anlatır.

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install openmetadata@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/openmetadata`

Gereksinimler: ek bir şey gerekmez. Skill'ler bilgi ve şablon içerir, çalıştırılacak kod yoktur. Bir iddiayı kendi sürümünüzde doğrulamak için OpenMetadata kaynak kodunun ilgili sürümü yeterlidir.

## 3. İçerik

### Skill'ler

| Skill | Ne zaman devreye girer | İçerdiği konular |
| --- | --- | --- |
| `openmetadata-architecture` | Kurulum, boyutlandırma, yükseltme, "bu yetenek nereye yazılmalı" sorusu, lisans riski | Sunucu, MySQL/PostgreSQL, Elasticsearch/OpenSearch, Airflow ve alternatifleri; HA, yedekleme/felaket kurtarma; JSON Schema varlık modeli, özel alan tipleri ve sınırları, sürümleme; REST API, SDK'lar, `metadata` CLI, CSV içe/dışa aktarım, değişim olayları; genişleme noktaları (app, `EXT_CLASSPATH` ile yüklenen sınıflar, ticari sürüme ayrılmış boş noktalar); Vault'un desteklenmemesi; Apache-2.0 / Collate lisans ayrımı |
| `openmetadata-platform-integration` | Ingestion planı, workflow YAML'ı yazmak, eksik soyağacını ayıklamak | Kafka, Flink, NiFi, Airflow, dbt, Spark, MinIO + Iceberg, Trino, ClickHouse, Great Expectations, Ranger, Vault için bağlayıcı matrisi; soyağacı yöntemleri (sorgu günlüğü, dbt, Airflow, Spline, OpenLineage, API); doğrulanmış YAML örnekleri; eksik bağlayıcılar ve geçici çözümler |
| `openmetadata-gap-closing` | Genişletme yol haritası, "yapmaya değer mi", efor tahmini | 24 boşluğun etki × kolaylık skorlu kataloğu; hızlı kazanımlar ve ayrı proje gerektirenler; her genel boşluk için adım adım kapatma yolu; yapılandırma → dış otomasyon → eklenti sınıfı → connector → şema değişikliği basamakları |
| `openmetadata-kvkk-compliance` | Kişisel veri envanteri, VERBİS çıktısı, ilgili kişi başvurusu, ihlal kapsamı, denetime hazırlık | Doğrulanmış 49 yükümlülüğün katalog yeteneğine eşlenmesi; doğrulanmış süreler (30 gün, 72 saat, 6 ay, 7 gün…); 7499 sayılı Kanun ile gelen yurt dışı aktarım rejimi; amaç, hukuki sebep, saklama, imha, rıza, özel nitelikli veri gibi bilgilerin bugünkü yapılarla modellenmesi (zorlanan / yalnız uzlaşma ayrımıyla); D14.1–D14.11 boşlukları. Hukuki görüş değildir. |
| `openmetadata-governance-assessment` | Olgunluk değerlendirmesi, ürün karşılaştırma, yükseltme sonrası yeniden ölçüm | 14 alan / 90 yetenekli dondurulmuş rubrik ve DMBOK eşlemesi; 0–4 puanlama, kanıt tipleri, "yokluk zayıf iddiadır" kuralı (`null` ile 0 ayrımı); kanıt YAML şablonu; çapraz kontrol kuralları; OpenMetadata 1.13.3 sonuçları ve DataHub/Atlas kıyası |
| `openmetadata-capability-design` | Yeni özellik önerisi, fikrin özgünlüğünü kontrol, aday sıralama, tasarım dokümanı | C01–C19 yeni yetenek adayları (LLM, agentic, ontoloji) ve özgünlük kararları; özgünlük kontrol yöntemi; ağırlıklı skorlama (Etki 25, KVKK 18, Özgünlük 17, Platform uyumu 17, OpenMetadata'ya oturma 13, Efor 10); KVKK etki değerlendirmesi zorunlu teknik tasarım şablonu |

### Agent'lar

| Agent | Model | Ne yapar | Ne zaman kullanılır |
| --- | --- | --- | --- |
| `openmetadata-platform-architect` | opus | Kurulumu, kimlik doğrulama ve sır yönetimini, bileşen başına ingestion ve soyağacı planını ve yönetişim modelini (domain, sözlük, sınıflandırma, özel alanlar) tasarlar. Eksik bağlayıcıları geçici çözümleriyle tabloda tutar. | OpenMetadata kurulumu planlanırken; platforma yeni bileşen eklenirken; soyağacı görünmediğinde |
| `openmetadata-extension-engineer` | opus | Bir boşluğu ya da yeni fikri OpenMetadata'nın gerçek yapıları üzerinde uygulanabilir tasarıma çevirir. En ucuz yolu seçer, özgünlüğü kontrol eder, KVKK etki değerlendirmesini yazar. | Bir eksik kapatılacağında; LLM/agentic/ontoloji özelliği düşünüldüğünde; özel alan mı şema değişikliği mi kararında |
| `openmetadata-kvkk-reviewer` | opus | Kurulumu, sınıflandırma taksonomisini, özel alanları ya da tasarımı KVKK yükümlülükleri açısından inceler. Her yükümlülük için "destekleniyor / yalnız uzlaşma / eksik" der. | Denetim öncesi; kişisel veri kataloğa alınırken; kişisel veri etiketleri veya özel alanlar değiştiğinde |
| `openmetadata-governance-assessor` | opus | Rubrikle puanlama yapar: sürümü sabitler, her puana kanıt bağlar, bilinmeyeni `null` bırakır, şüpheli satırları yeniden kontrol eder. | Olgunluk ölçümünde; ürün seçiminde; bir yetenek iddiasının kanıtı sorgulandığında |

### Komutlar

| Komut | Argüman | Ne yapar |
| --- | --- | --- |
| `/openmetadata-integration-plan` | Platform bileşenleri ve kısıtlar (air-gapped, kimlik sağlayıcı, hassasiyet) ya da eklenecek tek bileşen | Tasarımı architect agent'a yaptırır. Bağlayıcısı olmayan bileşenler tablodan düşmez. Kişisel veri varsa KVKK reviewer'dan geçirir. |
| `/openmetadata-gap-plan` | Yetenek kimliği (`D6.3`), aday kimliği (`C07`) ya da serbest gereksinim | Boş bırakılırsa hızlı kazanımları gösterip seçtirir. Tasarımı extension engineer'a yaptırır, kişisel veriye dokunuyorsa KVKK reviewer'dan geçirir. |
| `/openmetadata-kvkk-audit` | Dosya yolları (taksonomi, özel alan tanımları, sözlük, politika), yapıştırılmış metadata ya da bir soru | İncelemeyi KVKK reviewer'a yaptırır. Eksik bulgulara kapatma yolunu ekler. Bulguları önem sırasıyla verir. |
| `/openmetadata-assess` | Ürün veya kurulum, sürüm, kapsanacak alanlar, kanıtın yeri | Kapsamı ve sürümü netleştirir. Tam değerlendirmede alanları gruplara bölüp birden fazla assessor çalıştırır, sonra birleştirip çapraz kontrol yapar. |

## 4. Hızlı başlangıç

1. Plugin'i kurun.
2. Platformunuzu anlatın: `/openmetadata-integration-plan Kafka, Flink, MinIO+Iceberg, Trino, ClickHouse, dbt, Airflow; air-gapped; kimlik sağlayıcı Keycloak`.
3. Dönen tablodaki "Gap" sütununa bakın. Bağlayıcısı ya da soyağacı olmayan bileşenler orada geçici çözümleriyle yazılıdır.
4. Kişisel veri işliyorsanız sınıflandırma ve özel alan tanımlarınızı denetletin: `/openmetadata-kvkk-audit ./governance/classifications.yaml`.
5. Kapatmak istediğiniz bir eksik için tasarım isteyin: `/openmetadata-gap-plan D1.2`.

## 5. Kullanım senaryoları

**Iceberg tabloları katalogda görünmüyor.**
Ne yazarsınız: "MinIO üzerindeki Iceberg tablolarımızı OpenMetadata'ya nasıl alırız?"
Ne olur: `openmetadata-platform-integration` devreye girer. Ayrı bir Iceberg bağlayıcısı olmadığını ve Trino bağlayıcısının tabloyu Iceberg olarak işaretlediğini anlatır. `datalake`/`s3` bağlayıcısını Iceberg deposuna yöneltmenin neden yanlış olduğunu açıklar.
Çıktı: Trino üzerinden alım için doğrulanmış YAML ve snapshot bilgisi gerekiyorsa bağlayıcı yazma yolu.

**Denetim öncesi KVKK hazırlığı.**
Ne yazarsınız: `/openmetadata-kvkk-audit` ve sınıflandırma, özel alan ve sözlük dışa aktarımlarınız.
Ne olur: Reviewer her yükümlülük için durumu çıkarır. Örnek: "Hukuki sebep kolon düzeyinde tutuluyor ama zorunlu değil, yalnız uzlaşma."
Çıktı: Önem sıralı bulgu tablosu (madde, varlık, durum, OpenMetadata'da düzeltme), yükümlülük bazında kapsama özeti ve hukuki görüş olmadığı notu.

**İhlal anında etki alanı.**
Ne yazarsınız: "Şu üç tablo sızdı, hangi kişi kategorileri ve aşağı akış varlıkları etkilendi?"
Ne olur: KVKK skill'i soyağacı ve kişisel veri etiketleriyle kapsam çıkarmanın adımlarını verir. 72 saatlik sürenin ihlalin öğrenildiği andan başladığını ve OpenMetadata'da ayrı bir ihlal kaydı olmadığını hatırlatır.
Çıktı: Kapsam çıkarma sorgusu ve dışarıda tutulması gereken bilgiler.

**Ürün seçimi ya da yükseltme sonrası ölçüm.**
Ne yazarsınız: `/openmetadata-assess OpenMetadata 1.14, D3 ve D6 alanları, kaynak kodu ./openmetadata`.
Ne olur: Assessor sürümü sabitler, her yeteneği kanıtla puanlar ve sonucu 1.13.3 sonuçlarıyla karşılaştırır. Farkın sürüm değişikliğinden mi yoksa farklı okumadan mı geldiğini söyler.
Çıktı: Alan özeti, tam tablo, çapraz kontrol günlüğü ve sınırlılıklar.

**Yeni bir LLM özelliği fikri.**
Ne yazarsınız: "Katalogda Türkçe doğal dil araması istiyoruz."
Ne olur: Extension engineer bunu D2.3 boşluğuna ve C07 adayına bağlar. OpenMetadata'da boş bırakılmış `NoOpNLQService` noktasının yapılandırmayla değiştirilebildiğini gösterir. Açık ağırlıklı, on-prem model varsayımıyla tasarımı ve KVKK etki değerlendirmesini yazar.
Çıktı: Rota karar tablosu, tasarım, efor ve doğrulama planı.

## 6. Dosya yapısı

```text
plugins/openmetadata/
├── .claude-plugin/plugin.json
├── agents/
│   ├── openmetadata-platform-architect.md
│   ├── openmetadata-extension-engineer.md
│   ├── openmetadata-kvkk-reviewer.md
│   └── openmetadata-governance-assessor.md
├── commands/
│   ├── openmetadata-integration-plan.md
│   ├── openmetadata-gap-plan.md
│   ├── openmetadata-kvkk-audit.md
│   └── openmetadata-assess.md
└── skills/
    ├── openmetadata-architecture/         components-and-deployment, entity-model-and-apis, extension-points
    ├── openmetadata-platform-integration/ connector-matrix, lineage, ingestion-workflows, missing-connectors-and-workarounds
    ├── openmetadata-gap-closing/          gap-catalog, closing-routes-part-a, closing-routes-reference-data-sharing, extending-openmetadata
    ├── openmetadata-kvkk-compliance/      obligations-map, modelling-in-openmetadata, d14-gaps-and-routes
    ├── openmetadata-governance-assessment/ rubric, assessment-procedure, openmetadata-results
    └── openmetadata-capability-design/    candidate-pool, novelty-method, design-template
```

Her skill'de `SKILL.md` özet ve karar tablosunu, `references/` ayrıntıyı taşır. `references/sources.md` kaynakları, araştırmadan farklılaşan düzeltmeleri ve bilerek dışarıda bırakılanları listeler. Bir iddianın neden öyle olduğunu merak ettiğinizde önce oraya bakın.

## 7. İpuçları ve sınırlamalar

- **Sürüm sabittir.** Bulgular OpenMetadata 1.13.3 içindir. Bazı boşluklar `main` dalında ya da 2.0'da kapanıyor (politika ajanı, görev son tarihleri, süreli erişim talepleri). Yeni sürümde bir şey inşa etmeden önce yeniden kontrol edin.
- **Kodda var olmak, kullanılıyor olmak değildir.** Bir alanın şemada bulunması zorunlu tutulduğu anlamına gelmez. KVKK skill'i "zorlanan" ve "yalnız uzlaşma" ayrımını bu yüzden yapar.
- **Katalog veri yolunda değildir.** OpenMetadata politikaları katalog metadata'sına erişimi yönetir, veriye erişimi yönetmez. Satır/kolon erişimi ve maskeleme Ranger'da uygulanır. Plugin bu iki tarafı senkron tutmanın yollarını anlatır.
- **Yalnız kod okunarak doğrulanan yollar.** Kafka'ya özel tüketici sınıfıyla olay yayımlama ve bazı eklenti sınıfları yalnız kod okunarak doğrulandı, çalıştırılarak test edilmedi. Skill'ler bunu açıkça belirtir.
- **Skorlar araştırma bağlamındadır.** Boşlukların etki puanları belirli bir platform için verildi. Kendi bağlamınızda yeniden puanlayın.
- **Yeni yetenek skorlaması yapılmadı.** C01–C19 için ağırlıklı skorlama araştırmada yürütülmedi. Skill yöntemi ve boş çalışma sayfasını verir, sıralama uydurmaz.
- **Kapsam dışı.** Kapalı kaynak ürünler ve kuruma özgü gereksinimler plugin'e alınmadı.
- **KVKK içeriği hukuki görüş değildir.** Madde ve süreler resmî metinlerden doğrulandı. Uygulamayı kurumunuzun hukuk birimiyle teyit edin.

## 8. Bakım

- Yeni bir OpenMetadata sürümüne geçerken her skill'in giriş paragrafındaki sürüm ve commit bilgisini güncelleyin. Önce `openmetadata-governance-assessment` prosedürüyle değişen yetenekleri yeniden ölçün, sonra etkilenen skill'leri düzeltin.
- Kanıt referansları (`dosya:satır`) sürüme bağlıdır. Bir referans tuttuğunda kanıt geçerlidir, tutmadığında iddia yeniden doğrulanmalıdır.
- Plugin `build-platform-plugins.ps1` / `.sh` tarafından ezilmez. Kategori kaydı iki script'te `openmetadata` → `governance` olarak durur.
- İlgili dokümanlar: [README.md](README.md), [_TEMPLATE.md](_TEMPLATE.md).
