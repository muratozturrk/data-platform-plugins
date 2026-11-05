# qa-test-strategy

> HKM veri platformu için QA süreci ve test stratejisi: yazılım test yaşam döngüsü (STLC) ve test case yaşam döngüsü, test stratejisi/planı/raporu dokümanları, test suite taksonomisi, gereksinim izlenebilirliği (RTM) ve kapsam, öncelik × önem derecesiyle defect yönetimi, fonksiyonel olmayan ve performans testleri, QA metrikleri, platformun katmanlarına göre test envanteri ve unit/integration/e2e test yazma rehberleri.

| | |
| --- | --- |
| Sürüm | 0.1.1 |
| Kategori | testing |
| Kaynak | `plugins/qa-test-strategy/` |
| Kaynak materyal | Ekibin QA notları ve test case yaşam döngüsü diyagramı, proje teknoloji listesi (`HKM_kullaniacak_tum_teknolojiler.xlsx`, 11 katman / ~80 araç), ISTQB Foundation ve ISO/IEC 25010, araç dokümanları ve 2026 veri mühendisliği test pratikleri (plugin `README.md` → Sources) |

## 1. Ne işe yarar?

Plugin, veri platformunda "hangi testlere ihtiyacımız var, nasıl yazacağız, nasıl izleyip raporlayacağız" sorularını tek bir yerde toplar. İki parçadan oluşur:

- **Genel QA süreci:** STLC'nin altı fazı, test case yaşam döngüsü (gereksinim → ortam/araç → test case → eşleme → review → koşum → regresyon → inceleme), strateji ile plan farkı, test seviyeleri/tipleri/suite'ler, RTM, defect yaşam döngüsü, fonksiyonel olmayan testler ve QA metrikleri. Ekibin QA notlarındaki kavramlar ISTQB ve ISO/IEC 25010 diliyle yazılmış.
- **Platforma özgü katman:** HKM yığınının her katmanı için test taksonomisi (ingestion/CDC → işleme → lakehouse → veri kalitesi → katalog/yönetişim → güvenlik/gizlilik → sunum → portal/API → operasyon), 112 testlik başlangıç envanteri ve gerçek açık kaynak bileşenlerle unit, integration ve e2e test yazma rehberleri.

Platform kuralları agent ve skill'lere gömülü: veri üründür, yani testler kodun yanında veri doğruluğunu da (şema, hacim, tekillik, bütünlük, tazelik, iş kuralları) kapsar. Her bileşenin iki kurumlu bir izolasyon testi olur. KVKK gizliliği kendi `privacy` suite'i ve release kapısı olan bir kalite özelliğidir. Açık kaynak motorlar her ay yükseltildiği için her motorun bir uyumluluk/regresyon suite'i vardır.

İlgili plugin'ler: `tdd-workflows` (red/green/refactor), `unit-testing` (test üretimi), `spark-property-based-testing`, `data-governance` (DQ SLA'ları), `coding-standards` (test kodu kuralları), `naming-conventions` (id ve suite adları).

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install qa-test-strategy@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/qa-test-strategy`

Gereksinimler:
- Skill, agent ve komutlar için ek bir şey gerekmez.
- `examples/rtm_from_results.py` için Python 3 ve `PyYAML`.
- Seviye örnekleri kendi bağımlılıklarını ister: `unit/` için `pytest`, `pyspark`, `chispa`, `apache-airflow`; `integration/` için Docker, `testcontainers`, `httpx`, `psycopg`, `kafka-python`, `trino`; `e2e/` için `faker`, `httpx`, `trino`, `pyyaml` ve çalışan bir E2E ortamı.

## 3. İçerik

### Skill'ler

| Skill | Ne zaman devreye girer | İçerdiği konular |
| --- | --- | --- |
| `stlc-and-test-case-lifecycle` | Test sürecini fazlara ayırmak, bir test case'in yaşam döngüsünü kurmak | STLC'nin 6 fazı (gereksinim analizi, planlama, tasarım, ortam, koşum, kapanış), ekip diyagramındaki test case yaşam döngüsü, platforma özgü faz notları |
| `test-planning-and-strategy` | Test stratejisi veya planı yazmak/incelemek | Strateji ile plan farkı; strateji içeriği (organizasyon, risk → test tipi, otomasyon politikası, ortamlar, araçlar, kapsam hedefleri); plan içeriği |
| `test-suite-taxonomy` | Testleri adlandırmak, suite'lere ayırmak, bir koşum için suite seçmek | Seviye / tip / yöntem / koşum biçimi boyutları; amaca göre suite'ler (smoke, sanity, regression, UI, API, performance, security, data-quality, upgrade, DR); `S-{amaç}` ve `T-{alan}-{nnn}` adlandırması |
| `coverage-and-traceability` | İzlenebilirlik kurmak veya denetlemek | Kapsam raporu, RTM (gereksinim → test case → koşum → defect), test case yönetimi, kod ve veri kapsamı, araç seçenekleri (Jira/Zephyr/Xray, TestRail, Azure DevOps, repo içinde YAML) |
| `defect-management` | Defect sürecini kurmak veya yürütmek | Defect / error / failure / change request tanımları, öncelik ve önem derecesi ölçekleri, yaşam döngüsü ve yeniden açma kuralı, triage, kalite borcu, metrikler |
| `nonfunctional-and-performance-testing` | Performans, güvenlik, erişilebilirlik gibi testleri planlamak | Fonksiyonel / fonksiyonel olmayan ayrımı, 5 adımlı süreç, test tipleri ve platform hedefleri, load/stress/endurance/volume/spike/scalability, araçlar |
| `qa-metrics-and-reporting` | Metrik tanımlamak, rapor yazmak, release kararı vermek | Sonuç ve öngörü metrikleri, 12 metrikli platform seti (formülleriyle), test raporu içeriği, varsayılan release karar kuralı |
| `data-platform-test-inventory` | Bir bileşen veya katman için hangi testlerin gerektiğini çıkarmak | Her bileşen için ortak testler ve katman katman test taksonomisi; suite eşleme varsayılanları (smoke, regression-fast, regression-full, release) |
| `unit-testing-data-platform` | Unit test yazmak | Saf dönüşüm fonksiyonları, PySpark (pytest + chispa + Hypothesis), Flink test harness'leri, dbt ≥ 1.8 unit testleri, Trino SQL, Airflow DAG bütünlüğü, kontrol düzlemi derleyicisi golden file'ları, Rego/CEL/Ranger kuralları, gizlilik fonksiyonları |
| `integration-testing-data-platform` | Gerçek bileşenlerle integration testi yazmak | İki bant: PR'da Testcontainers (≤ 15 dk), gece PR başına geçici Kubernetes namespace'i (≤ 60 dk); izolasyon, kontrat (Karapace/OpenAPI/SPI/veri kontratı), CDC, katalog, idempotency, lineage testleri; flake politikası |
| `e2e-testing-data-platform` | Uçtan uca senaryo tasarlamak ve koşmak | 10 golden-path senaryosu (E2E-01…E2E-10), iş seviyesinde doğrulama, sentetik fixture, polling, ortam stratejisi, release kapıları, flakiness kontrolü |

### Agent'lar

| Agent | Model | Ne yapar | Ne zaman kullanılır |
| --- | --- | --- | --- |
| `qa-strategist` | opus | Hedefi (bileşen, pipeline, release) ve risklerini belirler; risk → test tipi → suite → otomasyon → ortam → sahip eşlemesini kurar; `T-{alan}-{n}` id'li envanter, giriş/çıkış kriterleri, kapsam hedefleri ve kapsam dışı listesini üretir. | Yeni bir bileşene başlarken, release planlarken, "hangi testlere ihtiyacımız var" sorusunda |
| `test-case-designer` | sonnet | Ekip standardındaki yapıyla test case yazar ve inceler: başlık alanları ve adım tablosu (gereksinim, defect ve otomasyon id'leri adım seviyesinde). Otomasyon iskeleti (pytest, dbt test, GE suite) ve review bulguları üretir. | Gereksinimleri veya bir envanteri somut test case'lere çevirirken |
| `defect-triager` | sonnet | Raporları doğrular ve sınıflar; platform önem derecesi rehberiyle önem ve öncelik verir; test ve gereksinime bağlar, sahibe yönlendirir, kalite borcunu günceller. | Bug raporlarını işlerken, triage toplantısında, defect metriği raporlarken |

### Komutlar

| Komut | Argüman | Ne yapar |
| --- | --- | --- |
| `/test-inventory` | Hedef: bileşen listesi, gereksinimler, mimari notlar | Hedefi test edilebilir birimlere ayırır, 7 kalite özelliğine göre riskleri çıkarır, id/tip/seviye/suite/araç/ortam/sahip/öncelikli test listesini üretir. Smoke ve regresyon alt kümelerini, koşum takvimini (PR, gece, release, motor yükseltmesi) ve boşlukları verir; istenirse `test-inventory.yaml` olarak dışa aktarır. |
| `/test-strategy` | Bağlam | `examples/test-strategy-template.md` ile platform veya bileşen için test stratejisi yazar. |
| `/test-plan` | Planlama bağlamı | `examples/test-plan-template.md` ile release/proje test planı yazar: kapsam, kabul kriterleri, riskler, ortamlar, ekip, takvim, giriş/çıkış kriterleri. |
| `/test-report` | Koşum verisi: sonuçlar, defect'ler, kapsam | `examples/test-report-template.md` ile iterasyon/release raporu yazar ve go / şartlı go / no-go önerisi verir. |
| `/defect-triage` | Defect raporları | `defect-triager` prosedürünü uygular: triage tablosu, kararlar ve metrik özeti. |

### Örnekler (examples/)

| Dosya | Ne yapar | Ek bağımlılık |
| --- | --- | --- |
| `test-strategy-template.md`, `test-plan-template.md`, `test-report-template.md` | Komutların kullandığı doküman şablonları | — |
| `test-case-template.yaml` | Zorunlu alanlar ve iki dolu örnek (veri kalitesi `T-dq-001`, izolasyon `T-trino-014`) | — |
| `test-inventory.yaml` | 112 testlik başlangıç envanteri (`kind: TestInventory`): id, alan, seviye, suite, araç, öncelik ve koşum takvimi | — |
| `rtm-template.csv` | RTM kolonları ve örnek satırlar | — |
| `defect-matrix.md` | Önem × öncelik rehberi ve defect şablonu | — |
| `rtm_from_results.py` | Test case YAML'larından ve JUnit XML sonuçlarından RTM/kapsam CSV'si üretir | `PyYAML` |
| `unit/` | PySpark + chispa dönüşüm testi, dbt unit test YAML'ı, Airflow DAG bütünlük testi, derleyici golden-file testi, OPA saklama politikası testleri | `pytest`, `pyspark`, `chispa`, `apache-airflow`, `opa` |
| `integration/` | Testcontainers `conftest.py` (PostgreSQL, Kafka KRaft, Karapace, Debezium Connect, Nessie, Keycloak, Trino), CDC sıralama, Karapace uyumluluk ve Trino izolasyon testleri, PR başına namespace için Argo CD ApplicationSet, hızlı/tam bantlı CI workflow'u | Docker, `testcontainers` ve istemci kütüphaneleri |
| `e2e/` | E2E-02 batch pipeline senaryosu (YAML), polling ve iş seviyesi doğrulamalı senaryo koşucusu, deterministik fixture üreticisi, E2E ortamını yeniden kuran gece workflow'u | `faker`, `httpx`, `trino`, E2E ortamı |

## 4. Hızlı başlangıç

1. Plugin'i kurun (bölüm 2).
2. Test envanterini çıkarın: `/test-inventory Trino + Gateway katmanı, 3 kurum, Ranger satır/kolon politikaları`.
3. Çıktıyı `examples/test-inventory.yaml` ile karşılaştırıp repo'nuza ekleyin.
4. Kritik testleri somut case'lere çevirin: "`T-trino-014` ve `T-trino-015` için test case yaz ve pytest iskeleti çıkar." (`test-case-designer`)
5. CI'da RTM üretin: `python rtm_from_results.py tests/cases junit-results/ requirements.yaml > rtm.csv`. `requirements.yaml`, `- {id: REQ-..., priority: ...}` biçiminde bir listedir. Kapsam yüzdesi stderr'e yazılır.

## 5. Kullanım senaryoları

**Yeni bileşen için test stratejisi**
- Durum: Platforma ClickHouse sunum katmanı ekleniyor; hangi testlerin gerektiği belli değil.
- Ne yazarsınız: "/test-strategy ClickHouse serving katmanı: Flink'ten CDC ile beslenecek, kurum başına DB, Grafana ve Superset okuyacak."
- Ne olur: `qa-strategist` kalite özelliklerini ve ürün risklerini çıkarır; `data-platform-test-inventory` ve `test-planning-and-strategy` skill'lerini kullanır.
- Çıktı: Risk → test tipi → suite eşlemesi, otomasyon politikası, ortamlar, kapsam hedefleri, kapsam dışı maddeler.

**Release testi ve go/no-go kararı**
- Durum: Release sonunda sonuçlar ve açık defect'ler var; karar verilecek.
- Ne yazarsınız: "/test-report" ve ardından JUnit özetini, açık defect listesini ve RTM'i yapıştırırsınız.
- Ne olur: `qa-metrics-and-reporting` skill'indeki metrikler hesaplanır ve varsayılan karar kuralı uygulanır. Kurala göre go için şunlar gerekir: smoke ve regresyon yeşil, açık Blocker/Critical defect yok, Critical/High gereksinimlerde %100 kapsam, izolasyon ve güvenlik kapıları yeşil, performans bütçeleri karşılanmış ya da resmi olarak muaf tutulmuş.
- Çıktı: Sonuç özeti, kapsam, önem derecesine göre defect'ler, trendler, riskler ve go / şartlı go / no-go önerisi.

**Defect triage toplantısı**
- Durum: Haftanın 15 bug raporu birikti.
- Ne yazarsınız: "/defect-triage" ve raporlar.
- Ne olur: `defect-triager` her raporun tekrar üretilebilir olup olmadığını kontrol eder, change request'leri ayırır. Platform rehberine göre önem derecesi verir: örneğin kurumlar arası veri sızıntısı veya maskelenmemiş kişisel veri her zaman Blocker'dır.
- Çıktı: Triage tablosu, karar kaydı, kalite borcu eklemeleri, açık defect / reopen / ortalama çözüm süresi özeti.

**Integration testi yazmak**
- Durum: Debezium → Kafka CDC akışı için PR'da çalışacak bir test gerekiyor.
- Ne yazarsınız: "Debezium CDC için insert/update/delete sırasını doğrulayan Testcontainers testi yaz."
- Ne olur: `integration-testing-data-platform` skill'i kullanılır; `examples/integration/conftest.py` ve `test_cdc_debezium.py` temel alınır.
- Çıktı: Kendi fixture'ını kuran, benzersiz topic adları kullanan ve sink'teki gözlemlenebilir sonucu doğrulayan pytest modülü.

**Uçtan uca senaryo**
- Durum: KVKK silme akışının uçtan uca kanıtlanması isteniyor.
- Ne yazarsınız: "E2E-05 silme senaryosunu YAML olarak tasarla ve koşucuyu yaz."
- Ne olur: `e2e-testing-data-platform` skill'i kullanılır; `examples/e2e/` yapısı izlenir.
- Çıktı: Fides DSR → silme/yeniden yazma → snapshot expire → Nessie GC → doğrulama taraması → immudb kaydı adımları ve iş seviyesindeki başarı ölçütü.

## 6. Dosya yapısı

```text
plugins/qa-test-strategy/
├── .claude-plugin/plugin.json
├── README.md                              # içerik özeti ve kaynaklar
├── agents/
│   ├── qa-strategist.md                   # platform yığını ve QA temelleri
│   ├── test-case-designer.md              # test case standardı ve otomasyon araçları
│   └── defect-triager.md                  # önem derecesi rehberi ve triage prosedürü
├── commands/
│   ├── test-inventory.md, test-strategy.md, test-plan.md
│   └── test-report.md, defect-triage.md
├── skills/                                # 11 skill (bkz. bölüm 3)
└── examples/
    ├── *-template.md, test-case-template.yaml, rtm-template.csv, defect-matrix.md
    ├── test-inventory.yaml, rtm_from_results.py
    ├── unit/
    ├── integration/
    └── e2e/
```

Süreç soruları için `stlc-and-test-case-lifecycle` ve `test-planning-and-strategy`'ye bakın. "Bu bileşen için hangi testler" sorusu için `data-platform-test-inventory` ve `examples/test-inventory.yaml`'a, test yazarken ilgili seviyenin skill'ine ve `examples/` alt klasörüne bakın.

## 7. İpuçları ve sınırlamalar

- **Örnek testler canlı ortamda koşturulmadı.** Python dosyaları ve YAML'lar yalnızca sözdizimi açısından kontrol edildi. `rtm_from_results.py` örnek test case ve sahte JUnit verisiyle denendi ve doğru çalıştı.
- **`unit/test_compiler_golden.py` için derleyici modülünü siz belirlersiniz.** `COMPILER_MODULE = "hkm_control_plane.compiler"` satırını kendi derleyicinizin giriş noktasıyla değiştirin; modül bulunamazsa test hata vermek yerine atlanır. Paketinize `platform` adını vermeyin, Python'un standart `platform` modülüyle çakışır. `--update-golden` seçeneğini kullanmak için `conftest.py`'de `parser.addoption("--update-golden", action="store_true")` ile kaydedin.
- **RTM script'i yalnızca `status: active` olan case'leri sayar.** Test case YAML dosyaları case listesi olmalıdır. JUnit'te test adının içinde `T-...` id'si geçmelidir (ör. `test_x[T-dq-001]`). Eşleşmeyen testler RTM'e girmez.
- **Platforma bağlı içerik.** Yığın listesi, önem derecesi örnekleri, alan kısaltmaları (`cp`, `dq`, `trino`, `ch`…) ve E2E senaryoları HKM teknoloji listesine göre yazıldı. Başka bir platformda kullanırken `qa-strategist`'teki yığın listesini ve `data-platform-test-inventory`'yi uyarlayın.
- **Sayısal hedefleri test planı belirler.** Performans ve fonksiyonel olmayan hedefler skill'lerde başlangıç önerisidir; bağlayıcı değerler her release'in test planında yazılmalı.
- Integration testlerini paylaşılan staging'de veya üretim verisiyle koşmayın; skill bunu açıkça yasaklar. İki kez flake olan test karantinaya alınır ve 2 haftalık süreyle bir issue açılır.

## 8. Bakım

- Plugin bu repoda elle yazılmıştır, upstream'den çekilmez. `build-platform-plugins.ps1` ve `build-platform-plugins_linux.sh` onu `testing` kategorisiyle `marketplace.json`'a ekler.
- Teknoloji listesi değişince güncellenecek yerler: `qa-strategist`'teki yığın paragrafı, `data-platform-test-inventory` katmanları, `examples/test-inventory.yaml` ve E2E senaryoları. Ardından `plugin.json` sürümünü artırın.
- Hızlı kontroller:
  - `python -c "import ast,sys;[ast.parse(open(f,encoding='utf-8').read()) for f in sys.argv[1:]]" $(find plugins/qa-test-strategy/examples -name '*.py')`
  - `python -c "import yaml,sys;[list(yaml.safe_load_all(open(f,encoding='utf-8'))) for f in sys.argv[1:]]" $(find plugins/qa-test-strategy/examples -name '*.y*ml')`
- İlgili dokümanlar: [spark-property-based-testing.md](spark-property-based-testing.md), [data-governance.md](data-governance.md), [project-nessie.md](project-nessie.md).
