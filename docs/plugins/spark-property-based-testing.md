# spark-property-based-testing

> PySpark için Hypothesis tabanlı property-based testing: null, skew ve uç değerli DataFrame üretimi, iç içe tipleri de kapsayan schema contract testleri, Great Expectations 1.x içerik kontrolleri ve transformation invariant'ları (satır sayısı, toplam, idempotency, join, window); pytest plugin'i, CLI ve HTML raporu olan gömülü `spark-prop-testing` kütüphanesiyle.

| | |
| --- | --- |
| Sürüm | 0.1.0 (gömülü kütüphane: `spark-prop-testing` 1.1.0) |
| Kategori | testing |
| Kaynak | `plugins/spark-property-based-testing/` |
| Kaynak materyal | İç kütüphane `lib/spark_prop_testing`, plugin'e düzeltilmiş ve GX 1.x'e taşınmış haliyle |

## 1. Ne işe yarar?

Örnek tabanlı Spark testleri, yazanın aklına gelen birkaç satırı dener. Null, boş DataFrame, tekrar eden key, uç tarih veya `decimal(5,2)`'nin sınırındaki `999.99` gibi girdiler çoğu zaman test edilmez. Bu plugin, testi "şu girdi için şu çıktı" yerine **her girdi için geçerli olması gereken kural** (property) olarak yazdırır. Hypothesis, schema'ya uygun yüzlerce DataFrame üretir. Kural bozulursa hatayı **en küçük girdiye indirir (shrink)** ve onu raporlar.

Plugin'in üç katmanı var:

1. **Gömülü kütüphane (`lib/spark_prop_testing`):** DataFrame stratejileri, `SchemaValidator`/`SchemaEvolutionChecker`, isteğe bağlı Great Expectations katmanı, `TransformationPropertyTester` ve hazır property'ler, `@spark_property` / `@prop` decorator'ları, pytest plugin'i, `spark-prop-test` CLI'ı ve HTML raporu.
2. **Skill'ler:** her katman için kavram, API, çalıştırılmış örnek testler ve sorun giderme tabloları.
3. **Agent'lar ve komut:** kodu okuyup property'leri seçen, testleri yazıp çalıştıran writer; mevcut testlerdeki zayıf property'leri bulan reviewer.

Kaynak kütüphanede çalışmayı engelleyen hatalar vardı. Örneğin GX 0.x API'si kullanılıyordu, Spark action'ları Hypothesis'in 200 ms deadline'ını aşıyordu, bazı property'ler her zaman geçiyordu (tautoloji), `NullConfig` %1'in altındaki oranları "hiç" sayıyordu ve iç içe tiplerde `array<int>` ile `array<string>` "EXACT" görünüyordu. Plugin'deki kopyada bunlar düzeltildi. Davranış skill'lerde güncel haliyle anlatılıyor.

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install spark-property-based-testing@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/spark-property-based-testing`

Kütüphaneyi test projesine kurmak:

```bash
pip install -e "plugins/spark-property-based-testing/lib[ge,dev]"   # GX gerekmiyorsa "ge"yi çıkarın
# veya: PYTHONPATH=plugins/spark-property-based-testing/lib
```

Gereksinimler:
- Python 3.9+, PySpark 3.3+, Java 8/11/17. Bağımlılıklar: `hypothesis`, `returns`, `jinja2`, `pyyaml`, `click`, `rich`.
- Great Expectations katmanı için `great-expectations>=1.3` (`[ge]` extra). Strateji, schema ve transformation katmanları GX olmadan çalışır.
- **Windows:** PySpark 3.5 Python worker'ı Python 3.12'de çöküyor ("Python worker exited unexpectedly"). Python 3.11 kullanın.

## 3. İçerik

### Skill'ler

| Skill | Ne zaman devreye girer | İçerdiği konular |
| --- | --- | --- |
| `spark-property-testing-foundations` | Spark pipeline'ı için property testine başlarken; yavaş, flaky veya tekrarlanamayan testlerde | Property taksonomisi (invariant, idempotency, metamorphic, oracle, determinism), generation/shrinking/replay, Spark'a uygun Hypothesis ayarları ve profilleri, deterministik test SparkSession'ı |
| `spark-dataframe-strategies` | Testin girdi stratejisini yazarken veya düzeltirken | Her Spark tipi için `strategy_for_type`, `spark_dataframes` + `DataFrameConfig`/`ColumnConfig` (`custom_strategy`, `unique`, `null_config`), `NullConfig`, skew/edge case/id yardımcıları, profiller, foreign key gibi ilişkili kolonlar |
| `spark-schema-contract-testing` | Bir pipeline'ın çıktı schema'sını korunması gerektiğinde | `SchemaValidator` (eksik/fazla kolon, `strict_types`, nullable contract), 4 seviyeli `check_type_compatibility` kural tablosu, `SchemaEvolutionChecker` ile breaking/güvenli değişiklik, CI kapısı |
| `spark-data-quality-property-testing` | Veri kalitesi kurallarını GX ile ifade ederken | `ExpectationSuiteBuilder` + `ColumnExpectationSpec`, `GEPropertyValidator`, `auto_generate_suite`, `DataQualityPropertySuite` (`run` vs `run_property_loop`), suite'e uygun strateji, GX 1.x tuzakları |
| `spark-transformation-property-testing` | DataFrame → DataFrame fonksiyonlarını test ederken | `TransformationPropertyTester` (`run(max_examples, phases)`, shrink edilmiş `failure_input`), hazır property'ler (`row_count_*`, `no_new_nulls_introduced`, `make_sum_linearity_property`, `make_idempotency_property`, join ve window property'leri), metamorphic ve pipeline property'leri |
| `spark-prop-testing-toolkit` | Kurulum, pytest plugin, CLI, YAML, CI | `@spark_property`, `@prop`, pytest seçenekleri, `spark`/`spark-ci` profilleri, `SPARK_PROP_MASTER`, `spark-prop-test init/run/validate`, GitHub Actions tarifi, Windows notları |

### Agent'lar

| Agent | Model | Ne yapar | Ne zaman kullanılır |
| --- | --- | --- | --- |
| `spark-property-test-writer` | opus | 1. Girdi schema'sını çıkarır. 2. Invariant'ları seçer (satır sayısı, kolon/tip koruma, null contract, toplam korunumu/linearity, idempotency, join key koruma, window rank, determinism). 3. Gerçekçi null, skew ve uç değerli stratejiler kurar. 4. Testleri yazar, çalıştırır ve hataları analiz eder. | "Bu transformation'ı test et"; örnek tabanlı testlerin uç durumları kaçırdığı yerler |
| `spark-property-test-reviewer` | sonnet | Mevcut Spark testlerini inceler: tautolojik property'ler, uç durumlara ulaşamayan stratejiler, Spark'la flaky Hypothesis ayarları, sıraya bağlı assert'ler, float eşitliği, eksik null/skew/boş girdi kapsamı. Yerine somut property testleri önerir. | Testler yazıldıktan sonra; testler flaky/yavaşsa veya production hatasını yakalayamadıysa |

### Komutlar

| Komut | Argüman | Ne yapar |
| --- | --- | --- |
| `/write-spark-property-tests` | Dosya yolu, fonksiyon adı, yapıştırılmış kod veya pipeline açıklaması | Kütüphanenin import edilebildiğini sağlar. Projede Spark testi varsa önce reviewer'ı çalıştırır, sonra writer'a testleri yazdırıp çalıştırır. Sonunda property'leri, minimal girdileriyle bulunan gerçek hataları, bilerek atlanan property'leri ve yerel/CI çalıştırma komutlarını özetler. |

### Araçlar (lib/)

| Dosya / komut | Ne yapar | Spark/ek bağımlılık gerekir mi |
| --- | --- | --- |
| `spark-prop-test init` | YAML config şablonu yazar | Hayır |
| `spark-prop-test run` | YAML'daki her suite için DataFrame üretir, varsa `transform`'u uygular, schema + GX + property katmanlarını çalıştırır. `-n`, `--suite`, `--report`, `--no-shrink` seçenekleri var. | Spark; `ge_expectations` varsa GX |
| `spark-prop-test validate` | Parquet/CSV/JSON dosyasını bir schema'ya karşı doğrular. `--strict-types` ile tip farkları da hata sayılır. | Spark |
| pytest plugin (`spark_prop_testing.plugin`) | Session-scoped `spark` fixture'ı, `spark` Hypothesis profilini otomatik yükler. `--spark-prop-report`, `--spark-prop-report-path`, `--spark-prop-max-examples`, `--spark-prop-config` seçenekleri. | Spark |

## 4. Hızlı başlangıç

1. Kütüphaneyi kurun: `pip install -e "plugins/spark-property-based-testing/lib[dev]"`.
2. İlk property'yi yazın:

   ```python
   from pyspark.sql import functions as F
   from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
   from spark_prop_testing import spark_property

   SCHEMA = StructType([StructField("id", IntegerType(), False),
                        StructField("amount", DoubleType(), True)])

   @spark_property(schema=SCHEMA, max_examples=25)
   def test_filter_never_adds_rows(spark, df):
       assert df.filter(F.col("amount") > 0).count() <= df.count()
   ```

3. Çalıştırın: `pytest tests/`. `spark` profili otomatik yüklenir: deadline yoktur, Spark health check'leri susturulur.
4. CI'da: `pytest tests/ --hypothesis-profile=spark-ci --spark-prop-max-examples 100 --spark-prop-report`.
5. Kendi transformation'ınız için Claude Code'da: `/write-spark-property-tests src/pipeline/clean_orders.py`.

## 5. Kullanım senaryoları

**Yeni bir transformation'a test yazmak**
- Durum: `clean_orders(df)` fonksiyonu var, hiç testi yok.
- Ne yazarsınız: `/write-spark-property-tests src/pipeline/orders.py:clean_orders`
- Ne olur: writer girdi schema'sını çıkarır, örneğin "satır sayısı artmaz", "order_id tekil", "amount null'a dönüşmez" ve "iki kez uygulamak sonucu değiştirmez" property'lerini seçer, testi yazar ve çalıştırır.
- Çıktı: `tests/` altında property testleri. Hata bulunduysa en küçük failing girdisiyle birlikte raporlanır.

**Zayıf testleri iyileştirmek**
- Durum: Spark testleri geçiyor ama production'da null kaynaklı bir hata kaçtı.
- Ne yazarsınız: "tests/spark altındaki testleri spark-property-test-reviewer ile incele"
- Ne olur: reviewer her zaman geçen property'leri, null üretemeyen stratejileri ve `collect() == collect()` gibi sıraya bağlı karşılaştırmaları bulur.
- Çıktı: dosya:satır bazında bulgular ve her biri için property tabanlı yerine geçecek test.

**Schema değişikliğini CI'da durdurmak**
- Durum: pipeline'ın yeni sürümü bir kolonu `NOT NULL` yapıyor ya da `long`'u `int`'e daraltıyor.
- Ne yazarsınız: "eski ve yeni ORDER_SCHEMA arasında breaking change kontrolü yapan bir test yaz"
- Ne olur: `spark-schema-contract-testing` skill'i devreye girer. `SchemaEvolutionChecker` breaking değişiklikleri listeler, `SchemaValidator(strict_types=True)` lossy tip farklarını hata sayar.
- Çıktı: CI'da kırmızıya düşen bir schema contract testi.

**Veri kalitesi kurallarını YAML ile çalıştırmak**
- Durum: ekip kodu değiştirmeden kural eklemek istiyor.
- Ne yazarsınız: `spark-prop-test init`, ardından YAML'a `transform: myproject.pipeline:clean_orders` ve `ge_expectations` ekleyip `spark-prop-test run --report`.
- Ne olur: her suite için üretilen girdi transform'dan geçer, çıktı schema ve GX kurallarıyla kontrol edilir.
- Çıktı: konsolda katman bazında PASS/FAIL tablosu, HTML rapor; geçmezse exit code 1.

## 6. Dosya yapısı

```text
plugins/spark-property-based-testing/
├── .claude-plugin/plugin.json
├── agents/
│   ├── spark-property-test-writer.md
│   └── spark-property-test-reviewer.md
├── commands/write-spark-property-tests.md
├── lib/                              # gömülü kütüphane (pip install -e)
│   ├── pyproject.toml, README.md, conftest.py
│   ├── spark_prop_testing/
│   │   ├── strategies/               # DataFrame ve tip stratejileri, null, skew, profiller
│   │   ├── schema/                   # SchemaValidator, tip uyumu, evolution
│   │   ├── ge_integration/           # GX 1.x köprüsü (isteğe bağlı)
│   │   ├── transformations/          # TransformationPropertyTester, hazır property'ler
│   │   ├── data_quality_suite.py     # schema + GX + property katmanlarını birleştirir
│   │   ├── decorators.py, plugin.py, cli.py, config.py, _hypothesis.py
│   │   └── reporting/html_reporter.py
│   └── tests/                        # kütüphanenin kendi testleri
└── skills/<skill>/
    ├── SKILL.md                      # özet, karar tablosu, anti-pattern'ler
    ├── references/*.md               # ayrıntılı API ve sorun giderme
    └── examples/test_*.py            # çalıştırılmış pytest örnekleri
```

API'nin ne yaptığını görmek için önce ilgili skill'in `references/` dosyasına, çalışan bir örnek için `examples/` dosyasına bakın. Kesin davranış için kaynak `lib/spark_prop_testing/`.

## 7. İpuçları ve sınırlamalar

- **Hız:** her Hypothesis örneği bir veya birkaç Spark action'ı demek. Unit testlerde `max_examples` 10-25, `min_rows`/`max_rows` küçük, `spark.sql.shuffle.partitions` 1-4 tutun. Hatası bilinen bir testte shrink'i atlamak için `phases=(Phase.explicit, Phase.generate)` (`TransformationPropertyTester.run`, `QualitySuiteConfig`) veya CLI'da `--no-shrink` kullanın.
- **Elle `@settings(suppress_health_check=[...])`** profildeki listeyi değiştirir, üstüne eklemez. Listeye `too_slow`, `data_too_large` ve `large_base_example`'ı kendiniz koyun.
- **GX beklentileri ile strateji uyumlu olmalı:** `unique`/`min_value` beklentisi varsa kolona `ColumnConfig(unique=True)` veya sınırlı bir strateji verin. Aksi halde test baştan başarısız olur.
- **`spark-ci` profili** kütüphane runner'larının örnek sayısını değiştirmez, çünkü runner'lar kendi değerini geçiriyor. CI'da ayrıca `--spark-prop-max-examples 100` verin. `derandomize=True` olduğu için bu profilde Hypothesis veritabanı kullanılmaz.
- **Bilinen açık sorun:** kütüphane runner'ları (`TransformationPropertyTester`, `DataQualityPropertySuite`) tek bir Hypothesis veritabanı anahtarını paylaşıyor. Kaydedilmiş bir failing örnek ilgisiz bir runner testinde tekrar oynatılabilir. Şüpheli bir durumda `.hypothesis/` klasörünü silin.
- **Windows:**
  - Python 3.11 kullanın.
  - Worker açılışı yavaşsa (antivirüs taraması) `SPARK_PROP_MASTER=local[1]` kullanın. Aksi halde "Python worker failed to connect back" hatası alınabilir.
  - Rapordaki semboller için `PYTHONUTF8=1` verin.
  - Timestamp stratejisi 1970-01-02'den başlar, çünkü negatif epoch Windows'ta `fromtimestamp` hatası verir.
- **Doğrulama:** kütüphane testleri ve skill örnekleri Windows'ta Spark 3.5.1 + Python 3.11 ile `local[1]` üzerinde çalıştırıldı. Kütüphane testlerinde 37 testten 36'sı geçiyor. Union additivity testi bu ortamda worker zaman aşımına takılıyor, kod hatası değil. Linux ve CI'da ayrıca denenmedi.

## 8. Bakım

- Kütüphane değişikliği `lib/spark_prop_testing/` altında yapılır. Davranış değişirse ilgili skill'in `SKILL.md`, `references/` ve `examples/` dosyaları birlikte güncellenir.
- Testleri çalıştırmak:

  ```bash
  cd plugins/spark-property-based-testing/lib
  SPARK_PROP_MASTER=local[1] SPARK_PROP_MAX_EXAMPLES=5 GX_ANALYTICS_ENABLED=false python -m pytest -q tests/
  # skill örnekleri (her dosyayı ayrı çalıştırmak bellek için daha güvenli):
  PYTHONPATH=. python -m pytest -q ../skills/spark-transformation-property-testing/examples/test_tester_api.py
  ```

- Plugin yapısını doğrulamak: repo kökünde `claude plugin validate .`.
- Sürüm: plugin `.claude-plugin/plugin.json`, kütüphane `lib/pyproject.toml` ve `spark_prop_testing/__init__.py` içindeki `__version__`.
- İlgili dokümanlar: [spark-code-optimizer.md](spark-code-optimizer.md) (PySpark performansı), [data-engineering-design-patterns.md](data-engineering-design-patterns.md).
