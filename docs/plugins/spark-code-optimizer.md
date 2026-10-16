# spark-code-optimizer

> PySpark kodunu sparkopt kural motoruyla analiz eder: join, aggregation, I/O ve partitioning, persistence (cache, localCheckpoint, checkpoint, StorageLevel), plan analizi ve karmaşık iş yükleri için öncelikli öneriler ve kod üretir.

| | |
| --- | --- |
| Sürüm | 0.1.0 |
| Kategori | data |
| Kaynak | `plugins/spark-code-optimizer/` |
| Kaynak materyal | `Desktop/spark/optimized_code_generation` (sparkopt 0.2.0), plugin'e düzeltilmiş kopyası konmuş haliyle |

## 1. Ne işe yarar?

Yavaş çalışan, OOM veren, spill yapan ya da skew'li PySpark job'larını iyileştirir. Arkasında **sparkopt** adlı bir kural motoru var. Motor, bir DataFrame'in **profiline** bakarak (boyut, partition sayısı, skew, DAG derinliği, reuse, iteratif/streaming olup olmadığı, executor belleği) 27 optimizasyon kuralından ("case") uyanları öncelik sırasıyla seçer ve hazır PySpark kodu üretir. Persistence önerileri birbiriyle çelişirse **PersistenceAdvisor** bunları tek bir karara indirir.

Plugin'in üç katmanı var:

1. **CLI ve script'ler:** `sparkopt_cli.py` Spark kurulu olmadan çalışır. Bir profil JSON'u ve/veya `df.explain()` çıktısını alıp eşleşen case'leri, persistence kararını ve kodu verir.
2. **Skill'ler:** her case için doğru Spark davranışını anlatan referanslar ve doğrulanmış PySpark örnekleri.
3. **Agent'lar:** kodu okur, profili çıkarır ya da tahmin eder, CLI'ı çalıştırır, kodla ilgisi olmayan önerileri eler ve kodu yeniden yazar.

Orijinal kütüphanede üretilen kodu çalışmaz hale getiren hatalar vardı. Örneğin PySpark'ta olmayan `StorageLevel.MEMORY_ONLY_SER` kullanılıyordu, `spark.memory.*` ayarları çalışma anında değiştirilmeye çalışılıyordu ve DSL'deki iteratif/streaming bayrakları etkisizdi. Bunlar plugin'deki kopyada düzeltildi; tam liste `skills/sparkopt-toolkit/references/library_fixes.md` içinde. Orijinal klasöre dokunulmadı.

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install spark-code-optimizer@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/spark-code-optimizer`

Gereksinimler:
- CLI ve testler: Python 3.10+, sadece standart kütüphane. **pyspark gerekmez.**
- `collect_profile.py` ve DSL (`SparkOptimizer`): canlı bir SparkSession (PySpark 3.3+).
- Skill örneklerini çalıştırmak için PySpark 3.4+. Bazı örnekler ayrıca Delta Lake veya Kafka ister.

## 3. İçerik

### Skill'ler

| Skill | Ne zaman devreye girer | Kapsadığı case'ler |
| --- | --- | --- |
| `spark-join-optimization` | Yavaş join, skew'li key, broadcast kararı | broadcast_join, skew_join (önce AQE, sonra salting), bucket_join, sort_merge_join |
| `spark-aggregation-optimization` | groupBy, countDistinct, window performansı | partial_agg, approx_distinct, window_opt, repartition_before_agg |
| `spark-io-and-partitioning` | Okuma/yazma, küçük dosyalar, partition sayısı, AQE | parquet_pushdown, coalesce_write, delta_zorder, repartition, coalesce, adaptive_qe |
| `spark-persistence-strategy` | cache, checkpoint, StorageLevel, unpersist | persistence_strategy, iterative_loop, fanout_cache, lineage_break, storage_level, hybrid_checkpoint, spill_prevention |
| `spark-complex-workloads` | ML pipeline, graph iterasyonu, çok aşamalı agg, incremental ETL, streaming × batch | ml_pipeline, graph_algorithm, multi_stage_agg, incremental_etl, cached_broadcast, staged_materialization, streaming_batch_hybrid |
| `spark-plan-analysis` | `explain()` çıktısını okumak | Exchange/shuffle, join stratejileri, cache sinyalleri, plan karmaşıklığı |
| `sparkopt-toolkit` | CLI/DSL kullanımı, profil JSON'u, özel kural yazmak | CLI, `collect_profile`, DSL, kural birleştirme (`&` `\|` `~`), kütüphane düzeltmeleri |

### Agent'lar

| Agent | Model | Ne yapar | Ne zaman kullanılır |
| --- | --- | --- | --- |
| `spark-code-optimizer` | opus | 1. Kodu okur, DataFrame'leri ve reuse noktalarını çıkarır. 2. Profili toplatır ya da tahmin eder. 3. CLI'ı `--advice` ile çalıştırır. 4. Kodla ilgisi olmayan case'leri eler. 5. `reviewed_example` dosyalarından yola çıkıp kodu yeniden yazar ve statik olarak kontrol eder. | Bir PySpark dosyasını hızlandırmak; cache, broadcast veya repartition kararları |
| `spark-plan-reviewer` | sonnet | `explain()` planını CLI'daki LineageInspector ile ayrıştırır. Her sinyali (Exchange, SortMergeJoin, InMemoryTableScan, CartesianProduct…) ilgili case'e ve düzeltmeye bağlar. | Plan yapıştırıldığında; "neden shuffle var", "neden SortMergeJoin seçildi" soruları |

### Komutlar

| Komut | Argüman | Ne yapar |
| --- | --- | --- |
| `/optimize-spark` | Dosya yolu, yapıştırılmış kod, `explain()` çıktısı veya job tarifi | Plan varsa önce `spark-plan-reviewer`'ı, sonra `spark-code-optimizer`'ı çalıştırır. Sonuçta değişiklikleri etkisine göre sıralar; runtime ve launch-time config'leri ayrı listeler; varsayımları ve her değişikliğin nasıl doğrulanacağını yazar. |

### Araçlar (`scripts/`)

| Dosya | Ne yapar | Spark gerekir mi? |
| --- | --- | --- |
| `sparkopt_cli.py` | Profil JSON'u ve/veya plan metnini alır; profili, plan sinyallerini, eşleşen case'leri (öncelik ve `reviewed_example` ile), PersistenceAdvisor kararını ve kodu verir. Çıktı `--format text\|json`. | Hayır |
| `collect_profile.py` | `collect(df, "isim", ...)` ile canlı bir DataFrame'den `isim.profile.json` ve `isim.plan.txt` üretir. | Evet |
| `sparkopt/` | Kütüphanenin düzeltilmiş kopyası: DataProfile, case'ler, PersistenceAdvisor, LineageInspector, CodeRenderer ve DSL. | Sadece DSL ve analyzer için |
| `sparkopt/tests/` | 134 test (120 orijinal + 14 regresyon). | Hayır |

CLI parametreleri:

| Parametre | Anlamı |
| --- | --- |
| `--profile FILE` (`-` = stdin) | DataFrame profili. Şema: `skills/sparkopt-toolkit/references/profile_json.md` |
| `--plan FILE` | `df.explain()` çıktısı. Depth, shuffle sayısı, streaming ve reuse plandan doldurulur; profilde verilen değer her zaman öncelikli. **Düz `df.explain()` kullanın**, `explain("formatted")` sayıları iki kat gösterir. |
| `--case NAME` (tekrarlanabilir) | Built-in bir isim verilirse sadece o case'ler değerlendirilir. Karmaşık bir isim (`ml_pipeline`, `graph_algorithm`, `multi_stage_agg`, `incremental_etl`, `cached_broadcast`, `staged_materialization`) verilirse o case eklenir. |
| `--exclude NAME` | Case'i hariç tutar |
| `--advice` | PersistenceAdvisor kararını ekler |
| `--format json` | Makinenin okuyacağı çıktı |
| `--list-cases` | Tüm case'leri öncelikleriyle listeler |

## 4. Hızlı başlangıç

1. Plugin'i kurun (bölüm 2).
2. Kodu verin:
   > /optimize-spark jobs/daily_revenue.py
3. Agent tahmin edemediği değerler için (tablo boyutu, executor belleği) size sorar ya da profil toplamanızı ister. Spark'a erişiminiz varsa job'ın içinde şunu çalıştırın:
   ```python
   import sys; sys.path.insert(0, "<plugin>/scripts")
   from collect_profile import collect
   collect(orders_df, "orders", join_cols=["user_id"], agg_cols=["category"],
           right_df=users_df, reuse_count=3)
   ```
   Oluşan `orders.profile.json` ve `orders.plan.txt` dosyalarını sohbete verin.
4. Çıktı şunları içerir: bulgu tablosu (öncelik, case, kanıt, değişiklik), yeniden yazılmış kod, ayrı listelenmiş runtime ve launch-time config'ler, varsayımlar ve doğrulama adımları.

CLI'ı doğrudan da çalıştırabilirsiniz:

```bash
python plugins/spark-code-optimizer/scripts/sparkopt_cli.py \
  --profile plugins/spark-code-optimizer/skills/sparkopt-toolkit/examples/profile_example.json --advice
```

## 5. Kullanım senaryoları

**Yavaş join.** 50 GB'lık fact tablo ile 40 MB'lık dimension tablosu join ediliyor ve plan'da SortMergeJoin görünüyor.
> Bu join neden yavaş? [explain çıktısı]

`spark-plan-reviewer` planda `SortMergeJoin` ve iki `Exchange` olduğunu gösterir. Profilde `right_table_bytes` küçük olduğu için `broadcast_join` eşleşir. Öneri: `F.broadcast(dim_df)` kullanmak. Spark'ın otomatik eşiği 10 MB, sparkopt'un 200 MB değeri ise yalnızca açık hint için bir üst sınırdır.

**Skew.** Birkaç task diğerlerinden 50 kat uzun sürüyor.
> user_id üzerinde skew var, join'i nasıl düzeltirim?

`spark-join-optimization` yüklenir. Önce AQE skew join ayarlarına bakılır (`spark.sql.adaptive.skewJoin.*`). AQE çözmezse salting önerilir. Üretilen salting kodu sağ taraftaki key'i ve `_salt` kolonunu düşürür, böylece join sonrası kolon çakışması olmaz.

**Cache mi, checkpoint mi?** Aynı DataFrame dört rapora gidiyor ve DAG 15 adım derinliğinde.
> base_df'i cache'leyeyim mi, checkpoint mi alayım?

CLI'ın `--advice` çıktısı `LOCAL_CHECKPOINT` kararını verir (derinlik > 10 ve reuse ≥ 2), gerekçesini ve uyarılarını da yazar. `spark-persistence-strategy` doğru StorageLevel'ı, `unpersist`'in son action'dan sonra çağrılmasını ve checkpoint dizininin temizlenmesini anlatır.

**İteratif algoritma.** PageRank döngüsü her iterasyonda yavaşlıyor ve driver OOM veriyor.
> PageRank döngüm her iterasyonda yavaşlıyor

`graph_algorithm` ve `iterative_loop` case'leri eşleşir: N iterasyonda bir `localCheckpoint(eager=True)` alınır ve dönen DataFrame kullanılır. Doğrulanmış uçtan uca örnek: `skills/spark-complex-workloads/examples/graph_algorithm.py`.

**Spark olmadan hızlı analiz.** Sadece tablo boyutlarını biliyorsunuz.
> 20 milyon satır, 8 GB, 200 partition, 3 kez kullanılıyor, executor 8 GB × 20. Ne önerirsin?

Agent bir profil JSON'u yazar, değerleri varsayım olarak işaretler ve CLI'ı çalıştırır.

**Özel kural.** Şirkete özgü bir kural eklemek istiyorsunuz, örneğin null oranı yüksek join key'i uyarısı.
> sparkopt'a null join key uyarısı veren bir custom case yaz

`sparkopt-toolkit` yüklenir. Hazır örnek: `skills/sparkopt-toolkit/examples/custom_case.py`.

## 6. Dosya yapısı

```text
plugins/spark-code-optimizer/
├── .claude-plugin/plugin.json
├── agents/
│   ├── spark-code-optimizer.md
│   └── spark-plan-reviewer.md
├── commands/
│   └── optimize-spark.md
├── scripts/
│   ├── sparkopt_cli.py          # offline CLI (pyspark gerekmez)
│   ├── collect_profile.py       # canlı DataFrame'den profil ve plan
│   └── sparkopt/                # düzeltilmiş kütüphane + tests/
└── skills/<skill>/
    ├── SKILL.md                 # karar tablosu, case özetleri, CLI örnekleri
    ├── references/<case>.md     # problem, plan sinyali, kural, öncesi/sonrası, config, sorun giderme
    └── examples/<case>.py       # doğrulanmış PySpark (CLI'daki reviewed_example)
```

## 7. İpuçları ve sınırlamalar

- **Eşikler heuristik.** 200 MB broadcast, skew > 3, 10M satır, derinlik 10/20 gibi değerler sparkopt'un kendi eşikleri, Spark kuralı değil. Agent bunları koda bakarak filtreler. CLI'ı doğrudan kullanıyorsanız kodunuzla ilgisiz case'leri siz eleyin (örneğin kodda window yoksa `window_opt`).
- **Üretilen kod bir taslak.** CLI'ın `code` alanında `left_df`, `dim_df` gibi yer tutucu isimler var. Asıl başlangıç noktası her case'in `reviewed_example` dosyası.
- **Karmaşık senaryo şablonları** (ml_pipeline, graph_algorithm, incremental_etl, cached_broadcast) kütüphanede hâlâ mantık hataları içeriyor. Doğru halleri `spark-complex-workloads/examples/` altında.
- **Plan heuristiklerinin sınırları:** derinlik girinti/3 ile hesaplanır, reuse ise aynı FileScan kolon listesinin tekrarından tahmin edilir. Sol-derin join zincirlerinde derinlik olduğundan düşük çıkar.
- **Launch-time config'ler** (`spark.memory.*`, `spark.serializer`, `spark.executor.*`) `spark.conf.set` ile değiştirilemez. Bunlar çıktıda `--conf` yorumu olarak gelir.
- **Profilde partition sayısı yoksa** (örneğin sadece `--plan` verildiyse) `repartition` önerilmez. Join bilgisi yoksa `broadcast_join` önerilmez.
- **Doğrulama durumu:** CLI, `collect_profile`, DSL, streaming tespiti ve salting kodu yerel PySpark 3.5.3'te uçtan uca çalıştırıldı. Dosya yazan bazı örnekler (bucket_join, parquet_pushdown, coalesce_write) Windows'ta winutils olmadığı için çalıştırılamadı, sadece derleme kontrolünden geçti. Delta gerektiren `delta_zorder` örneğinin de sadece yardımcı kontrolü çalıştı.
- Strateji adı `CACHE_MEMORY_SER` geriye dönük uyumluluk için korundu. Önerilen StorageLevel ise geçerli PySpark sabiti olan `MEMORY_ONLY`.

## 8. Bakım

- Kütüphane `scripts/sparkopt/` altında. Orijinali `Desktop/spark/optimized_code_generation` klasöründe, değiştirilmeden duruyor. Orijinal güncellenirse değişiklikleri kopyaya taşıyın ve `library_fixes.md`'deki düzeltmelerin korunduğunu kontrol edin.
- Testler (pyspark gerekmez):
  ```bash
  python -m pytest plugins/spark-code-optimizer/scripts/sparkopt/tests
  ```
  Regresyon testleri şunları kontrol eder: geçersiz StorageLevel kullanılmaması, üretilen kodun hepsinin geçerli Python olması, Spark 3.5 operatör adlarının tanınması ve CLI davranışları.
- Yeni bir case ekleme:
  1. `sparkopt/cases/` altına case sınıfını yazın.
  2. Sınıfı `ALL_BUILTIN_CASES` listesine ya da `complex_cases.ALL_COMPLEX_CASES` listesine ekleyin.
  3. İlgili skill'e `references/<case>.md` ve `examples/<case>.py` dosyalarını ekleyin. CLI bu örneği `reviewed_example` olarak otomatik bulur.
- Doğrulama: `claude plugin validate .`
- İlgili doküman: [data-engineering-design-patterns](data-engineering-design-patterns.md). Pipeline tasarım kararları için onu kullanın.
