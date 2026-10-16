# data-engineering-design-patterns

> Veri pipeline'ları için tasarım pattern'lerini skill, agent ve Spark/Kafka/Airflow kod örnekleri olarak sunar.

| | |
| --- | --- |
| Sürüm | 0.1.0 |
| Kategori | data |
| Kaynak | `plugins/data-engineering-design-patterns/` |

## 1. Ne işe yarar?

Bir veri pipeline'ı tasarlarken ya da mevcut bir pipeline'ı gözden geçirirken "hangi pattern'i kullanmalıyım?" sorusuna gerekçeli bir cevap verir. 10 skill, 10 konu alanını kapsar ve toplam 77 pattern içerir. Her pattern için şunlar var:

- **Referans dokümanı:** problem, çözümün mekaniği, sonuçları ve trade-off'ları, kullanım tablosu, sık hatalar, ilgili pattern'ler.
- **Kod örnekleri:** `examples/<pattern>/{spark,kafka,airflow}/` altında pattern'in tanımına uyan örnekler. Bir stack pattern'e uymuyorsa koda zorlamak yerine o klasörde eşdeğerini anlatan bir README var.

Claude bu skill'leri konuşmanın içeriğine göre kendisi yükler. Örneğin "geç gelen veriyi nasıl işlerim" diye sorulunca `error-management-patterns` devreye girer. İki agent ise tasarım ve inceleme işlerini uçtan uca yürütür.

## 2. Kurulum

```text
/plugin marketplace add muratozturrk/data-platform-plugins
/plugin install data-engineering-design-patterns@data-platform-plugins
```

Yerel geliştirme: `claude --plugin-dir ./plugins/data-engineering-design-patterns`

Gereksinimler: plugin'in kendisi bir şey kurmaz. Örnekleri çalıştırmak için ilgili stack gerekir: PySpark 3.4+ (çoğu örnek Delta Lake kullanır), `confluent-kafka` veya `kafka-python`, Airflow 2.x. Bazı örneklerde PostgreSQL de var. Bağlantı bilgileri ortam değişkenlerinden okunur.

## 3. İçerik

### Skill'ler

| Skill | Konu | Pattern'ler |
| --- | --- | --- |
| `data-ingestion-patterns` | Data Ingestion | Full Loader, Incremental Loader, Change Data Capture, Passthrough Replicator, Transformation Replicator, Compactor, Readiness Marker, External Trigger |
| `error-management-patterns` | Error Management | Dead-Letter, Windowed Deduplicator, Late Data Detector, Static/Dynamic Late Data Integrator, Filter Interceptor, Checkpointer |
| `idempotency-patterns` | Idempotency | Fast Metadata Cleaner, Data Overwrite, Merger, Stateful Merger, Keyed Idempotency, Transactional Writer, Proxy |
| `data-value-patterns` | Data Value | Static/Dynamic Joiner, Wrapper, Metadata Decorator, Distributed/Local Aggregator, Incremental/Stateful Sessionizer, Bin Pack Orderer, FIFO Orderer |
| `data-flow-patterns` | Data Flow | Local/Isolated Sequencer, Aligned/Unaligned Fan-In, Parallel Split, Exclusive Choice, Single/Concurrent Runner |
| `data-security-patterns` | Data Security | Vertical Partitioner, In-Place Overwriter, Fine-Grained Accessor (tables/resources), Encryptor, Anonymizer, Pseudo-Anonymizer, Secrets Pointer, Secretless Connector |
| `data-storage-patterns` | Data Storage | Horizontal/Vertical Partitioner, Bucket, Sorter, Metadata Enhancer, Dataset Materializer, Manifest, Normalizer, Denormalizer |
| `data-quality-patterns` | Data Quality | Audit-Write-Audit-Publish, Constraints Enforcer, Schema Compatibility Enforcer, Schema Migrator, Offline/Online Observer |
| `data-observability-patterns` | Data Observability | Flow Interruption, Skew, Lag, SLA Misses Detector, Dataset Tracker, Fine-Grained Tracker |
| `data-integration-patterns` | Streaming | API Gateway, Zero-ETL Synchronizer, Hybrid Source, Sidecar, Partial State Writer, Streaming/Batch Reprocessor |

Her `SKILL.md` dosyasında bir **Pattern Selection Guide** tablosu var. Tabloda bir belirti ya da ihtiyaç, ona uyan pattern ve referans dosyası eşleştirilir. Hangi pattern'in seçileceğine karar verirken ilk bakılacak yer burasıdır.

### Agent'lar

| Agent | Model | Ne yapar | Ne zaman kullanılır |
| --- | --- | --- | --- |
| `data-pattern-architect` | opus | Problemi pipeline yaşam döngüsü boyunca ele alır (ingest → hata yönetimi → dönüşüm → idempotent yazma → depolama → kalite → gözlem). Her aşama için en fazla bir pattern seçer, reddettiği alternatifi gerekçelendirir. Bir pattern tablosu, ASCII akış diyagramı, riskler ve örnek dosya yolları verir. | Yeni pipeline tasarımı; "full load mu incremental mı", "merge mi overwrite mı" gibi kararlar |
| `data-pipeline-pattern-reviewer` | sonnet | Mevcut Spark, Kafka, Airflow, dbt veya SQL kodunu pattern'lerin "gotcha" listelerine göre inceler. Her bulguyu `[severity] path:line — problem → pattern — düzeltme` biçiminde verir. | Kod yazıldıktan ya da değiştirildikten sonra; duplicate, veri kaybı veya re-run sorunlarında |

### Komutlar

| Komut | Argüman | Ne yapar |
| --- | --- | --- |
| `/design-with-patterns` | Pipeline'ın tarifi | 1. `data-pattern-architect` ile tasarım yapar. 2. Seçilen pattern'lerin örnek kodunu projeye uyarlar. 3. Üretilen kodu `data-pipeline-pattern-reviewer` ile inceler ve kritik/yüksek bulguları düzeltir. 4. Özet verir. |

Komut başka bir plugin'deki bir komutla aynı adı taşıyorsa tam adını kullanın: `/data-engineering-design-patterns:design-with-patterns`.

## 4. Hızlı başlangıç

1. Plugin'i kurun (bölüm 2).
2. Problemi sorun. İlgili skill otomatik yüklenir:
   > Kafka'dan gelen siparişlerde aynı event iki kez gelebiliyor ve geç gelen kayıtlar var. Silver tabloya nasıl yazmalıyım?
3. Uçtan uca bir tasarım için komutu kullanın:
   > /design-with-patterns Postgres'teki orders tablosunu saatlik olarak lakehouse'a taşı, silinen kayıtlar da yansısın, backfill güvenli olsun
4. Yazdığınız kodu inceletin:
   > data-pipeline-pattern-reviewer agent'ı ile dags/orders_dag.py ve jobs/orders_job.py dosyalarını incele

## 5. Kullanım senaryoları

**Ingestion stratejisi seçmek.** Kaynak tablo 500 milyon satır ve her gün %1'i değişiyor.
> Bu tablo için full loader mı, incremental loader mı, CDC mi kullanmalıyım? Silinen kayıtlar önemli.

`data-ingestion-patterns` skill'i yüklenir. Silmeler önemli olduğu için CDC'ye ya da soft-delete'li Incremental Loader'a yönlendirir ve `references/change_data_capture.md` ile `examples/change_data_capture/` dosyalarını gösterir.

**Backfill'de duplicate oluşuyor.** Airflow job'ı yeniden çalıştırılınca satırlar ikileniyor.
> Bu job'ı rerun ettiğimde duplicate oluşuyor, nasıl idempotent yaparım?

`idempotency-patterns` devreye girer. Duruma göre Data Overwrite (partition bazlı `replaceWhere`), Merger (MERGE) ya da Keyed Idempotency önerilir ve ilgili Spark/Airflow örneği verilir.

**Geç gelen veri.** Streaming job geç gelen event'leri düşürüyor.
> Watermark'ı geçen event'ler kayboluyor, bunları nasıl yakalarım?

`error-management-patterns` yüklenir. Late Data Detector ile tespit yapılır, ardından seyrek geliyorsa Static, sürekli geliyorsa Dynamic Late Data Integrator ile entegre edilir.

**Mimari inceleme.** Yeni bir pipeline'ın tasarımını inceletmek istiyorsunuz.
> data-pattern-architect ile şu pipeline'ı tasarla: IoT sensörleri → Kafka → 5 dakikalık agregasyon → dashboard, SLA 10 dakika

Çıktıda her aşama için seçilen pattern, akış diyagramı, SLA ve lag izleme için observability pattern'leri ve örnek dosya yolları yer alır.

**Güvenlik ve PII.** Bir tabloda PII var ve analistler sadece maskeli veriyi görmeli.
> Analistlerin email'i göremeyeceği ama join yapabileceği bir yapı kur

`data-security-patterns` yüklenir. Join için deterministik HMAC ile Pseudo-Anonymizer, yetki için Fine-Grained Accessor for Tables, anahtarlar için Secrets Pointer önerilir.

## 6. Dosya yapısı

```text
plugins/data-engineering-design-patterns/
├── .claude-plugin/plugin.json
├── agents/
│   ├── data-pattern-architect.md
│   └── data-pipeline-pattern-reviewer.md
├── commands/
│   └── design-with-patterns.md
└── skills/<skill>/
    ├── SKILL.md                     # seçim tablosu + pattern özetleri (ilk bakılacak yer)
    ├── references/<pattern>.md      # derin anlatım
    └── examples/<pattern>/
        ├── README.md                # dosyalar ne gösteriyor, nasıl çalıştırılır
        ├── spark/  kafka/  airflow/ # kod; uygun olmayan stack'te README
```

## 7. İpuçları ve sınırlamalar

- **Tanımlar tutarlı.** Her pattern tek bir tanıma göre uygulanır; örneğin Transformation Replicator, Bronze→Silver dönüşümü olarak değil, PII temizleyerek prod→staging kopyası olarak uygulanır.
- **Örnekler gerçek ortamda çalıştırılmadı.** Tüm `.py` dosyaları derleme kontrolünden geçti, saf mantık kısımlarının `--self-test` / `_self_check()` testleri geçiyor. Spark, Kafka ve Airflow çağrıları gerçek bir cluster'da denenmedi. Kendi ortamınızda önce küçük veriyle deneyin.
- **Örnekler başlangıç noktasıdır.** Tablo, topic ve yol isimleri örnektir. `/design-with-patterns` bunları sizin projenize uyarlar.
- **Bağlantılar:** şifre ve DSN'ler ortam değişkenlerinden, Airflow connection'larından veya bir secrets manager'dan gelir. Kodda sabit yazılmış secret yok.
- **Kafka Connect örnekleri** `${env:...}` kullanır. Bunun için Connect worker'da `config.providers=env` tanımlı olmalı.
- **Schema migrator ve compatibility gate birlikte:** DROP COLUMN sadece `dp_schema_migrator` rolüne sahip ve `SET LOCAL dp.schema_migration = 'contract'` çalıştıran bir bağlantıyla yapılabilir. Ayrıntılar `data-quality-patterns` README'lerinde.

## 8. Bakım

- Plugin bu repoda geliştirilir; upstream'den çekilmez. Build script'leri bu plugin'in klasörüne dokunmaz.
- Doğrulama:
  ```bash
  claude plugin validate .
  python -m py_compile $(find plugins/data-engineering-design-patterns -name '*.py')
  ```
- Yeni bir pattern eklerken ilgili skill'de üç şey yapılır: `references/<pattern>.md` ve `examples/<pattern>/` eklenir, `SKILL.md`'deki seçim tablosuna bir satır yazılır.
- İlgili doküman: [spark-code-optimizer](spark-code-optimizer.md). Spark tarafındaki performans kararları için onu kullanın.
