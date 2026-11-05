# Plugin kullanım dokümanları

Bu repodaki plugin'lerin kullanım kılavuzları. Her plugin için bir dosya var: `docs/plugins/<plugin-adı>.md`.

| Plugin | Kategori | Özet | Doküman |
| --- | --- | --- | --- |
| apache-superset | data | Apache Superset 6.x: dağıtım, güvenlik/RLS, gömme, dashboards-as-code, dashboard tasarımı ve yaşam döngüsü, grafik seçimi, doğrulanmış API sınırları, gizlilik, AI katmanı; 16 skill, 4 agent, 7 komut | [apache-superset.md](apache-superset.md) |
| data-governance | governance | Ürüne bağlı olmayan veri yönetişimi: veri sözleşmeleri, katalog seçimi ve soyağacı, sınıflandırma ve erişim kontrolü, kalite SLA'ları, KVKK yükümlülük eşlemesi, anonimleştirme (k-anonimlik, diferansiyel gizlilik, risk ölçümü), 14 alanlı değerlendirme rubriği; 7 skill, 4 agent, 4 komut | [data-governance.md](data-governance.md) |
| openmetadata | governance | OpenMetadata 1.13.x yönetişim kataloğu: mimari ve genişleme noktaları, platform bileşenleri için connector/lineage kapsamı, öncelikli boşluk kataloğu, KVKK uyum eşlemesi, 14 alanlı değerlendirme rubriği, yeni yetenek tasarımı; 6 skill, 4 agent, 4 komut | [openmetadata.md](openmetadata.md) |
| project-nessie | data | Project Nessie 0.108.x, HKM platformunun Iceberg kataloğu: branch/tag/commit modeli, Iceberg REST ile S3 imzalama ve credential vending, Helm/PostgreSQL kurulumu, Keycloak OIDC ve Ranger ile CEL yetkilendirmesi, motor entegrasyonları, write-audit-publish, GC ve KVKK silme, Polaris'ten geçiş; 8 skill, 2 agent, 2 komut | [project-nessie.md](project-nessie.md) |
| apache-polaris | data | Apache Polaris REST katalog: erişim kontrolü, REST API, federation, kurulum, CLI, Spark/Snowflake/Dremio entegrasyonu; 9 skill ve Docker lab'ları | [apache-polaris.md](apache-polaris.md) |
| data-engineering-design-patterns | data | Veri pipeline'ları için 77 tasarım pattern'inin skill, agent ve Spark/Kafka/Airflow örnekleri | [data-engineering-design-patterns.md](data-engineering-design-patterns.md) |
| spark-code-optimizer | data | sparkopt kural motoruyla PySpark optimizasyonu: offline CLI, 7 skill, kodu yeniden yazan agent | [spark-code-optimizer.md](spark-code-optimizer.md) |
| spark-property-based-testing | testing | Hypothesis ile PySpark property testleri: DataFrame stratejileri, schema contract, GX 1.x kalite kontrolleri, transformation invariant'ları, pytest plugin ve CLI | [spark-property-based-testing.md](spark-property-based-testing.md) |

## Kurallar

- Dokümanlar `docs/` altında durur. `build-platform-plugins.ps1` ve `.sh`, kök `README.md`'yi ve upstream'den çekilen plugin'lerin `README.md` dosyalarını her çalıştırmada yeniden yazar; `docs/` klasörüne dokunmaz.
- Yeni bir doküman [_TEMPLATE.md](_TEMPLATE.md) şablonuna göre yazılır ve bu tabloya bir satır eklenir.
- İçerik plugin'in gerçek dosyalarından çıkarılır: `plugin.json`, `agents/`, `commands/`, `skills/*/SKILL.md`, `scripts/`.
