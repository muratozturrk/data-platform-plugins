# Plugin kullanım dokümanları

Bu repodaki plugin'lerin kullanım kılavuzları. Her plugin için bir dosya var: `docs/plugins/<plugin-adı>.md`.

| Plugin | Kategori | Özet | Doküman |
| --- | --- | --- | --- |
| data-engineering-design-patterns | data | Veri pipeline'ları için 77 tasarım pattern'inin skill, agent ve Spark/Kafka/Airflow örnekleri | [data-engineering-design-patterns.md](data-engineering-design-patterns.md) |
| spark-code-optimizer | data | sparkopt kural motoruyla PySpark optimizasyonu: offline CLI, 7 skill, kodu yeniden yazan agent | [spark-code-optimizer.md](spark-code-optimizer.md) |

## Kurallar

- Dokümanlar `docs/` altında durur. `build-platform-plugins.ps1` ve `.sh`, kök `README.md`'yi ve upstream'den çekilen plugin'lerin `README.md` dosyalarını her çalıştırmada yeniden yazar; `docs/` klasörüne dokunmaz.
- Yeni bir doküman [_TEMPLATE.md](_TEMPLATE.md) şablonuna göre yazılır ve bu tabloya bir satır eklenir.
- İçerik plugin'in gerçek dosyalarından çıkarılır: `plugin.json`, `agents/`, `commands/`, `skills/*/SKILL.md`, `scripts/`.
