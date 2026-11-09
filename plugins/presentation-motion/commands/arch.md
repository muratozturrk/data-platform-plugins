---
description: architecture.yaml'dan (veya anlatımdan) katman katman açılan, veri akışlı mimari sahnesi üretir
argument-hint: <architecture.yaml | "serbest anlatım"> [--detail executive|technical]
---

**solution-architect** agent'ını çalıştır: `$ARGUMENTS`

1. Girdi YAML değilse anlatımdan `data/architecture.yaml` oluştur (şema: `${CLAUDE_PLUGIN_ROOT}/examples/26-poc-realtime-pipeline/architecture.yaml`).
2. Doğruluk kontrolleri: akış yönü, stream/batch, katman ≤ 5, katman başına düğüm ≤ 5.
3. `demo.yaml`'a ekle:
```yaml
- type: html
  template: architecture-layers
  source: data/architecture.yaml
```
4. Yalnızca bu segmenti render et (`compose.mjs demo.yaml --only <i>`) ve son kareyi göster.
