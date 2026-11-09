---
description: storyboard.md'den sahneleri ve demo.yaml'ı üretir (artist → animator → data-scientist), önizleme render'ı alır
argument-hint: [sahne-numaraları, ör. 2,5]
---

`storyboard.md` → çalışan sahneler. Kısıtlama: `$ARGUMENTS` (boşsa tüm sahneler).

1. Her sahne için şablonu `${CLAUDE_PLUGIN_ROOT}/templates/CATALOG.md`'den seç.
   - Hazır şablon varsa: `demo.yaml`'a `type: html, template, data` segmenti ekle (veriyi storyboard'dan doldur).
   - Yoksa: **artist** statik sahneyi `scenes/NN-ad.html` olarak tasarlar → **animator** timeline ekler → `type: html, scene:` segmenti.
2. Grafik/formül/benchmark sahneleri **data-scientist**'ten geçer.
3. Mimari/DAG/lineage sahneleri **solution-architect**'ten geçer.
4. Doğrula:
```bash
node "${CLAUDE_PLUGIN_ROOT}/scripts/lint-determinism.mjs" scenes/
node "${CLAUDE_PLUGIN_ROOT}/scripts/size-budget.mjs" scenes/
node "${CLAUDE_PLUGIN_ROOT}/capture/compose/compose.mjs" demo.yaml
```
5. Kullanıcıya video yolunu, süreyi ve storyboard hedefinden sapmayı raporla. Sonraki adım: `/presentation-motion:review`.
