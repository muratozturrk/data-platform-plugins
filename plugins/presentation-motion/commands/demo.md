---
description: demo.yaml'daki tüm segmentleri (HTML sahne + tarayıcı kaydı + terminal + video) render edip tek videoda birleştirir
argument-hint: [demo.yaml] [--only 2,4] [-o çıktı.mp4]
---

Çalıştır (render-engineer):
```bash
node "${CLAUDE_PLUGIN_ROOT}/capture/compose/compose.mjs" ${ARGUMENTS:-demo.yaml}
```
- Şema: `${CLAUDE_PLUGIN_ROOT}/capture/compose/demo.schema.json`. Önce YAML'ı şemaya göre kontrol et.
- Uzun sürer (sahne başına ~3 sn render / 1 sn video). Kullanıcıya tahmini süreyi söyle; uzun işlerde arka planda çalıştır.
- Bitince: video yolu, süre, boyut, probe sonucu. Hata varsa render-engineer teşhis eder.
