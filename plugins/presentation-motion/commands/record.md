---
description: Doğal dille anlatılan akışı Playwright script'ine çevirip gerçek uygulamayı kaydeder (demo-scripter + camera-operator)
argument-hint: <url> "<senaryo>"   ör. http://localhost:8088/superset/dashboard/5/ "bölge filtresini Marmara yap, satış grafiğine yakınlaş"
---

Girdi: `$ARGUMENTS`

1. **demo-scripter**: uygulamayı tanı (adapter seç: superset/airflow/jupyter/generic), selector'ları bul, `capture/<ad>.yaml` akışını yaz. Giriş gerekiyorsa:
```bash
node "${CLAUDE_PLUGIN_ROOT}/capture/playwright/recorder.mjs" --login <app> --base-url <url> --save .auth/<app>.json
```
2. Deneme kaydı:
```bash
node "${CLAUDE_PLUGIN_ROOT}/capture/playwright/recorder.mjs" capture/<ad>.yaml -o out/<ad>.mp4
```
3. **camera-operator**: zoom/spotlight/caption ekle, kontak sayfası (contact sheet) ile kontrol et, akışı güncelle.
4. **compliance-guard**: kare örneklerini denetle.
5. `demo.yaml`'a `type: browser` segmenti olarak ekle.
