---
description: Arayüz değiştiğinde tüm browser/terminal segmentlerini yeniden çeker ve videoyu günceller (CI'da da çalışabilir)
argument-hint: [demo.yaml] [--check]   --check: kaydetmeden yalnızca selector'ları doğrular
---

1. `demo.yaml`'daki `browser` ve `terminal` segmentlerinin indekslerini bul.
2. `--check`: her browser segmentini kayıtsız çalıştır (adımları dene, selector bulunamazsa raporla).
3. Aksi halde yalnızca bu segmentleri yeniden render et:
```bash
node "${CLAUDE_PLUGIN_ROOT}/capture/compose/compose.mjs" demo.yaml --only <indeksler>
```
4. Kırılan adımları **demo-scripter**'a düzelttir, tekrar dene.
5. CI örneği (GitHub Actions): Node 22+, `npx playwright install chromium-headless-shell`, ffmpeg kur, `compose.mjs` çalıştır, `out/*.mp4`'ü artifact olarak yükle.
