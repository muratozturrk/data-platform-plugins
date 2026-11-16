---
description: Sahnelerden PowerPoint sunumu üretir - her sahne bir slayt; step-* durakları olan sahnelerde slayt içinde tıklamayla adım adım ilerler
argument-hint: [scenes/] [-o out/sunum.pptx] [--auto] [--notes notlar.json]
---

**render-engineer**: `$ARGUMENTS`

1. Sahne klasörü argümanda yoksa `scenes/` kullan. Hangi sahnelerde `step-` label'ı olduğunu bul (`grep -l "addLabel('step-\|addLabel(\`step-" scenes/*.html`) ve kullanıcıya kısaca söyle: "N slayt, şu slaytlarda M tıklama".
2. Kullanıcı bir sahnede slayt içi tıklama istiyorsa ve durak yoksa, `deterministic-render` skill'indeki "Tıklama durakları" kurallarına göre label ekle (durak anında ekran durağan olmalı). `--auto` istendiyse durak eklenmez.
3. Konuşmacı notu istenirse `storyboard.md`'den `out/notes.json` üret: `{"<sahne dosya adı, .html'siz>": "not"}`.
4. Render ve paketleme:
```bash
node "${CLAUDE_PLUGIN_ROOT}/scripts/render-scene.mjs" scenes --steps -o out/steps
node "${CLAUDE_PLUGIN_ROOT}/scripts/build-pptx.mjs" out/steps -o out/<deck>.pptx [--notes out/notes.json] [--auto]
```
   `--auto` (kiosk, video bitince ilerler) tıklamalı slaytlarla birlikte kullanılamaz; gerekirse durakları kaldırıp yeniden render al.
5. Kullanıcıya teslimde söyle:
   - Her slayt tam ekran video; metin PowerPoint'te düzenlenemez, değişiklik sahne HTML'inde yapılıp yeniden üretilir.
   - Slayt içinde bir sonraki tıklama, mevcut parça bitince yapılmalı; erken tıklanırsa sonraki parça hemen başlar.
   - Geri (←) tuşu PowerPoint'in standart davranışıyla önce slayt içindeki adımları geri alır.
   - Sunum bilgisayarında F5 ile bir kez dene; eski PowerPoint sürümlerinde ve Mac'te video başlangıç gecikmesi farklı olabilir.
