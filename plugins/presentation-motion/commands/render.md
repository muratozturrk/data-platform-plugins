---
description: Final video çıktısı - profil seçimi (pptx|web|4k|alpha), boyut hedefi ve doğrulama
argument-hint: [pptx|web|4k|alpha] [--max-mb 25] [--skip-review]
---

**render-engineer**: `$ARGUMENTS`

1. `--skip-review` yoksa: son `out/review.md` GEÇTİ ve `out/compliance.md` GEÇTİ olmalı; değilse dur ve `/presentation-motion:review` öner.
2. `demo.yaml` → `output.profile` ve `max_size_mb` değerlerini argümanlarla güncelle (geçici kopya: `out/_render.yaml`).
3. Çalıştır:
```bash
node "${CLAUDE_PLUGIN_ROOT}/capture/compose/compose.mjs" out/_render.yaml -o out/<deck>-<profil>.<mp4|webm>
```
4. Teslim paketi öner: MP4 (PowerPoint) + çevrimdışı HTML (scenes/ + vendor/) + PDF yedek (`/presentation-motion:handout`).
