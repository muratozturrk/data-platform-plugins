---
description: Bağımsız kalite incelemesi - reviewer (ve gerekirse compliance-guard) çalıştırır, review.md üretir
argument-hint: [--audience executive|technical|government|academic] [--strict]
---

1. Önce teknik kontroller (render-engineer):
```bash
node "${CLAUDE_PLUGIN_ROOT}/scripts/lint-determinism.mjs" scenes/
node "${CLAUDE_PLUGIN_ROOT}/scripts/size-budget.mjs" scenes/
node "${CLAUDE_PLUGIN_ROOT}/scripts/probe.mjs" out/<deck>-pptx.mp4 --max-mb 25 --fps 30
```
2. `out/compliance.md` yoksa veya eskiyse **compliance-guard** çalıştır.
3. **reviewer** agent'ını çalıştır (`$ARGUMENTS`; kitle belirtilmediyse deck.yaml'dan).
4. Sonucu özetle: GEÇTİ/KALDI, puan tablosu, en önemli 3 düzeltme ve sorumlu agent'lar.
5. KALDI ise kullanıcıya düzeltmeleri otomatik uygulamayı teklif et (ilgili agent'lara dağıt, sonra tekrar review). `--strict`: hiçbir madde < 4 olmamalı.
