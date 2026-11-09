---
description: Tek bir sahneye motion-recipes'ten bir hareket tarifi uygular (animator agent)
argument-hint: <sahne.html> <tarif> [hedef-seçici]   ör. scenes/04.html split-text-rise "#title"
---

**animator** agent'ını çalıştır: `$ARGUMENTS`

- Tarif adları `motion-recipes` skill'inde. Ad tanınmazsa en yakın 3 tarifi öner.
- Mevcut timeline'a ekle, mevcut hareketleri bozmadan zamanlamayı yeniden dengele.
- Sonra o sahneyi render et ve orta/son kareyi kontrol et:
```bash
node "${CLAUDE_PLUGIN_ROOT}/scripts/render-scene.mjs" <sahne.html> -o out/preview-<ad>.mp4
```
