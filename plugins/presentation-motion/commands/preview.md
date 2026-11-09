---
description: Sahneleri tarayıcıda canlı önizler (Space oynat/duraklat, R baştan, ←/→ sar)
argument-hint: [sahne.html | scenes/]
---

Önizleme: `$ARGUMENTS` (boşsa `scenes/` içindeki tüm sahneler).

1. Proje klasöründe basit bir statik sunucu başlat (arka planda):
```bash
npx --yes http-server -p 4173 -c-1 .
```
2. Kullanıcıya adresleri listele: `http://localhost:4173/scenes/<sahne>.html`
   - Şablonların ham hali: `${CLAUDE_PLUGIN_ROOT}/templates/<ad>.html` (dosya olarak açılabilir)
3. Kısayollar: **Space** oynat/duraklat · **R** baştan · **←/→** 0,5 sn sar.
4. Canlı sunum için: sahneler sırayla tam ekran açılabilir; `?render=1` eklenirse otomatik oynatma kapanır.
