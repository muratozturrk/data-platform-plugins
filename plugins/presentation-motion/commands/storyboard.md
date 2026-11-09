---
description: Kaynak metin/rapor/makaleden sahne sahne storyboard.md üretir (director agent)
argument-hint: <kaynak-dosya(lar)> [--minutes 5]
---

**director** agent'ını çalıştır. Girdi: `deck.yaml` + `$ARGUMENTS` içindeki kaynak dosyalar.

- Hedef kitle ve süre `deck.yaml`'dan okunur.
- Çıktı: `storyboard.md` (sahne tablosu + açık sorular).
- Bittiğinde açık soruları kullanıcıya özetle ve onay iste. Onaydan sonra sıradaki adım: `/presentation-motion:build`.
