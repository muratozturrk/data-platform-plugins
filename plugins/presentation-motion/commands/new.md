---
description: Yeni animasyonlu sunum/demo projesi oluşturur (klasör yapısı, deck.yaml, tema, vendor ve _base kopyaları)
argument-hint: <klasör-adı> [konu] [--audience technical|executive|government|academic] [--minutes 5] [--theme corporate-dark]
---

Yeni bir sunum projesi oluştur. Argümanlar: `$ARGUMENTS`

1. Eksik bilgi varsa kullanıcıya **tek seferde** sor: konu, hedef kitle, süre, tema, dil (varsayılan tr), çıktı (video / canlı sunum / ikisi).
2. Şu yapıyı kur:
```
<klasör>/
├─ deck.yaml        # title, audience, minutes, theme, language, output.profile, output.max_size_mb
├─ storyboard.md    # boş şablon (director dolduracak)
├─ demo.yaml        # segments: [] (build/poc dolduracak)
├─ scenes/_base/    # ${CLAUDE_PLUGIN_ROOT}/templates/_base kopyası
├─ vendor/          # ${CLAUDE_PLUGIN_ROOT}/vendor kopyası (çevrimdışı çalışma)
├─ capture/         # Playwright akışları ve VHS tape'leri
├─ data/            # CSV, metrikler, architecture.yaml
├─ .auth/           # oturum dosyaları (git'e girmez)
├─ out/
└─ .gitignore       # out/, .auth/, scenes/_gen-*
```
3. `vendor/` boşsa önce `node "${CLAUDE_PLUGIN_ROOT}/scripts/fetch-vendor.mjs"` çalıştır.
4. Kitle `executive` veya `government` ise tema önerisi: `corporate-light` / `government`, `max_size_mb: 25`.
5. Son olarak sıradaki adımı söyle: `/presentation-motion:storyboard` (kaynak metinle) veya `/presentation-motion:poc`.
