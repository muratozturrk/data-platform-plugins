---
description: Kurum renkleri/fontu/logosundan tema token dosyası üretir ve kontrastı doğrular (artist agent)
argument-hint: <ana-renk #hex> [ikincil #hex] [font] [--logo path] [--base corporate-light|corporate-dark|government]
---

**artist** agent'ını `brand-theme` skill'iyle çalıştır: `$ARGUMENTS`

1. `scenes/_base/tokens.css` içine yeni `[data-theme="brand"]` bloğu ekle (mevcut temaları silme).
2. Tüm metin/arka plan çiftleri için kontrast oranını hesapla; < 4.5:1 olanları ayarla ve raporla.
3. Projeksiyon için `brand-projection` varyantı üret (kontrast ≥ 7:1).
4. Logo verildiyse `scenes/_base/logo.svg` olarak kopyala; yerleşim kuralını (government-protocol) uygula.
5. `deck.yaml` → `theme: brand` ve `demo.yaml` → `theme: brand`.
