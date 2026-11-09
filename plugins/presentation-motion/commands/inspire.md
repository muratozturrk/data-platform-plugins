---
description: Codrops vb. bir demo URL'sinden veya efekt adından tekniği çıkarıp videoya uygun (seek edilebilir) tarife dönüştürür
argument-hint: <url | efekt-adı>   ör. "svg mask reveal" | https://tympanus.net/codrops/...
---

Girdi: `$ARGUMENTS`

1. URL ise sayfayı ve (varsa) GitHub deposunu incele: kullanılan kütüphaneler, ana teknik, tetikleyici (scroll/fare/zaman).
2. **Lisansı kontrol et.** Kodu kopyalama; tekniği anlayıp sıfırdan yaz.
3. Videoya uyarlama (skill: `deterministic-render`):
   - ScrollTrigger/scroll → zamana bağlı timeline (`tl.to(proxy, {progress:1})`)
   - Fare → önceden programlanmış bezier yol
   - Fizik/fluid → sabit adımlı (`dt = 1/fps`) simülasyon ya da proxy tabanlı yaklaşım
   - Lenis → gereksiz (easing yeterli)
4. Yeni tarifi `motion-recipes` formatında yaz ve bir deneme sahnesi üret: `scenes/_inspire-<ad>.html`.
5. Render et, boyut bütçesini ölç, kullanıcıya önizlemeyi ve maliyeti (KB, render süresi) raporla.
