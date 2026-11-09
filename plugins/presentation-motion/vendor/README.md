# vendor/ — çevrimdışı kütüphaneler

`node scripts/fetch-vendor.mjs` ile sabit sürümlerle indirilir. CDN kullanılmaz (kapalı ağ / kamu ortamları için).

| Kütüphane | Sürüm | Lisans | Not |
|---|---|---|---|
| GSAP (core, SplitText, DrawSVG, MorphSVG, MotionPath, Text) | 3.13.0 | GreenSock Standard "No Charge" License (Webflow) | Ticari kullanım ücretsiz. Webflow benzeri **görsel animasyon editörü** ürünleri kapsam dışı. https://gsap.com/community/standard-license/ |
| KaTeX | 0.16.11 | MIT | fontları `katex/fonts/` altına kopyalayın |
| D3 | 7.9.0 | ISC | |
| rough-notation | 0.5.1 | MIT | yalnızca şekil üretimi (kendi animasyonu seek edilemez) |

Tam OSI açık kaynak lisans gerekiyorsa GSAP yerine Anime.js v4 (MIT) kullanılabilir; runtime sözleşmesi (paused timeline + seek) aynı kalır.
