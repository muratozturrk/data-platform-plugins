---
name: video-export
description: Video çıktısı rehberi - profil seçimi (PowerPoint H.264, web AV1, 4K, alfa kanallı WebM), CRF/bitrate değerleri, dosya boyutu hedefi, bantlanma ve boyut şişmesinin nedenleri, ffmpeg komutları ve teslim paketi. Render ayarı seçerken, boyut/kalite sorunu çözerken kullan.
---

# Video dışa aktarma

## Profiller (render-scene.mjs / compose.mjs)
| Profil | Kodlama | Kullanım |
|---|---|---|
| `pptx` | `libx264 -preset slow -crf 18 -pix_fmt yuv420p -movflags +faststart` | PowerPoint, Keynote, e-posta, Teams |
| `web` | `libsvtav1 -preset 6 -crf 30` | web sitesi, arşiv (en küçük) |
| `4k` | H.264 CRF 18, deviceScaleFactor 2 | projeksiyon, büyük LED ekran |
| `alpha` | `libvpx-vp9 -pix_fmt yuva420p -crf 30 -b:v 0` | video üstü katman (PowerPoint'te alfa desteği sınırlı, test et) |

CRF codec'ler arası karşılaştırılamaz (x264 23 ≠ AV1 23). Metin kenarlarına bakarak karar ver.

## Boyut hedefi
`demo.yaml → output.max_size_mb` aşılırsa: `kbps = MB × 8192 × 0.95 / süre` ile hedef bitrate yeniden kodlanır.
Tipik: motion graphics 1080p ≈ 0,3–1 Mbit/s → 5 dk ≈ 15–35 MB.

## Boyutu şişirenler
- Grain/noise, partikül bulutu, sürekli hareketli doku → 5–10× büyüme
- Sahne boyunca hareket eden büyük gradyan arka plan
- Fotoğraf/video gömme (WebP/AVIF kullan, gereğinden büyük çözünürlük koyma)
Durağan sahneler neredeyse bedava: okuma payı (`data-hold`) boyutu artırmaz.

## Kalite sorunları
- **Bantlanma** (koyu gradyan): CRF 16, düz renk tercih et ya da 10-bit (`-pix_fmt yuv420p10le`, AV1/HEVC; PowerPoint uyumunu test et).
- **Bulanık metin** (kayıtlarda): `scale: 2` ile yakala, lanczos ile küçült.
- **yuvj420p**: JPEG kaynaklı tam aralık → `out_range=tv,format=yuv420p`.

## Yararlı komutlar
```bash
ffmpeg -i in.mp4 -vf "fps=1/3,scale=640:-1,tile=4x3" -frames:v 1 contact.png     # kontak sayfası
ffmpeg -i in.mp4 -ss 12 -frames:v 1 frame.png                                   # tek kare
ffmpeg -i in.mp4 -vf "setpts=PTS/2" -an fast.mp4                                # 2× hız
ffmpeg -i in.mp4 -vf "fps=15,scale=960:-1:flags=lanczos" -loop 0 out.gif        # GIF (README/e-posta)
```

## Teslim paketi
1. `deck-pptx.mp4` 2. çevrimdışı HTML (`scenes/` + `vendor/`, internet gerekmez) 3. PDF yedek (`/handout`) 4. (varsa) `subtitles.srt`
