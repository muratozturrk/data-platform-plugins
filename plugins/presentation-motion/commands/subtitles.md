---
description: Seslendirme metni ve SRT altyazı üretir, istenirse videoya gömer (narrator agent)
argument-hint: [--burn] [--tts piper]
---

**narrator** agent'ını çalıştır: `$ARGUMENTS`

1. Segment sürelerini `out/_segments/*.mp4` dosyalarından ffprobe ile oku (tahmin etme).
2. `out/narration.md` ve `out/subtitles.srt` üret.
3. `--burn` verildiyse altyazıyı videoya göm → `out/<deck>-subtitled.mp4`.
4. `--tts` verildiyse TTS komutlarını hazırla ve kullanıcı onayıyla çalıştır; ses dosyalarını segmentlerle hizalayıp birleştir.
