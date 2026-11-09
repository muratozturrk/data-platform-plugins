---
description: Tek kaynaktan üç süre varyantı üretir - 60 sn teaser, 5 dk yönetici, 15 dk teknik
argument-hint: [--only teaser,executive,technical]
---

**audience-adapter** ile varyantları üret: `$ARGUMENTS`

| Varyant | Dosya | İçerik |
|---|---|---|
| teaser (60 sn) | `demo.teaser.yaml` | açılış + 1 kanıt + 1 sayı + kapanış; caption'lı, sessiz izlenebilir |
| executive (5 dk) | `demo.executive.yaml` | BLUF yapısı, karar sahnesi |
| technical (15 dk) | `demo.technical.yaml` | mimari, demo kayıtları, benchmark |

Ortak segmentler yeniden render edilmez: aynı `template+data` için mevcut `out/_segments` çıktısını kopyala (önce karşılaştır). Sonunda üç videonun süre ve boyut tablosunu ver.
