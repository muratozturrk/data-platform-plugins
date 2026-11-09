---
description: OpenLineage olaylarından veya dbt manifest'ten etki analizi animasyonu üretir (bir kolon/tablo değişince etkilenenler zincirleme yanar)
argument-hint: <openlineage.json | manifest.json> <başlangıç-dataset> [--direction downstream|upstream]
---

**solution-architect** + **animator**: `$ARGUMENTS`

1. Grafiği kur, başlangıç düğümünden BFS ile etkilenen düğümleri katman katman bul (varsayılan downstream).
2. 25 düğümden fazlaysa: doğrudan etkilenenleri göster, gerisini "+N tablo" düğümünde grupla.
3. Animasyon: başlangıç düğümü vurgulanır → her BFS katmanı 0,4 sn arayla turuncuya döner → etkilenen dashboard/raporlar en sonda kırmızı çerçeve + sayaç ("7 rapor etkileniyor").
4. Gerçek tablo adları için **compliance-guard** onayı; gerekirse takma ad kullan.
