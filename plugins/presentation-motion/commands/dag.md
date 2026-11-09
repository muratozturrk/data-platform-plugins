---
description: Airflow DAG dosyasından veya dbt manifest.json'dan pipeline çalışma animasyonu üretir
argument-hint: <dags/x.py | target/manifest.json> [--fail task_id]  (bir görevin hata verip retry ile toparlanması)
---

**solution-architect** + **animator**: `$ARGUMENTS`

1. Girdiyi ayrıştır:
   - Airflow: `>>`/`<<`, `set_downstream`, `@task` bağımlılıkları → görev grafiği. Kodu ÇALIŞTIRMA, statik oku.
   - dbt: `manifest.json` → `nodes[*].depends_on.nodes` (model/source/seed/snapshot), `exposures`.
2. `data/dag.yaml` yaz: `{ nodes: [{id,label,group}], edges: [{from,to}], run: [{id, start, duration, state}] }`.
   - Süreler bilinmiyorsa göreli ve eşit; `--fail` verildiyse o görev kırmızı → retry → yeşil.
3. Şablon `dag` henüz yoksa (CATALOG'a bak) artist + animator ile `scenes/NN-dag.html` üret: düğümler topolojik sıraya göre soldan sağa; durum renkleri gri → mavi (çalışıyor) → yeşil/kırmızı.
4. Render et ve son kareyi göster.
