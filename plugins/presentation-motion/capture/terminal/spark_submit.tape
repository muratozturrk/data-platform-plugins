# VHS tape — Spark işi gönderme
Set Shell "bash"
Set FontSize 28
Set Padding 48
Set Theme "Catppuccin Mocha"
Set TypingSpeed 40ms

Type "spark-submit --master k8s://https://cluster:6443 --deploy-mode cluster jobs/daily_aggregation.py --date 2026-11-01"
Sleep 300ms
Enter
Sleep 12s
