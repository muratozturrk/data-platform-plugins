# VHS tape — dbt çalıştırma demosu (https://github.com/charmbracelet/vhs)
# compose.mjs Output/Width/Height satırlarını kendisi ekler.
Set Shell "bash"
Set FontSize 30
Set FontFamily "JetBrains Mono"
Set Padding 48
Set Theme "Catppuccin Mocha"
Set TypingSpeed 45ms

Hide
Type "cd ~/platform && clear"
Enter
Show

Type "dbt run --select tag:sales"
Sleep 400ms
Enter
Sleep 8s

Type "dbt test --select gold.daily_sales"
Sleep 400ms
Enter
Sleep 5s
