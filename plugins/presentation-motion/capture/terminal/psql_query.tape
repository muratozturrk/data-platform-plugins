# VHS tape — psql ile sorgu (DEMO veritabanı, sentetik veri)
Set Shell "bash"
Set FontSize 28
Set Padding 48
Set Theme "Catppuccin Mocha"
Set TypingSpeed 35ms

Type "psql -h localhost -d demo -c \"SELECT region, sum(amount) FROM gold.daily_sales GROUP BY 1 ORDER BY 2 DESC LIMIT 5;\""
Enter
Sleep 4s
