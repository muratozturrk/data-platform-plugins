# Dashboard review checklist

## Few's thirteen common mistakes (score 0 = violated, 1 = partly, 2 = clean)
1. Exceeding the boundaries of a single screen
2. Supplying inadequate context for the data (no target/prior/benchmark)
3. Displaying excessive detail or precision
4. Choosing a deficient measure (wrong KPI, vanity metric)
5. Choosing inappropriate display media (gauges, pies, 3D)
6. Introducing meaningless variety (different chart types for the same question)
7. Using poorly designed display media (axes, labels, scales)
8. Encoding quantitative data inaccurately (truncated axes, area for quantity)
9. Arranging data poorly (reading order, grouping, KPIs buried)
10. Highlighting important data ineffectively or not at all
11. Cluttering with useless decoration
12. Misusing or overusing colour
13. Designing an unattractive visual display (crowding, misalignment)

## Superset-specific checks
- [ ] Charts use certified datasets and dataset metrics (no ad-hoc SQL metrics)
- [ ] Every chart: title as message, description (source, refresh), number/date formats, bounded time range
- [ ] Native filters ≤ 6 visible, defaults set, scoping correct; cross-filters justified
- [ ] 12-column layout: KPIs top-left, sections with markdown headers, one screen per tab
- [ ] Owners, tags (tenant, domain), certification, stable slug; synced from Git (`is_managed_externally`)
- [ ] Theme tokens, colour-blind-safe palette, contrast ≥ 4.5:1, dark mode verified
- [ ] p95 load < 5 s cached; slow charts moved to aggregated datasets; dashboard `cache_timeout`
- [ ] Embedded: registered embeddable, allowed domains, filter bar behaviour decided
