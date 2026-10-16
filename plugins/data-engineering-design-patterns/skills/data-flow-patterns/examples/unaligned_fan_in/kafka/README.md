# Unaligned Fan-In -- Kafka equivalent

No standalone code. In streaming, "relaxed parents" is the default: a consumer reading several
topics keeps producing even if one input is silent. What you must add is the *partial data*
signal:

- **Bounded wait instead of all-or-nothing**: in a stream join use a watermark / join window and
  emit results when the window closes, even if one side never arrived (outer join, null side).
- **Flag completeness**: add `is_approximate` / `sources_seen` to the emitted record (e.g. which
  input topics contributed to the window) so consumers don't assume a complete view.
- **Late fill**: allow late corrections (upsert by key on a compacted output topic) so the gaps
  are filled later -- the streaming version of "release partial, fill the gaps later".
- **Batch fan-in over readiness events**: a merger that waits for 24 hourly readiness events can
  use a deadline; after it, merge what arrived and publish the completeness ratio.
