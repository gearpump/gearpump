# Gearpump Beam Runner

This module adds a low-level Apache Beam runner built directly on Gearpump's `Processor` and `Task`
graph API.

Current scope:

- `Create`, `Impulse`, and other `Read.Bounded` sources
- `ParDo`, including multi-output `ParDo` without side inputs
- `Flatten.pCollections()`
- `Window.into(...)` for non-merging windows
- `GroupByKey` in non-merging windows with a single final pane
- `Combine.GroupedValues` and common keyed combines such as `Sum.integersPerKey()`

Current limitations:

- No side inputs
- No merging windows
- No unbounded sources
- No Beam state/timers support
- No custom trigger/pane semantics beyond one final emission at watermark max

The implementation intentionally keeps the first supported transform set small and routes Beam
execution through Gearpump's low-level runtime instead of the older DSL-based runner design.
