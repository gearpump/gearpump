# Gearpump Beam Runner

This module adds a low-level Apache Beam runner built directly on Gearpump's `Processor` and `Task`
graph API.

Current scope:

- `Create` and other `Read.Bounded` sources
- `ParDo`
- `Flatten.pCollections()`
- `GroupByKey` in the default global window

Current limitations:

- No side inputs
- No custom window assignment or merging windows
- No unbounded sources
- No Beam state/timers support

The implementation intentionally keeps the first supported transform set small and routes Beam
execution through Gearpump's low-level runtime instead of the older DSL-based runner design.
