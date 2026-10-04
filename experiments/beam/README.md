# Gearpump Beam Runner

This module adds a low-level Apache Beam runner built directly on Gearpump's `Processor` and `Task`
graph API.

Current scope:

- `Create`, `Impulse`, `Read.Bounded`, and basic `Read.Unbounded` sources
- `ParDo`, including multi-output `ParDo` without side inputs
- `Flatten.pCollections()`
- `Window.into(...)` for non-merging windows
- `GroupByKey` in non-merging windows, including unbounded input in built-in fixed or sliding
  windows, with one final pane emitted when the input watermark passes each window end
- `Combine.GroupedValues` and common keyed combines such as `Sum.integersPerKey()`

Current limitations:

- No side inputs
- No merging windows
- No checkpoint restoration for unbounded sources
- No Beam state/timers support
- Unbounded `GroupByKey` and keyed combines support only built-in `FixedWindows`/`SlidingWindows`;
  custom window functions are rejected even if they claim to produce finite windows
- No custom triggers, multiple panes, or nonzero allowed lateness; data for closed windows is dropped
- Grouping state is held in memory until the source watermark passes each window end; stalled
  watermarks or large windows can still accumulate data

The implementation intentionally keeps the first supported transform set small and routes Beam
execution through Gearpump's low-level runtime instead of the older DSL-based runner design.

Beam applications opt into finite idle watermark progress. Grouping processors keep the recovery
start clock pinned to the original start time while event-time watermarks advance, because their
volatile state must be reconstructed from the original input, including across chained combines
that change timestamps. Recovery may replay the complete source history and duplicate outputs;
this does not add checkpoint restoration for unbounded sources. Native Gearpump tasks retain the
existing acknowledgement-based idle progress behavior by default.

See `examples/beam/quickstart` for a runnable Beam quick start that submits to a Gearpump local
cluster.
