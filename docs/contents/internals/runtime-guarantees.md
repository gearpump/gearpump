# Streaming Runtime Guarantees

This page describes the guarantees implemented by the Gearpump streaming
runtime on the current `master` branch. It separates runtime mechanisms from
end-to-end delivery guarantees, which require application and deployment
support.

The short version is:

* Gearpump serializes managed callbacks within each task and applies
  per-connection flow control between tasks.
* Message-count acknowledgements detect some transport loss and trigger
  application-level recovery.
* Recovery replays data only when every source can restart from the selected
  application timestamp.
* Checkpointed state is recovered only when every participating stateful task
  uses `PersistentTask` with a durable `CheckpointStore`.
* These mechanisms do not make arbitrary calls to external systems exactly
  once.

## Guarantee matrix

| Area | What the runtime provides | Required application or deployment support | Boundary |
| --- | --- | --- | --- |
| Task execution | A `TaskActor` invokes one task's managed callbacks serially. | Task code must avoid unmanaged concurrent mutation. | There is no total order across multiple upstream tasks. |
| Task-to-task flow control | Each downstream task connection tracks outstanding message counts and stops the publisher task from draining more managed work at the configured limit. | Acknowledgements must continue to make progress. | This is not a hard bound on all memory: actor mailboxes and internal queues are not durable and may be unbounded. |
| Message-loss detection | Periodic count acknowledgements detect a sent/received count mismatch and report `MessageLoss`. | Messages must use the managed task transport. | Counts do not provide a durable per-event identity and cannot detect source omissions or duplicated external effects. |
| Watermark progress | Each task reports a clock derived from upstream, processing, and acknowledged downstream watermarks; the `ClockService` computes application progress. | Sources must report conservative watermarks and attach meaningful timestamps. | The runtime does not correct a source that advances its watermark too early. |
| Task or executor failure | A task exception is treated as message loss; an executor loss or message-loss report moves the application into recovery, subject to the restart budget. | The application must tolerate task recreation. | In-memory task state and messages are lost unless reconstructed from replay and checkpoints. |
| AppMaster or master failure | The master can recreate an AppMaster from replicated application metadata, and a multi-master cluster uses majority reads and writes. | Master quorum and a highly available shared jar store are required. | Master metadata is replicated in memory, not written to a durable database; losing the quorum or all replicas is outside this guarantee. |
| At-least-once processing | The recovered graph can restart from its selected start clock and process replayed input again. | Every source must be time-replayable and persist timestamp-to-offset information durably. | Replay can duplicate messages and effects. |
| Checkpointed-state processing | The recovery clock can be limited to a checkpoint shared by all checkpoint-enabled tasks, and `PersistentTask` restores state at that time. | All relevant tasks need checkpointing enabled, a compatible `PersistentState`, and a durable `CheckpointStore`; sources must replay from the same time. | This covers Gearpump-managed state only, not arbitrary sinks, RPCs, model calls, or tool calls. |

## Processing and ordering

Every streaming task runs inside a `TaskActor`. Managed input is placed in the
task's internal queue and the actor calls `Task.onNext` serially. This prevents
two managed callbacks for the same task from running at the same time.

This is a task-local property. Partitioners decide which downstream task
receives a message, and multiple publishers can feed the same task. Gearpump
does not expose a global ordering guarantee across those publishers. Event
timestamps and watermarks describe event-time progress; they are not sequence
numbers and do not sort the input automatically.

Messages passed through `Task.receiveUnManagedMessage` are outside the managed
streaming protocol. Applications must provide their own ordering, flow control,
replay, and state-safety rules for such messages.

## Flow control and loss detection

A `Subscription` represents the edges from one publishing task to one
downstream processor. It keeps a sent count and an outstanding count for every
downstream task:

1. After a configurable number of messages, the publisher sends an
   `AckRequest`.
2. The downstream task responds with the count accepted in the current
   transport session and its watermark.
3. A count mismatch raises `MsgLostException` and reports `MessageLoss` to the
   AppMaster.
4. If any downstream connection reaches the pending-message limit, the
   publisher task stops draining managed work until acknowledgements reduce the
   outstanding count.

This propagates pressure upstream through the graph, but it does not make the
whole application memory-bounded. The default task dispatcher uses an
unbounded actor mailbox, and `TaskActor` also has an in-memory queue. Operators
must monitor queueing and latency, and sources must stop or slow ingestion when
backpressure reaches them.

The relevant settings are:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `gearpump.streaming.ack-once-every-message-count` | `100` | Send a count acknowledgement request after this many messages. |
| `gearpump.streaming.max-pending-message-count-per-connection` | `1000` | Stop draining managed work when a task-to-task connection reaches this outstanding count. It must be greater than or equal to the acknowledgement interval and less than half of `Short.MaxValue`. |
| `gearpump.application.total-retries` | `5` | Restart budget used by application and task recovery. Exhausting it fails the application. |

Count acknowledgements detect a discrepancy between messages sent and accepted
inside a live task session. They are not a durable event log. They do not prove
that a source produced every expected record, that user code applied a record
only once, or that an external system accepted an effect only once.

## Watermarks and the recovery clock

Sources supply messages with timestamps and report progress through
`DataSource.getWatermark`. For a non-source task, Gearpump combines:

* the minimum watermark reported by its upstream processors;
* the watermark that the task says it has processed; and
* the minimum acknowledged watermark of its downstream subscriptions.

The `ClockService` tracks these values across task parallelism. It snapshots a
start clock to the master-side application metadata store every five seconds.
During recovery, `TaskManager` restarts the graph at the clock returned by the
service:

* the shared minimum checkpoint clock when checkpoint-enabled tasks exist; or
* the application's current minimum clock otherwise.

Correctness therefore depends on timestamp and watermark discipline. A source
must not report a watermark past records that it may still emit, and a replay
implementation must map the recovery timestamp back to source offsets without
skipping records. Records may be replayed from before the exact failure point.

## Failure and recovery behavior

### Task exception or detected message loss

The executor supervisor reports any task exception as `MessageLoss`. If the
restart policy allows another attempt, `TaskManager` enters recovery and tells
the executors to recreate the tasks for the current DAG version. New transport
session IDs prevent stale senders from being accepted by the recreated tasks.

The runtime restores no ordinary task fields. The application must reconstruct
them from input replay or from the persistent-state API.

### Executor or worker failure

If an executor stops, the AppMaster requests replacement resources, relaunches
the affected executor tasks, and coordinates the same application recovery.
Recovery is bounded by `gearpump.application.total-retries` and the configured
resource-allocation timeouts.

### AppMaster or master failure

The master stores application metadata and the most recent recovery clock in a
Pekko Distributed Data `LWWMap` using majority consistency. A surviving master
quorum can relaunch an AppMaster. Application jars must also be reachable from
the replacement master; use a highly available HDFS or shared filesystem jar
store rather than a master-local path.

This metadata store is in memory. A deployment must not treat it as disaster
recovery after all master replicas or the quorum are lost.

## Delivery levels and prerequisites

### Without replay and persistent state

Gearpump still provides task isolation, flow control, watermarks, loss
detection, and restart attempts. After a failure, however, a non-replayable
source cannot reconstruct lost input and ordinary in-memory task state cannot
be restored. This is best-effort recovery rather than an end-to-end delivery
guarantee.

### At least once

At-least-once processing requires every source to implement the
`TimeReplayableSource` contract and durably checkpoint the mapping between
application timestamps and source offsets. When Gearpump restarts at an earlier
clock, each source must replay all records that may not have completed.

Replaying conservatively can deliver a record more than once. Sinks and other
effects must therefore be idempotent, transactional, or able to deduplicate by
a stable application event ID.

### Checkpointed state (historically called exactly once)

Gearpump's existing documentation calls its persisted-state behavior "exactly
once." The precise scope is narrower: replayed messages are intended to affect
the state managed by `PersistentTask` once in the recovered state history.

The application must satisfy all of these prerequisites:

1. Every source needed for recovery is time-replayable from the selected clock.
2. Every participating stateful processor sets `state.checkpoint.enable` to
   `true`.
3. Its task extends `PersistentTask`, supplies a compatible `PersistentState`,
   and configures `state.checkpoint.interval.ms`.
4. `state.checkpoint.store.factory` creates a durable, shared
   `CheckpointStore` whose checkpoints remain available to replacement tasks.
5. State serialization and task code remain compatible with stored
   checkpoints.

`PersistentTask` saves state after the upstream watermark reaches a checkpoint
boundary. The task reports each completed checkpoint to `ClockService`, which
only advances the shared checkpoint clock after all checkpoint-enabled tasks
report the same time. Recovery loads state at that clock and replays input from
the same point.

## External effects are not exactly once

Gearpump does not atomically commit a state checkpoint together with an
external side effect. A task can call a database, HTTP API, model endpoint, or
tool successfully and fail before its next checkpoint. Replay can then issue
the call again.

Applications that perform external mutations must add an appropriate protocol,
for example:

* a stable idempotency key understood by the destination;
* a transactional sink that commits consistently with recovered progress;
* an inbox/outbox or effect journal; or
* explicit human reconciliation for operations that cannot be retried safely.

No Gearpump API currently supplies these protocols automatically.

## Current implementation limitations

The current source tree defines the `TimeReplayableSource`, `CheckpointStore`,
`PersistentTask`, and `PersistentState` contracts. It does not currently ship a
production replayable source or durable checkpoint-store implementation. The
only included `CheckpointStore` is `InMemoryCheckpointStore`, which is marked
for tests and clears its contents on close. Applications must supply and test
their production implementations.

Some older documentation examples refer to built-in `KafkaSource`,
`KafkaStorageFactory`, and `HadoopCheckpointStore` classes. Those classes are
not present on current `master`; treat the examples as historical architecture,
not deployable instructions.

The experimental Beam runner does not change these guarantees. Beam state and
timers are not backed by Gearpump persistent state, grouping is currently
in-memory, and the unbounded-source wrapper does not restore a Beam checkpoint
mark. A pipeline using that runner must be evaluated against its implemented
transforms rather than assuming the guarantees on this page.

## Source and detailed design references

* [`TaskActor`](https://github.com/gearpump/gearpump/blob/master/streaming/src/main/scala/io/gearpump/streaming/task/TaskActor.scala)
  and [`Subscription`](https://github.com/gearpump/gearpump/blob/master/streaming/src/main/scala/io/gearpump/streaming/task/Subscription.scala)
* [`TaskManager`](https://github.com/gearpump/gearpump/blob/master/streaming/src/main/scala/io/gearpump/streaming/appmaster/TaskManager.scala)
  and [`ClockService`](https://github.com/gearpump/gearpump/blob/master/streaming/src/main/scala/io/gearpump/streaming/appmaster/ClockService.scala)
* [`TimeReplayableSource`](https://github.com/gearpump/gearpump/blob/master/streaming/src/main/scala/io/gearpump/streaming/transaction/api/TimeReplayableSource.scala)
  and [`CheckpointStore`](https://github.com/gearpump/gearpump/blob/master/streaming/src/main/scala/io/gearpump/streaming/transaction/api/CheckpointStore.scala)
* [`PersistentTask`](https://github.com/gearpump/gearpump/blob/master/streaming/src/main/scala/io/gearpump/streaming/state/api/PersistentTask.scala)
* [Gearpump internals](gearpump-internals.md)
* [Master high-availability deployment](../deployment/deployment-ha.md)
