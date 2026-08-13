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
* Gearpump does not expose a built-in at-most-once delivery mode. Without an
  application-provided delivery protocol, recovery behavior is best effort.
* `PersistentTask` does not currently propagate watermark progress. It can
  prevent bounded completion and stall later checkpoints.
* These mechanisms do not make arbitrary calls to external systems exactly
  once.

## Guarantee matrix

| Area | What the runtime provides | Required application or deployment support | Boundary |
| --- | --- | --- | --- |
| Task execution | A `TaskActor` invokes one task's managed callbacks serially. | Task code must avoid unmanaged concurrent mutation. | There is no total order across multiple upstream tasks. |
| Task-to-task flow control | Each downstream task connection tracks outstanding message counts and stops the publisher task from draining more managed work at the configured limit. | Acknowledgements must continue to make progress. | This is not a hard bound on all memory: actor mailboxes and internal queues are not durable and may be unbounded. |
| Message-loss detection | Periodic count acknowledgements detect a sent/received count mismatch and report `MessageLoss`. | Messages must use the managed task transport. | Counts do not provide a durable per-event identity and cannot detect source omissions or duplicated external effects. |
| Watermark progress | Each task reports a clock derived from upstream and processing watermarks plus the publisher processing watermarks echoed in downstream acknowledgements; the `ClockService` computes application progress. | Sources must report conservative watermarks and attach meaningful timestamps. | The runtime does not correct a source that advances its watermark too early. `PersistentTask` fails to propagate its processing watermark, so downstream progress and bounded completion can stall. |
| Task or executor failure | A task exception is treated as message loss. In the steady application-ready state, an executor loss is reprocessed during recovery and can request replacement resources, subject to the restart budget. | The application must tolerate task recreation. | In-memory task state and messages are lost unless reconstructed from replay and checkpoints. During an in-progress dynamic DAG update, an executor loss enters recovery without replacement-resource scheduling and can stall it. |
| AppMaster failure | The `AppManager` that accepted an application can relaunch its AppMaster while its process-local restart policy permits. | Application metadata and the jar must remain available. | Exhaustion only logs an error and leaves the application nonterminal and unrecovered. After `AppManager` failover, restart policies are not reconstructed, so a later AppMaster failure can fail instead of relaunching. |
| Master failure | A multi-master cluster replicates application-registry metadata and the recovery clock with majority reads and writes. | A master quorum and a shared jar backend are required; affected workers may also need restarting so they resolve the replacement master's jar-server endpoint. | A replacement `AppManager` does not reconstruct restart policies, cached application results, or result listeners. Workers that have already resolved the jar server cache the failed master's URL. The replicated metadata is memory-resident, so quorum or all-replica loss is outside this boundary. |
| At-most-once processing | Gearpump has no runtime-selectable at-most-once mode; recovery recreates tasks and passes its selected start clock to every source. | Each source and downstream effect path must independently avoid redelivery and retry, and the application must accept failure-time loss. | The runtime neither persists per-record identities nor deduplicates arbitrary `DataSource` output or external effects, so Gearpump alone does not guarantee at most once. |
| At-least-once processing | The recovered graph can restart from its selected start clock and process replayed input again. | Every source must be time-replayable and persist timestamp-to-offset information durably. | Replay can duplicate messages and effects. |
| Checkpointed-state processing | The recovery clock can be limited to a checkpoint shared by all checkpoint-enabled tasks, and `PersistentTask` restores state at that time. | All relevant tasks need checkpointing enabled, a compatible `PersistentState`, and a durable `CheckpointStore`; sources must replay from the same time. Every enabled task must keep reporting aligned checkpoint times. | Consecutive `PersistentTask`s cannot currently propagate later checkpoint progress, and an idle or unevenly active task can prevent exact checkpoint-time alignment. This covers Gearpump-managed state only, not arbitrary external effects. |

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
2. The downstream task responds with the transport-session count it has
   accepted and echoes the publisher's processing watermark carried by that
   `AckRequest`.
3. A count mismatch raises `MsgLostException` and reports `MessageLoss` to the
   AppMaster.
4. If any downstream connection reaches the pending-message limit, the
   publisher task stops draining managed work until acknowledgements reduce the
   outstanding count.

The echoed value is not the downstream task's own watermark. After receiving a
count-matched `Ack`, the publisher records that value as acknowledged output
progress for the subscription.

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
| `gearpump.application.total-retries` | `5` | Initializes separate master-side AppMaster and in-AppMaster task/executor retry policies. Task or executor exhaustion reports `FailedToRecover`; AppMaster exhaustion only logs an error and leaves the application nonterminal and unrecovered. |

Count acknowledgements detect a discrepancy between messages sent and accepted
inside a live task session. They are not a durable event log. They do not prove
that a source produced every expected record, that user code applied a record
only once, or that an external system accepted an effect only once.

## Watermarks and the recovery clock

Sources supply messages with timestamps and report progress through
`DataSource.getWatermark`. For a non-source task, Gearpump combines:

* the minimum watermark reported by its upstream processors;
* the watermark that the task says it has processed; and
* the minimum of its processing-watermark values acknowledged by downstream
  subscriptions.

These clocks are conservative progress bounds, not an exact inventory of the
timestamps of pending records. They can lag the records actually in flight or
already processed.

The `ClockService` tracks these values across task parallelism. It snapshots a
start clock to the master-side application metadata store every five seconds.
During recovery, `TaskManager` restarts the graph at the clock returned by the
service:

* the shared minimum checkpoint clock when checkpoint-enabled tasks exist; or
* the application's current minimum clock otherwise.

!!! warning "PersistentTask watermark propagation"
    Unlike the base `Task` implementation, `PersistentTask.onWatermarkProgress`
    does not call `TaskContext.updateWatermark`. Unless application code updates
    it separately, the task's `TaskActor` keeps its processing watermark at the
    initial value. Its reported clock cannot advance beyond the task's birth
    time, and downstream processors do not receive advancing upstream
    watermarks.

    This has two consequences. A bounded application containing such a task can
    remain `ACTIVE` after its sources finish because the application clock never
    reaches `Watermark.MAX` and `ClockService` never emits `EndingClock`. In a
    serial stateful topology, the next checkpoint-enabled `PersistentTask`
    cannot reach later checkpoint boundaries, so the shared checkpoint clock
    also stalls.

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

In the steady `applicationReady` state, `TaskManager` queues an
`ExecutorStopped` event for its recovery behavior. Recovery applies the restart
policy, asks `JarScheduler` for replacement resource requests, relaunches the
affected tasks after resources arrive, and coordinates graph restart. This path
is bounded by `gearpump.application.total-retries` and the configured
resource-allocation timeouts. Exhausting the policy sends `FailedToRecover` to
the AppMaster.

!!! warning "Executor loss during a dynamic DAG update"
    In the `dynamicDag` state, `TaskManager` consumes `ExecutorStopped` by
    switching to recovery but does not queue the event for replacement-resource
    scheduling. Recovery can therefore wait indefinitely for tasks assigned to
    the stopped executor. Do not treat executor or worker recovery as guaranteed
    during a dynamic DAG transition.

### AppMaster or master failure

The master stores application-registry metadata and the most recent recovery
clock in Pekko Distributed Data `LWWMap`s using majority consistency. While the
same `AppManager` remains alive, it can relaunch a failed AppMaster from stored
application metadata while a separate process-local restart policy allows it.
If that policy is exhausted, the current implementation only logs the failure;
it does not assign a terminal application status or notify result listeners.

Master failover does not preserve that full behavior. A replacement
`AppManager` restores the application registry but does not reconstruct its
`appMasterRestartPolicies`, `applicationResults`, or `appResultListeners` maps.
If a restored application's AppMaster later dies, recovery directly indexes the
missing policy and can fail instead of launching a replacement. A client that
registered `RunningApplication.waitUntilFinish` before failover is not
automatically re-registered and can wait until its timeout without receiving the
terminal result; a result cached only by the old `AppManager` is also
unavailable.

Jar availability has a separate endpoint limitation. A shared HDFS or
filesystem root makes the jar bytes visible to a replacement master, but each
worker owns a `JarStoreClient` that caches the first master's HTTP file-server
URL it resolves. Each master starts that server on a dynamically selected port.
After master failure, a worker that cached the old URL can fail a later executor
launch even though the jar bytes are shared. Until endpoint refresh is
implemented, affected workers must be restarted so they resolve the replacement
master; shared storage alone does not make jar access transparently highly
available.

The replicated metadata itself is in memory, so a deployment must not treat it
as disaster recovery after all master replicas or the quorum are lost.

## Delivery levels and prerequisites

### Without an application delivery protocol

Gearpump still provides task isolation, flow control, watermarks, loss
detection, and restart attempts. After a failure, however, a non-replayable
source cannot reconstruct lost input and ordinary in-memory task state cannot
be restored. This is best-effort recovery rather than an end-to-end delivery
guarantee.

### At most once (application-defined)

At most once means that a record is not processed again after failure, at the
cost of losing records that were in flight or whose in-memory effects were not
preserved. Gearpump does not expose a delivery-mode switch that enforces this
policy. Recovery recreates the graph and passes its selected `startTime` to
every `DataSource.open`; each source decides whether that time causes replay.

An application can obtain at-most-once behavior only when every source and
downstream effect path independently prevents redelivery and retry and the
application accepts failure-time loss. Count acknowledgements do not assign
durable event identities or deduplicate callbacks, and an arbitrary
`DataSource` may redeliver. Without those application-level constraints,
Gearpump provides best-effort recovery rather than an at-most-once guarantee.

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
advances the shared checkpoint clock only to a timestamp reported by every
checkpoint-enabled task instance. Recovery loads state at that clock and
replays input from the same point.

A `PersistentTask` schedules a checkpoint boundary only after it processes a
message. If an enabled task instance, for example an idle parallel task or
partition, has no message that schedules the same later boundary, it does not
report that timestamp merely because its upstream watermark advances. It can
therefore hold recovery at the last checkpoint shared by all enabled tasks,
often the initial start clock.

Separately, this path is not currently reliable for consecutive
`PersistentTask`s. Because the upstream stateful task does not propagate its
processing watermark, the downstream stateful task cannot reach later
checkpoint boundaries. Do not rely on the checkpointed-state delivery level
while either limitation applies.

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
* [`AppManager`](https://github.com/gearpump/gearpump/blob/master/core/src/main/scala/io/gearpump/cluster/master/AppManager.scala)
* [Historical Gearpump internals design](gearpump-internals.md)
* [Master high-availability deployment](../deployment/deployment-ha.md)
