## What is At Least Once Message Delivery?

!!! note "Current implementation boundary"
    The delivery guarantees on this page are conditional on application-provided
    replayable sources and durable checkpoint stores. Current `master` exposes
    the required interfaces but does not ship the historical Kafka and Hadoop
    implementations used by older examples. See [Streaming Runtime
    Guarantees](../internals/runtime-guarantees.md) for the complete prerequisite
    and limitation matrix. Gearpump also has no built-in at-most-once mode;
    omitting replay only provides best-effort behavior unless the application
    supplies a no-redelivery protocol.

Messages could be lost on delivery due to network partitions. **At Least Once Message Delivery** (at least once) means the lost messages are delivered one or more times such that at least one is processed and acknowledged by the whole flow. 

Gearpump can provide at-least-once processing for a source that can replay
messages from a past timestamp and durably maps application timestamps to source
offsets. Each message carries an event-time timestamp. Each task reports a
progress clock derived from upstream watermarks, its processing watermark, and
its processing-watermark values acknowledged by downstream subscriptions.
`ClockService` takes the minimum across task instances as the application clock.
This is a conservative event-time progress frontier, not an inventory of pending
records or their exact minimum timestamp. On message loss, the application
restarts from a recovery clock. A conforming
`TimeReplayableSource` must replay every record that may not have completed after
that clock. Replay can produce duplicates, so sinks and external effects still
need idempotency or deduplication.

## What is Exactly Once Message Delivery?

At least once delivery doesn't guarantee the correctness of the application result. For instance,  for a task keeping the count of received messages, there could be overcount with duplicated messages and the count is lost on task failure.
 In that case, **Exactly Once Message Delivery** (exactly once) is required, where state is updated by a message exactly once. This further requires that duplicated messages are filtered out and in-memory states are persisted.

Gearpump's historical "exactly once" term applies to state managed by the Persistent API, not to arbitrary external side effects. It requires a `TimeReplayableSource`, checkpointing enabled on every participating stateful processor, and a durable application-provided `CheckpointStore`. Gearpump restores those states at a shared checkpoint clock and asks sources to replay from that clock. However, the current `PersistentTask` does not propagate its processing watermark, so consecutive stateful tasks cannot reliably advance that shared checkpoint clock. Database writes, HTTP calls, model calls, and other effects require a separate idempotency or transactional protocol.

### Persistent API
Persistent API consists of `PersistentTask` and `PersistentState`.

Here is an example of using them to keep count of incoming messages.

	:::scala
	class CountProcessor(taskContext: TaskContext, conf: UserConfig)
  	  extends PersistentTask[Long](taskContext, conf) {

  	  override def persistentState: PersistentState[Long] = {
        import com.twitter.algebird.Monoid.longMonoid
        new NonWindowState[Long](new AlgebirdMonoid(longMonoid), new ChillSerializer[Long])
      }

      override def processMessage(state: PersistentState[Long], message: Message): Unit = {
        state.update(message.timestamp, 1L)
      }
    }

   
The `CountProcessor` creates a customized `PersistentState` which will be managed by `PersistentTask` and overrides the `processMessage` method to define how the state is updated on a new message (each new message counts as `1`, which is added to the existing value)

Gearpump has already offered two types of states
 
1. NonWindowState - state with no time or other boundary
2. WindowState - each state is bounded by a time window

They are intended for states that satisfy monoid laws.

1. has binary associative operation, like `+`  
2. has an identity element, like `0`

In the above example, we make use of the `longMonoid` from [Twitter's Algebird](https://github.com/twitter/algebird) library which provides a bunch of useful monoids. 
