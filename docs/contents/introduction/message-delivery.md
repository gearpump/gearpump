## What is At Least Once Message Delivery?

Messages could be lost on delivery due to network partitions. **At Least Once Message Delivery** (at least once) means the lost messages are delivered one or more times such that at least one is processed and acknowledged by the whole flow. 

Gearpump guarantees at least once for any source that is able to replay message from a past timestamp. In Gearpump, each message is tagged with a timestamp, and the system tracks the minimum timestamp of all pending messages (the global minimum clock). On message loss, application will be restarted to the global minimum clock. Since the source is able to replay from the global minimum clock, all pending messages before the restart will be replayed. Gearpump calls that kind of source `TimeReplayableSource` and already provides a built in
[KafkaSource](../internals/gearpump-internals#at-least-once-message-delivery-and-kafka). With the KafkaSource to ingest data into Gearpump, users are guaranteed at least once message delivery.

## What is Exactly Once Message Delivery?

At least once delivery doesn't guarantee the correctness of the application result. For instance,  for a task keeping the count of received messages, there could be overcount with duplicated messages and the count is lost on task failure.
 In that case, **Exactly Once Message Delivery** (exactly once) is required, where state is updated by a message exactly once. This further requires that duplicated messages are filtered out and in-memory states are persisted.

Users are guaranteed exactly once in Gearpump if they use both a `TimeReplayableSource` to ingest data and the Persistent API to manage their in memory states. With the Persistent API, user state is periodically checkpointed by the system to a persistent store (e.g HDFS) along with its checkpointed time. Gearpump tracks the global minimum checkpoint timestamp of all pending states (global minimum checkpoint clock), which is persisted as well. On application restart, the system restores states at the global minimum checkpoint clock and source replays messages from that clock. This ensures that a message updates all states exactly once.

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
