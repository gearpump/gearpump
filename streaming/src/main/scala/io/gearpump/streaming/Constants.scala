package io.gearpump.streaming

import io.gearpump.streaming.partitioner.{BroadcastPartitioner, CoLocationPartitioner, HashPartitioner, ShuffleGroupingPartitioner, ShufflePartitioner}

object Constants {

  val GEARPUMP_STREAMING_OPERATOR = "gearpump.streaming.dsl.operator"
  val GEARPUMP_STREAMING_SOURCE = "gearpump.streaming.source"
  val GEARPUMP_STREAMING_GROUPBY_FUNCTION = "gearpump.streaming.dsl.groupby-function"

  val GEARPUMP_STREAMING_LOCALITIES = "gearpump.streaming.localities"

  val GEARPUMP_STREAMING_REGISTER_TASK_TIMEOUT_MS = "gearpump.streaming.register-task-timeout-ms"

  val GEARPUMP_STREAMING_MAX_PENDING_MESSAGE_COUNT =
    "gearpump.streaming.max-pending-message-count-per-connection"

  val GEARPUMP_STREAMING_ACK_ONCE_EVERY_MESSAGE_COUNT =
    "gearpump.streaming.ack-once-every-message-count"

  // The partitioners provided by Gearpump
  val BUILTIN_PARTITIONERS = Array(
    classOf[BroadcastPartitioner],
    classOf[CoLocationPartitioner],
    classOf[HashPartitioner],
    classOf[ShuffleGroupingPartitioner],
    classOf[ShufflePartitioner])
}
