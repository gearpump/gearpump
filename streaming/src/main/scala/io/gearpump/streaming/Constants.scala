package io.gearpump.streaming

object Constants {

  val GEARPUMP_STREAMING_OPERATOR = "gearpump.streaming.dsl.operator"
  val GEARPUMP_STREAMING_SOURCE = "gearpump.streaming.dsl.source"
  val GEARPUMP_STREAMING_SINK = "gearpump.streaming.dsl.sink"
  val GEARPUMP_STREAMING_GROUPBY_FUNCTION = "gearpump.streaming.dsl.groupby-function"

  val GEARPUMP_STREAMING_LOCALITIES = "gearpump.streaming.localities"

  val GEARPUMP_STREAMING_REGISTER_TASK_TIMEOUT_MS = "gearpump.streaming.register-task-timeout-ms"

  val GEARPUMP_STREAMING_MAX_PENDING_MESSAGE_COUNT = "gearpump.streaming.max-pending-message-count-per-connection"
  val GEARPUMP_STREAMING_ACK_ONCE_EVERY_MESSAGE_COUNT = "gearpump.streaming.ack-once-every-message-count"

  val CLUE_KEY_NAME = "gearpump.streaming.processor.name"
}
