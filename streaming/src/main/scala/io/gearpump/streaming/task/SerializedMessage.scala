package io.gearpump.streaming.task

import io.gearpump.Time.MilliSeconds

case class SerializedMessage(timeStamp: MilliSeconds, bytes: Array[Byte])
