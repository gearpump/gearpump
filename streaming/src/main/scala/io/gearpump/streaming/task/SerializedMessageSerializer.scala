package io.gearpump.streaming.task

import java.io.{DataInput, DataOutput}

class SerializedMessageSerializer extends TaskMessageSerializer[SerializedMessage] {
  override def getLength(obj: SerializedMessage): Int = 12 + obj.bytes.length

  override def write(dataOutput: DataOutput, obj: SerializedMessage): Unit = {
    dataOutput.writeLong(obj.timeStamp)
    dataOutput.writeInt(obj.bytes.length)
    dataOutput.write(obj.bytes)
  }

  override def read(dataInput: DataInput): SerializedMessage = {
    val timestamp = dataInput.readLong()
    val length = dataInput.readInt()
    val bytes = new Array[Byte](length)
    dataInput.readFully(bytes)
    SerializedMessage(timestamp, bytes)
  }
}
