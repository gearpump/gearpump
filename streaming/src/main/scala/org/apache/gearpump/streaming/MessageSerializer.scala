/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming

import java.io.{DataInput, DataOutput}

import org.apache.gearpump.streaming.task._

class TaskIdSerializer extends TaskMessageSerializer[TaskId] {
  override def getLength(obj: TaskId): Int = 8

  override def write(dataOutput: DataOutput, obj: TaskId): Unit = {
    dataOutput.writeInt(obj.processorId)
    dataOutput.writeInt(obj.index)
  }

  override def read(dataInput: DataInput): TaskId = {
    val processorId = dataInput.readInt()
    val index = dataInput.readInt()
    new TaskId(processorId, index)
  }
}

class AckSerializer extends TaskMessageSerializer[Ack] {
  val taskIdSerializer = new TaskIdSerializer

  override def getLength(obj: Ack): Int = taskIdSerializer.getLength(obj.taskId) + 16

  override def write(dataOutput: DataOutput, obj: Ack): Unit = {
    taskIdSerializer.write(dataOutput, obj.taskId)
    dataOutput.writeShort(obj.seq)
    dataOutput.writeShort(obj.actualReceivedNum)
    dataOutput.writeInt(obj.sessionId)
    dataOutput.writeLong(obj.watermark)
  }

  override def read(dataInput: DataInput): Ack = {
    val taskId = taskIdSerializer.read(dataInput)
    val seq = dataInput.readShort()
    val actualReceivedNum = dataInput.readShort()
    val sessionId = dataInput.readInt()
    val watermark = dataInput.readLong()
    Ack(taskId, seq, actualReceivedNum, sessionId, watermark)
  }
}

class InitialAckRequestSerializer extends TaskMessageSerializer[InitialAckRequest] {
  val taskIdSerialzer = new TaskIdSerializer()

  override def getLength(obj: InitialAckRequest): Int = taskIdSerialzer.getLength(obj.taskId) + 4

  override def write(dataOutput: DataOutput, obj: InitialAckRequest): Unit = {
    taskIdSerialzer.write(dataOutput, obj.taskId)
    dataOutput.writeInt(obj.sessionId)
  }

  override def read(dataInput: DataInput): InitialAckRequest = {
    val taskId = taskIdSerialzer.read(dataInput)
    val sessionId = dataInput.readInt()
    InitialAckRequest(taskId, sessionId)
  }
}

class AckRequestSerializer extends TaskMessageSerializer[AckRequest] {
  val taskIdSerializer = new TaskIdSerializer

  override def getLength(obj: AckRequest): Int = taskIdSerializer.getLength(obj.taskId) + 14

  override def write(dataOutput: DataOutput, obj: AckRequest): Unit = {
    taskIdSerializer.write(dataOutput, obj.taskId)
    dataOutput.writeShort(obj.seq)
    dataOutput.writeInt(obj.sessionId)
    dataOutput.writeLong(obj.watermark)
  }

  override def read(dataInput: DataInput): AckRequest = {
    val taskId = taskIdSerializer.read(dataInput)
    val seq = dataInput.readShort()
    val sessionId = dataInput.readInt()
    val watermark = dataInput.readLong()
    AckRequest(taskId, seq, sessionId, watermark)
  }
}

class LatencyProbeSerializer extends TaskMessageSerializer[LatencyProbe] {
  override def getLength(obj: LatencyProbe): Int = 8

  override def write(dataOutput: DataOutput, obj: LatencyProbe): Unit = {
    dataOutput.writeLong(obj.timestamp)
  }

  override def read(dataInput: DataInput): LatencyProbe = {
    val timestamp = dataInput.readLong()
    LatencyProbe(timestamp)
  }
}