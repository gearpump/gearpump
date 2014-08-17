/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.serializer

import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.gearpump.task.{Ack, TaskId, AckRequest, Message}
import com.esotericsoftware.kryo.{Kryo, Serializer}

class MessageSerializer extends Serializer[Message] {
  override def write(kryo: Kryo, output: Output, obj: Message) = {
    output.writeLong(obj.timestamp)
    output.writeString(obj.msg)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[Message]): Message = {
    var timeStamp = input.readLong()
    val msg = input.readString()
    return new Message(timeStamp, msg)
  }
}

class AckRequestSerializer extends Serializer[AckRequest] {
  override def write(kryo: Kryo, output: Output, obj: AckRequest) = {
    output.writeInt(obj.taskId.groupId)
    output.writeInt(obj.taskId.index)
    output.writeLong(obj.seq)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[AckRequest]): AckRequest = {
    val groupId = input.readInt()
    val index = input.readInt()
    val seq = input.readLong()

    return new AckRequest(TaskId(groupId, index), seq)
  }
}

class AckSerializer extends Serializer[Ack] {
  override def write(kryo: Kryo, output: Output, obj: Ack) = {
    output.writeInt(obj.taskId.groupId)
    output.writeInt(obj.taskId.index)
    output.writeLong(obj.seq)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[Ack]): Ack = {
    val groupId = input.readInt()
    val index = input.readInt()
    val seq = input.readLong()

    return new Ack(TaskId(groupId, index), seq)
  }
}