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

package org.apache.gearpump.streaming

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.transport.{ExpressAddress, HostPort}

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

class TaskIdSerializer  extends Serializer[TaskId] {
  override def write(kryo: Kryo, output: Output, obj: TaskId) = {
    output.writeInt(obj.groupId)
    output.writeInt(obj.index)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[TaskId]): TaskId = {
    val groupId = input.readInt()
    val index = input.readInt()
    return new TaskId(groupId, index)
  }
}

class AckRequestSerializer extends Serializer[AckRequest] {
  val taskIdSerialzer = new TaskIdSerializer()

  override def write(kryo: Kryo, output: Output, obj: AckRequest) = {
    taskIdSerialzer.write(kryo, output, obj.taskId)
    output.writeInt(obj.seq.id)
    output.writeLong(obj.seq.seq)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[AckRequest]): AckRequest = {
    val taskId = taskIdSerialzer.read(kryo, input, classOf[TaskId])
    val id = input.readInt()
    val seq = input.readLong()
    return new AckRequest(taskId, Seq(id, seq))
  }
}

class AckSerializer extends Serializer[Ack] {
  val taskIdSerialzer = new TaskIdSerializer()

  override def write(kryo: Kryo, output: Output, obj: Ack) = {
    taskIdSerialzer.write(kryo, output, obj.taskId)
    output.writeInt(obj.seq.id)
    output.writeLong(obj.seq.seq)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[Ack]): Ack = {
    val taskId = taskIdSerialzer.read(kryo, input, classOf[TaskId])
    val id = input.readInt()
    val seq = input.readLong()
    return new Ack(taskId, Seq(id, seq))
  }
}

class ExpressAddressSerializer extends Serializer[ExpressAddress] {

  override def write(kryo: Kryo, output: Output, obj: ExpressAddress) = {
    output.writeString(obj.hostPort.host)
    output.writeInt(obj.hostPort.port)
    output.writeInt(obj.id)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[ExpressAddress]): ExpressAddress = {
    val host = input.readString()
    val port = input.readInt()
    val id =  input.readInt()

    return new ExpressAddress(HostPort(host, port), id)
  }
}


class IdentitySerializer extends Serializer[Identity] {
  val taskIdSerialzer = new TaskIdSerializer()
  val expressAddressSerializer = new ExpressAddressSerializer()

  override def write(kryo: Kryo, output: Output, obj: Identity) = {
    taskIdSerialzer.write(kryo, output, obj.taskId)
    expressAddressSerializer.write(kryo, output, obj.address)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[Identity]): Identity = {
    val taskId = taskIdSerialzer.read(kryo, input, classOf[TaskId])
    val expressAddress = expressAddressSerializer.read(kryo, input, classOf[ExpressAddress])
    return new Identity(taskId, expressAddress)
  }
}