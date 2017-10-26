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

import org.apache.gearpump.streaming.source.Watermark
import org.jboss.netty.buffer.{ChannelBufferOutputStream, ChannelBuffers}
import org.scalatest.{Matchers, WordSpec}
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.transport.netty.WrappedChannelBuffer

class MessageSerializerSpec extends WordSpec with Matchers {

  def testSerializer[T](obj: T, taskMessageSerializer: TaskMessageSerializer[T]): T = {
    val length = taskMessageSerializer.getLength(obj)
    val bout = new ChannelBufferOutputStream(ChannelBuffers.buffer(length))
    taskMessageSerializer.write(bout, obj)
    val input = new WrappedChannelBuffer(ChannelBuffers.wrappedBuffer(bout.buffer().array()))
    taskMessageSerializer.read(input)
  }

  "SerializedMessageSerializer" should {
    "serialize and deserialize SerializedMessage properly" in {
      val serializer = new SerializedMessageSerializer
      val data = new Array[Byte](256)
      new java.util.Random().nextBytes(data)
      val msg = SerializedMessage(1024, data)
      val result = testSerializer(msg, serializer)
      assert(result.timeStamp == msg.timeStamp && result.bytes.sameElements(msg.bytes))
    }
  }

  "TaskIdSerializer" should {
    "serialize and deserialize TaskId properly" in {
      val taskIdSerializer = new TaskIdSerializer
      val taskId = TaskId(1, 3)
      assert(testSerializer(taskId, taskIdSerializer).equals(taskId))
    }
  }

  "AckRequestSerializer" should {
    "serialize and deserialize AckRequest properly" in {
      val serializer = new AckRequestSerializer
      val ackRequest = AckRequest(TaskId(1, 2), 1000, 1024, Watermark.MAX.toEpochMilli)
      assert(testSerializer(ackRequest, serializer).equals(ackRequest))
    }
  }

  "InitialAckRequestSerializer" should {
    "serialize and deserialize AckRequest properly" in {
      val serializer = new InitialAckRequestSerializer
      val ackRequest = InitialAckRequest(TaskId(1, 2), 1024)
      assert(testSerializer(ackRequest, serializer).equals(ackRequest))
    }
  }

  "AckSerializer" should {
    "serialize and deserialize Ack properly" in {
      val serializer = new AckSerializer
      val ack = Ack(TaskId(1, 2), 1024, 1023, 1799, Watermark.MAX.toEpochMilli)
      assert(testSerializer(ack, serializer).equals(ack))
    }
  }
}
