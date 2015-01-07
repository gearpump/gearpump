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

import com.esotericsoftware.kryo.Kryo
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.serializer.GearpumpSerialization
import org.apache.gearpump.streaming.task._
import org.scalatest.{Matchers, WordSpec}

import com.esotericsoftware.kryo.io.{Input, Output}

class MessageSerializerSpec extends WordSpec with Matchers {
  val kryo = new Kryo
  val config = ClusterConfig.load.application
  GearpumpSerialization.init(config)
  val serialization = new GearpumpSerialization
  serialization.customize(kryo, config)
  val buffer = new Array[Byte](1024)
  val outPut = new Output(buffer)
  val input = new Input(buffer)

  "MessageSerializer" should {
    "serialize and deserialize Message properly" in {
      val serializer = kryo.getRegistration(classOf[Message]).getSerializer
      assert(serializer.isInstanceOf[MessageSerializer])
      val msgSerializer = serializer.asInstanceOf[MessageSerializer]
      val message = Message("test")
      msgSerializer.write(kryo, outPut, message)
      val result = msgSerializer.read(kryo, input, classOf[Message])
      assert(result.equals(message))
    }
  }

  "TaskIdSerializer"  should {
    "serialize and deserialize TaskId properly" in {
      val serializer = kryo.getRegistration(classOf[TaskId]).getSerializer
      assert(serializer.isInstanceOf[TaskIdSerializer])
      val taskIdSerializer = serializer.asInstanceOf[TaskIdSerializer]
      val taskId = TaskId(1, 3)
      taskIdSerializer.write(kryo, outPut, taskId)
      val result = taskIdSerializer.read(kryo, input, classOf[TaskId])
      assert(result.equals(taskId))
    }
  }

  "AckRequestSerializer"  should {
    "serialize and deserialize AckRequest properly" in {
      val serializer = kryo.getRegistration(classOf[AckRequest]).getSerializer
      assert(serializer.isInstanceOf[AckRequestSerializer])
      val ackRequestSerializer = serializer.asInstanceOf[AckRequestSerializer]
      val ackRequest = AckRequest(TaskId(1, 2), Seq(0, 1000), 1024)
      ackRequestSerializer.write(kryo, outPut, ackRequest)
      val result = ackRequestSerializer.read(kryo, input, classOf[AckRequest])
      assert(result.equals(ackRequest))
    }
  }

  "AckSerializer"  should {
    "serialize and deserialize Ack properly" in {
      val serializer = kryo.getRegistration(classOf[Ack]).getSerializer
      assert(serializer.isInstanceOf[AckSerializer])
      val ackSerializer = serializer.asInstanceOf[AckSerializer]
      val ack = Ack(TaskId(1, 2), Seq(1, 1024), 1023, 1799)
      ackSerializer.write(kryo, outPut, ack)
      val result = ackSerializer.read(kryo, input, classOf[Ack])
      assert(result.equals(ack))
    }
  }
}
