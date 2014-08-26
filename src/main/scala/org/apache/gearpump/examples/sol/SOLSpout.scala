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

package org.apache.gearpump.examples.sol

import java.util.Random

import akka.serialization.SerializationExtension
import org.apache.gearpump.task.{Message, TaskActor}
import org.apache.gears.cluster.Configs

class SOLSpout(conf : Configs) extends TaskActor(conf) {
  import org.apache.gearpump.examples.sol.SOLSpout._

  private val sizeInBytes = conf.getInt(SOLSpout.BYTES_PER_MESSAGE)

  private var messages : Array[String] = null
  private var rand : Random = null
  private var messageCount : Long = 0

  override def onStart() : Unit = {
    prepareRandomMessage
    self ! Start
    val s = SerializationExtension(context.system)
    val serializer = s.findSerializerFor("hello")
    val serialized = serializer.toBinary("hello")

    Console.println(s"Active serialization for string is: $serializer")


  }

  private def prepareRandomMessage = {
    rand = new Random()
    val differentMessages = 100;
    messages = new Array(differentMessages)

    0.until(differentMessages).map { index =>
      val sb = new StringBuilder(sizeInBytes);
      //Even though java encodes strings in UCS2, the serialized version sent by the tuples
      // is UTF8, so it should be a single byte
      0.until(sizeInBytes).foldLeft(sb){(sb, j) =>
        sb.append(rand.nextInt(9));
      }
      messages(index) = sb.toString();
    }
  }

  override def onNext(msg : String) : Unit = {
    val message = messages(rand.nextInt(messages.length))
    output(message)
    messageCount = messageCount + 1L

    self ! Continue
  }
}

object SOLSpout{
  val BYTES_PER_MESSAGE = "bytesPerMessage"

  val Start = Message(0, "start")
  val Continue = Message(0, "continue")
}
