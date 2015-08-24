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

package io.gearpump.streaming.examples.sol

import java.util.Random

import io.gearpump.streaming.task.{StartTime, Task, TaskContext}
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.examples.sol.SOLStreamProducer._

class SOLStreamProducer(taskContext : TaskContext, conf: UserConfig) extends Task(taskContext, conf) {

  import taskContext.output

  private val sizeInBytes = conf.getInt(SOLStreamProducer.BYTES_PER_MESSAGE)
      .getOrElse(DEFAULT_MESSAGE_SIZE)
  private var messages : Array[String] = null
  private var rand : Random = null
  private var messageCount : Long = 0

  override def onStart(startTime : StartTime) : Unit = {
    prepareRandomMessage
    self ! Start
  }

  private def prepareRandomMessage = {
    rand = new Random()
    val differentMessages = 100
    messages = new Array(differentMessages)

    0.until(differentMessages).map { index =>
      val sb = new StringBuilder(sizeInBytes)
      //Even though java encodes strings in UCS2, the serialized version sent by the tuples
      // is UTF8, so it should be a single byte
      0.until(sizeInBytes).foldLeft(sb){(sb, j) =>
        sb.append(rand.nextInt(9));
      }
      messages(index) = sb.toString();
    }
  }

  override def onNext(msg : Message) : Unit = {
    val message = messages(rand.nextInt(messages.length))
    output(new Message(message, System.currentTimeMillis()))
    messageCount = messageCount + 1L
    self ! messageSourceMinClock
  }

  // messageSourceMinClock represent the min clock of the message source
  private def messageSourceMinClock : Message = {
    Message("tick", System.currentTimeMillis())
  }
}

object SOLStreamProducer {
  val DEFAULT_MESSAGE_SIZE = 100 // bytes
  val BYTES_PER_MESSAGE = "bytesPerMessage"
  val Start = Message("start")
}
