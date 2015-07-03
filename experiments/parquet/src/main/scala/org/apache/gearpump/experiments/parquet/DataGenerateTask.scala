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
package org.apache.gearpump.experiments.parquet

import java.util.Random

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.parquet.serializer.GenericAvroSerializer
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

import scala.collection.mutable.StringBuilder

class DataGenerateTask(taskContext : TaskContext, config: UserConfig) extends Task(taskContext, config) {
  import taskContext.output
  val avroSerializer = new GenericAvroSerializer[User](classOf[User])
  private var names : Array[String] = null
  val rand = new Random()

  override def onStart(startTime : StartTime) = {
    prepareRandomNames
    self ! Message("start")
  }

  private def prepareRandomNames = {
    val differentNames = 100
    val nameSize = 10
    names = new Array(differentNames)

    0.until(differentNames).map { index =>
      val sb = new StringBuilder(nameSize)
      //Even though java encodes strings in UCS2, the serialized version sent by the tuples
      // is UTF8, so it should be a single byte
      0.until(nameSize).foldLeft(sb){(sb, j) =>
        sb.append(rand.nextInt(9))
      }
      names(index) = sb.toString()
    }
  }

  override def onNext(msg: Message): Unit = {
    val user = new User(names(rand.nextInt(names.length)), System.currentTimeMillis())
    output(Message(avroSerializer.serialize(user), System.currentTimeMillis()))
    self ! messageSourceMinClock
  }

  override def onStop(): Unit ={
    avroSerializer.close()
  }

  private def messageSourceMinClock : Message = {
    Message("tick", System.currentTimeMillis())
  }
}
