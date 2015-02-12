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

package org.apache.gearpump.streaming.examples.kafka.topn

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}


class RollingCount(taskContext : TaskContext, conf: UserConfig) extends Task(taskContext, conf) {

  import org.apache.gearpump.streaming.examples.kafka.topn.Config._
  import taskContext.output

  private val windowLengthMS = getWindowLengthMS(conf)
  private val emitFrequencyMS = getEmitFrequencyMS(conf)
  private var lastEmitTime = 1L

  private val counter: SlidingWindowCounter[String] = new SlidingWindowCounter[String](windowLengthMS / emitFrequencyMS)

  def onStart(time: StartTime): Unit = {}

  def onNext(msg: Message): Unit = {
    val timestamp = msg.timestamp
    if (timestamp - lastEmitTime >= emitFrequencyMS) {
      counter.getCountsThenAdvanceWindow.foreach {
        case (obj, count) =>
          output(Message((obj, count), timestamp))
      }
      lastEmitTime = timestamp
    }
    counter.incrementCount(msg.msg.asInstanceOf[String])
  }
}
