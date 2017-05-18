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

package org.apache.gearpump.akkastream.task

import java.time.Instant
import java.util.concurrent.TimeUnit

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.TaskContext

import scala.concurrent.duration.FiniteDuration

class TickSourceTask[T](context: TaskContext, userConf : UserConfig)
  extends GraphTask(context, userConf) {

  val initialDelay = userConf.getValue[FiniteDuration](TickSourceTask.INITIAL_DELAY).
    getOrElse(FiniteDuration(0, TimeUnit.MINUTES))
  val interval = userConf.getValue[FiniteDuration](TickSourceTask.INTERVAL).
    getOrElse(FiniteDuration(0, TimeUnit.MINUTES))
  val tick = userConf.getValue[T](TickSourceTask.TICK).get

  override def onStart(startTime: Instant): Unit = {
    context.schedule(initialDelay, interval)(
      self ! Message(tick, Instant.now())
    )
  }

  override def onNext(msg: Message) : Unit = {
    context.output(msg)
  }
}

object TickSourceTask {
  val INITIAL_DELAY = "INITIAL_DELAY"
  val INTERVAL = "INTERVAL"
  val TICK = "TICK"
}
