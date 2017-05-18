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

import java.util.concurrent.TimeUnit

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.TaskContext

import scala.concurrent.duration.FiniteDuration

class ThrottleTask[T](context: TaskContext, userConf : UserConfig)
  extends GraphTask(context, userConf) {

  val cost = userConf.getInt(ThrottleTask.COST).getOrElse(0)
  val costCalc = userConf.getValue[T => Int](ThrottleTask.COST_CALC)
  val maxBurst = userConf.getInt(ThrottleTask.MAX_BURST)
  val timePeriod = userConf.getValue[FiniteDuration](ThrottleTask.TIME_PERIOD).
    getOrElse(FiniteDuration(0, TimeUnit.MINUTES))
  val interval = timePeriod.toNanos / cost

  // TODO control rate from TaskActor
  override def onNext(msg: Message) : Unit = {
    val data = msg.value.asInstanceOf[T]
    val time = msg.timestamp
    context.output(msg)
  }
}

object ThrottleTask {
  val COST = "COST"
  val COST_CALC = "COST_CAL"
  val MAX_BURST = "MAX_BURST"
  val TIME_PERIOD = "TIME_PERIOD"
}
