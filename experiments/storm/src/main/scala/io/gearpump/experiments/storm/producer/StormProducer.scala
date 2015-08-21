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

package io.gearpump.experiments.storm.producer

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.topology.GearpumpStormComponent.GearpumpSpout
import io.gearpump.experiments.storm.util._
import io.gearpump.streaming.task.{StartTime, Task, TaskContext}

private[storm] class StormProducer(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {
  import StormUtil._

  private val gearpumpSpout = getGearpumpStormComponent(taskContext, conf).asInstanceOf[GearpumpSpout]

  override def onStart(startTime: StartTime): Unit = {
    gearpumpSpout.start(startTime)
    self ! Message("start")
  }

  override def onNext(msg: Message): Unit = {
    gearpumpSpout.next(msg)
    self ! Message("Continue")
  }
}
