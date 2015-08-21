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

package io.gearpump.experiments.storm.processor

import java.util.concurrent.TimeUnit

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.topology.GearpumpStormComponent.GearpumpBolt
import io.gearpump.experiments.storm.util._
import io.gearpump.streaming.task._
import io.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.concurrent.duration.Duration

object StormProcessor {
  private val LOG: Logger = LogUtil.getLogger(classOf[StormProcessor])
  private val TICK = Message("tick")
}

private[storm] class StormProcessor(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {
  import StormUtil._
  import io.gearpump.experiments.storm.processor.StormProcessor._

  private val gearpumpBolt = getGearpumpStormComponent(taskContext, conf).asInstanceOf[GearpumpBolt]
  private val freqOpt = gearpumpBolt.getTickFrequency

  override def onStart(startTime: StartTime): Unit = {
    gearpumpBolt.start(startTime)
    freqOpt.foreach(scheduleTick)
  }

  override def onNext(message: Message): Unit = {
    message.msg match {
      case "tick" =>
        freqOpt.foreach { freq =>
          scheduleTick(freq)
          gearpumpBolt.tick(freq)
        }
      case _ =>
        gearpumpBolt.next(message)
    }
  }

  def scheduleTick(freq: Long): Unit = {
    taskContext.scheduleOnce(Duration(freq, TimeUnit.SECONDS)){ self ! TICK }
  }
}
