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

package io.gearpump.experiments.storm.processor

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.topology.GearpumpStormComponent.GearpumpBolt
import io.gearpump.experiments.storm.util._
import io.gearpump.streaming.task._

object StormProcessor {
  private[storm] val TICK = Message("tick")
}

/**
 * this is runtime container for Storm bolt
 */
private[storm] class StormProcessor(gearpumpBolt: GearpumpBolt,
    taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {
  import io.gearpump.experiments.storm.processor.StormProcessor._

  def this(taskContext: TaskContext, conf: UserConfig) = {
    this(StormUtil.getGearpumpStormComponent(taskContext, conf)(taskContext.system)
      .asInstanceOf[GearpumpBolt], taskContext, conf)
  }

  private val freqOpt = gearpumpBolt.getTickFrequency

  override def onStart(startTime: StartTime): Unit = {
    gearpumpBolt.start(startTime)
    freqOpt.foreach(scheduleTick)
  }

  override def onNext(message: Message): Unit = {
    message match {
      case TICK =>
        freqOpt.foreach { freq =>
          gearpumpBolt.tick(freq)
          scheduleTick(freq)
        }
      case _ =>
        gearpumpBolt.next(message)
    }
  }

  private def scheduleTick(freq: Int): Unit = {
    taskContext.scheduleOnce(Duration(freq, TimeUnit.SECONDS)) {
      self ! TICK
    }
  }
}
