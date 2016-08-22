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

package org.apache.gearpump.experiments.storm.producer

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.Actor.Receive
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.storm.topology.GearpumpStormComponent.GearpumpSpout
import org.apache.gearpump.experiments.storm.util._
import org.apache.gearpump.streaming.source.Watermark
import org.apache.gearpump.streaming.task._

import scala.concurrent.duration.Duration

object StormProducer {
  private[storm] val TIMEOUT = Message("timeout")
}

/**
 * this is runtime container for Storm spout
 */
private[storm] class StormProducer(gearpumpSpout: GearpumpSpout,
    taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {
  import org.apache.gearpump.experiments.storm.producer.StormProducer._

  def this(taskContext: TaskContext, conf: UserConfig) = {
    this(StormUtil.getGearpumpStormComponent(taskContext, conf)(taskContext.system)
      .asInstanceOf[GearpumpSpout], taskContext, conf)
  }

  private val timeoutMillis = gearpumpSpout.getMessageTimeout

  override def onStart(startTime: Instant): Unit = {
    gearpumpSpout.start(startTime)
    if (gearpumpSpout.ackEnabled) {
      getCheckpointClock
    }
    timeoutMillis.foreach(scheduleTimeout)
    self ! Watermark(Instant.now)
  }

  override def onNext(msg: Message): Unit = {
    msg match {
      case TIMEOUT =>
        timeoutMillis.foreach { timeout =>
          gearpumpSpout.timeout(timeout)
          scheduleTimeout(timeout)
        }
      case _ =>
        gearpumpSpout.next(msg)
    }
    self ! Watermark(Instant.now)
  }

  override def receiveUnManagedMessage: Receive = {
    case CheckpointClock(optClock) =>
      optClock.foreach { clock =>
        gearpumpSpout.checkpoint(clock)
      }
      getCheckpointClock()
  }

  def getCheckpointClock(): Unit = {
    taskContext.scheduleOnce(Duration(StormConstants.CHECKPOINT_INTERVAL_MILLIS,
      TimeUnit.MILLISECONDS))(taskContext.appMaster ! GetCheckpointClock)
  }

  private def scheduleTimeout(timeout: Long): Unit = {
    taskContext.scheduleOnce(Duration(timeout, TimeUnit.MILLISECONDS)) {
      self ! TIMEOUT
    }
  }
}
