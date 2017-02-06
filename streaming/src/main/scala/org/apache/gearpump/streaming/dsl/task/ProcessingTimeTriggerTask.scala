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
package org.apache.gearpump.streaming.dsl.task

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.Actor.Receive
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.Constants._
import org.apache.gearpump.streaming.dsl.task.ProcessingTimeTriggerTask.Triggering
import org.apache.gearpump.streaming.dsl.window.api.SlidingWindowFunction
import org.apache.gearpump.streaming.dsl.window.impl.{DefaultWindowRunner, GroupAlsoByWindow, WindowRunner}
import org.apache.gearpump.streaming.task.{Task, TaskContext}

import scala.concurrent.duration.FiniteDuration

object ProcessingTimeTriggerTask {
  case object Triggering
}

/**
 * This task triggers output on scheduled system time interval.
 */
class ProcessingTimeTriggerTask[IN, GROUP](
    groupBy: GroupAlsoByWindow[IN, GROUP],
    windowRunner: WindowRunner,
    taskContext: TaskContext,
    userConfig: UserConfig)
  extends Task(taskContext, userConfig) {

  def this(groupBy: GroupAlsoByWindow[IN, GROUP],
      taskContext: TaskContext, userConfig: UserConfig) = {
    this(groupBy, new DefaultWindowRunner(taskContext, userConfig, groupBy)(taskContext.system),
      taskContext, userConfig)
  }

  def this(taskContext: TaskContext, userConfig: UserConfig) = {
    this(userConfig.getValue[GroupAlsoByWindow[IN, GROUP]](
      GEARPUMP_STREAMING_GROUPBY_FUNCTION)(taskContext.system).get,
      taskContext, userConfig)
  }

  private val windowFn = groupBy.window.windowFn.asInstanceOf[SlidingWindowFunction[IN]]
  private val windowSizeMs = windowFn.size.toMillis
  private val windowStepMs = windowFn.step.toMillis

  override def onStart(startTime: Instant): Unit = {
    val initialDelay = windowSizeMs - Instant.now.toEpochMilli % windowSizeMs
    taskContext.scheduleOnce(
      new FiniteDuration(initialDelay, TimeUnit.MILLISECONDS))(self ! Triggering)
  }

  override def onNext(message: Message): Unit = {
    windowRunner.process(message)
  }

  override def receiveUnManagedMessage: Receive = {
    case Triggering =>
      windowRunner.trigger(Instant.now)
      taskContext.scheduleOnce(
        new FiniteDuration(windowStepMs, TimeUnit.MILLISECONDS))(self ! Triggering)
  }

}
