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

package org.apache.gearpump.streaming.task

import java.util
import java.util.Date
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Cancellable}
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.{DAG, TaskGroup}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.FiniteDuration

/**
 * The clockService will maintain a global view of message timestamp in the application
 */
class ClockService(dag : DAG) extends Actor {
import org.apache.gearpump.streaming.task.ClockService._

  private val taskGroupClocks = new util.TreeSet[TaskGroupClock]()
  private val taskGroupLookup = new util.HashMap[TaskGroup, TaskGroupClock]()
  private var scheduler : Cancellable = null

  private val LOG: Logger = LoggerFactory.getLogger(classOf[ClockService])

  override def receive = clockService

  override def preStart() : Unit = {
    dag.tasks.foreach { taskIdWithDescription =>
      val (taskGroupId, description) = taskIdWithDescription
      val taskClocks = new Array[TimeStamp](description.parallelism).map(_ => Long.MaxValue)
      val taskGroupClock = new TaskGroupClock(taskGroupId, Long.MaxValue, taskClocks)
      taskGroupClocks.add(taskGroupClock)
      taskGroupLookup.put(taskGroupId, taskGroupClock)
    }

    import context.dispatcher
    scheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportGlobalMinClock())
  }

  override def postStop() : Unit = {
    scheduler.cancel()
  }

  def clockService : Receive = {
    case UpdateClock(task, clock) =>
      val TaskId(taskGroupId, taskIndex) = task

      val taskGroup = taskGroupLookup.get(taskGroupId)
      taskGroupClocks.remove(taskGroup)
      taskGroup.taskClocks(taskIndex) = clock
      taskGroup.minClock = taskGroup.taskClocks.min
      taskGroupClocks.add(taskGroup)
      sender ! ClockUpdated(minClock)
    case GetLatestMinClock =>
      sender ! LatestMinClock(minClock)
  }

  private def minClock : TimeStamp = {
    val taskGroup = taskGroupClocks.first()
    taskGroup.minClock
  }

  def reportGlobalMinClock() : Unit = {
    val minTimeStamp = new Date(minClock)
    LOG.info(s"Application minClock tracking: $minTimeStamp")
  }
}

object ClockService {

  class TaskGroupClock(val taskGroup : TaskGroup, var minClock : TimeStamp = Long.MaxValue,
                       var taskClocks : Array[TimeStamp] = null) extends Comparable[TaskGroupClock] {
    override def equals(obj: Any): Boolean = {
      this.eq(obj.asInstanceOf[AnyRef])
    }

    override def compareTo(o: TaskGroupClock): Int = {
      val delta = minClock - o.minClock
      if (delta > 0) {
        1
      } else if (delta < 0) {
        -1
      } else {
        taskGroup - o.taskGroup
      }
    }
  }
}