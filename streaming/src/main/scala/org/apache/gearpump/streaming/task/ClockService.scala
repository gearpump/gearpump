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
 * The clockservice will maintain a global view of message timestamp in the application
 */
class ClockService(dag : DAG) extends Actor {
import org.apache.gearpump.streaming.task.ClockService._

  private val taskgroupClocks = new util.TreeSet[TaskGroupClock]()
  private val taskgroupLookup = new util.HashMap[TaskGroup, TaskGroupClock]()
  private var scheduler : Cancellable = null

  private val LOG: Logger = LoggerFactory.getLogger(classOf[ClockService])

  override def receive = clockService

  override def preStart : Unit = {
    dag.tasks.foreach { taskIdWithDescription =>
      val (taskGroupId, description) = taskIdWithDescription
      val taskClocks = new Array[TimeStamp](description.parallism)
      val taskgroupClock = new TaskGroupClock(taskGroupId, 0L, taskClocks)
      taskgroupClocks.add(taskgroupClock)
      taskgroupLookup.put(taskGroupId, taskgroupClock)
    }

    import context.dispatcher
    scheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportGlobalMinClock)
  }

  override def postStop() : Unit = {
    scheduler.cancel()
  }

  def clockService : Receive = {
    case UpdateClock(task, clock) =>
      val TaskId(taskgroupId, taskIndex) = task

      val taskgroup = taskgroupLookup.get(taskgroupId)
      taskgroupClocks.remove(taskgroup)
      taskgroup.taskClocks(taskIndex) = clock
      taskgroup.minClock = taskgroup.taskClocks.min
      taskgroupClocks.add(taskgroup)
      sender ! ClockUpdated(minClock)
    case GetLatestMinClock =>
      sender ! LatestMinClock(minClock)
  }

  private def minClock : TimeStamp = {
    val taskgroup = taskgroupClocks.first()
    taskgroup.minClock
  }

  def reportGlobalMinClock : Unit = {
    val minTimeStamp = new Date(minClock)
    LOG.info(s"Application minClock tracking: ${minTimeStamp}")
  }
}

object ClockService {

  class TaskGroupClock(val taskgroup : TaskGroup, var minClock : TimeStamp = 0L, var taskClocks : Array[TimeStamp] = null) extends Comparable[TaskGroupClock] {
    override def equals(obj: Any): Boolean = {
      if (this.eq(obj.asInstanceOf[AnyRef])) {
        return true
      } else {
        return false
      }
    }

    override def compareTo(o: TaskGroupClock): Int = {
      val delta = minClock - o.minClock
      if (delta > 0) {
        1
      } else if (delta < 0) {
        -1
      } else {
        taskgroup - o.taskgroup
      }
    }
  }
}