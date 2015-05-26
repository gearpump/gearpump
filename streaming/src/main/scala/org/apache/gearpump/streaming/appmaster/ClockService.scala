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

package org.apache.gearpump.streaming.appmaster

import java.util
import java.util.Date
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Cancellable, Stash}
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.cluster.ClientToMaster.GetStallingTasks
import org.apache.gearpump.streaming.AppMasterToExecutor.StartClock
import org.apache.gearpump.streaming.AppMasterToMaster.StallingTasks
import org.apache.gearpump.streaming.appmaster.ClockService.ProcessorClock.ClockValue
import org.apache.gearpump.streaming.appmaster.ClockService._
import org.apache.gearpump.streaming.storage.AppDataStore
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.streaming.{DAG, ProcessorId}
import org.apache.gearpump.util.{Graph, LogUtil}
import org.slf4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConversions._

/**
 * The clockService will maintain a global view of message timestamp in the application
 */
class ClockService(dag : DAG, store: AppDataStore) extends Actor with Stash {


  private val LOG: Logger = LogUtil.getLogger(getClass)

  import context.dispatcher

  private var startClock: Long = 0
  
  private val processorClockLookup = new util.HashMap[ProcessorId, ProcessorClock]()

  private var healthCheckScheduler : Cancellable = null
  private var snapshotScheduler : Cancellable = null
  private var stallingTasks: Option[List[StallingTask]] = None

  private var processorIdToLevel = Map.empty[ProcessorId, Int]
  private var levelMinClock = Array.empty[TimeStamp]
  private var sortedProcessorClocks: List[ProcessorClock] = null
  private val healthChecker = new HealthChecker

  override def receive = null

  override def preStart() : Unit = {
    LOG.info("Initializing Clock service, get snapshotted StartClock ....")

    store.get(START_CLOCK).asInstanceOf[Future[TimeStamp]].map { clock =>
      val startClock = Option(clock).getOrElse(0L)
      self ! StartClock(startClock)
      LOG.info(s"Start Clock Retrived, starting ClockService, startClock: $startClock")
    }

    context.become(waitForStartClock)
  }

  override def postStop() : Unit = {
    Option(healthCheckScheduler).map(_.cancel)
    Option(snapshotScheduler).map(_.cancel)
  }

  private def initializeDagWithStartClock(startClock: TimeStamp): Unit = {
    this.startClock = startClock
    dag.processors.foreach {
      processorIdWithDescription =>
        val (processorId, description) = processorIdWithDescription
        val taskClocks = new Array[TimeStamp](description.parallelism).map(_ => startClock)
        val processorClock = new ProcessorClock(processorId, taskClocks)
        processorClockLookup.put(processorId, processorClock)
    }

    this.processorIdToLevel = dag.graph.topologicalOrderIterator.zipWithIndex.toMap
    this.levelMinClock = Array.fill(processorIdToLevel.size)(startClock)

    this.sortedProcessorClocks = sortProcessorClockByLevel
  }

  def waitForStartClock: Receive = {
    case StartClock(startClock) =>

      initializeDagWithStartClock(startClock)

      import context.dispatcher

      //period report current clock
      healthCheckScheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
        new FiniteDuration(5, TimeUnit.SECONDS), self, HealthCheck)

      //period snpashot latest min startclock to external storage
      snapshotScheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
        new FiniteDuration(5, TimeUnit.SECONDS), self, SnapshotStartClock)

      unstashAll()
      context.become(clockService)
    case _ =>
      stash()
  }

  def clockService: Receive = {

    case GetUpstreamMinClock(task) =>
      val TaskId(processorId, taskIndex) = task
      val level = processorIdToLevel(processorId)
      val upstreamClock = UpstreamMinClock(minClockOfLevel(level - 1))
      sender ! upstreamClock

    case update@ UpdateClock(task, clock) =>

      val TaskId(processorId, taskIndex) = task

      val processor = processorClockLookup.get(processorId)
      processor.taskClocks(taskIndex) = clock

      val level = processorIdToLevel(processorId)
      levelMinClock(level) = processor.taskClocks.min

      val upstream = UpstreamMinClock(minClockOfLevel(level - 1))
      sender ! upstream

    case GetLatestMinClock =>
      sender ! LatestMinClock(minClock)

    case HealthCheck =>
      selfChecker()
    case SnapshotStartClock =>
      snapshotStartClock()
    case getStalling: GetStallingTasks =>
      val tasks = stallingTasks.getOrElse(List.empty)
        .map(stallingTask => TaskId(stallingTask.processorId, stallingTask.taskId))
      sender ! StallingTasks(tasks)
  }

  private def minClockOfLevel(level: Int): TimeStamp = {
    if (level >= 0) {
      levelMinClock(level)
    }  else {
      Long.MaxValue
    }
  }

  private def minClock: TimeStamp = {
    if (levelMinClock.length > 0) {
      levelMinClock(levelMinClock.length - 1)
    } else {
      0
    }
  }

  def selfChecker() : Unit = {
    val latestMinClock = minClock
    val minTimeStamp = new Date(latestMinClock)
    LOG.info(s"Application minClock tracking: $minTimeStamp")
    this.stallingTasks = healthChecker.check(latestMinClock, sortedProcessorClocks)
  }

  private def snapshotStartClock() : Unit = {
    store.put(START_CLOCK, minClock)
  }

  private def sortProcessorClockByLevel: List[ProcessorClock] = {
    val sortedProcessorIds = processorClockLookup.keySet().toList.sortWith{(left, right) =>
      processorIdToLevel(left) < processorIdToLevel(right)
    }
    sortedProcessorIds.map(processorClockLookup.get(_))
  }
}

object ClockService {
  val START_CLOCK = "startClock"

  case object HealthCheck
  case object SnapshotStartClock

  case class StallingTask(processorId: ProcessorId, taskId: Int, taskClock: TimeStamp)

  class ProcessorClock(var processorId: ProcessorId, var taskClocks : Array[TimeStamp] = null)

  class HealthChecker {

    private val LOG: Logger = LogUtil.getLogger(getClass)
    private var minClock = ClockValue(0L, 0L)
    private val SELF_CHECK_INTERVAL_MILLIS = 60 * 1000 // 60 seconds


    def check(currentMinClock: TimeStamp, processors: List[ProcessorClock]): Option[List[StallingTask]] = {
      val now = System.currentTimeMillis()
      var isClockStalling = false
      if (currentMinClock > minClock.appClock) {
        minClock = ClockValue(systemClock = now, appClock = currentMinClock)
      } else {
        //clock not advanding
        if (now > minClock.systemClock + SELF_CHECK_INTERVAL_MILLIS) {
          LOG.warn(s"Clock has not advanced for ${SELF_CHECK_INTERVAL_MILLIS/1000} seconds..")
          isClockStalling = true
        }
      }

      if (isClockStalling) {
        val processor = processors.find {processor =>
          processor.taskClocks.min == minClock.appClock
        }
        val stallingTasks = processor.map {processor =>
          import processor.{processorId, taskClocks}
          (0 until taskClocks.length).flatMap {taskIndex =>
            val taskClock = taskClocks(taskIndex)
            if (taskClock == minClock.appClock) {
              Some(StallingTask(processorId, taskIndex, taskClock))
            } else {
              None
            }
          }.toList
        }
        stallingTasks
      } else {
        None
      }
    }
  }

  object ProcessorClock {
    case class ClockValue(systemClock: TimeStamp, appClock: TimeStamp)
  }
}