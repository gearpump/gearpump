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
import org.apache.gearpump.streaming.AppMasterToExecutor.StartClock
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

  private var reportScheduler : Cancellable = null
  private var snapshotScheduler : Cancellable = null

  var processorIdToLevel = Map.empty[ProcessorId, Int]
  var levelMinClock = Array.empty[TimeStamp]
  
  

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
    Option(reportScheduler).map(_.cancel)
    Option(snapshotScheduler).map(_.cancel)
  }

  private def initializeDagWithStartClock(startClock: TimeStamp) = {
    this.startClock = startClock
    dag.processors.foreach {
      processorIdWithDescription =>
        val (processorId, description) = processorIdWithDescription
        val taskClocks = new Array[TimeStamp](description.parallelism).map(_ => startClock)
        val processorClock = new ProcessorClock(taskClocks)
        processorClockLookup.put(processorId, processorClock)
    }

    this.processorIdToLevel = dag.graph.topologicalOrderIterator.zipWithIndex.toMap
    this.levelMinClock = Array.fill(processorIdToLevel.size)(startClock)
  }

  def waitForStartClock: Receive = {
    case StartClock(startClock) =>

      initializeDagWithStartClock(startClock)

      import context.dispatcher

      //period report current clock
      reportScheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
        new FiniteDuration(5, TimeUnit.SECONDS), self, ReportGlobalClock)

      //period snpashot latest min startclock to external storage
      snapshotScheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
        new FiniteDuration(5, TimeUnit.SECONDS), self, SnapshotStartClock)

      unstashAll()
      context.become(clockService)
    case _ =>
      stash()
  }

  def clockService: Receive = {
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

    case ReportGlobalClock =>
      selfChecker()
    case SnapshotStartClock =>
      snapshotStartClock()
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

  private var selfCheckClock: SelfCheckClock = new SelfCheckClock(System.currentTimeMillis(), 0)
  def selfChecker() : Unit = {
    val latestMinClock = minClock
    val minTimeStamp = new Date(latestMinClock)
    LOG.info(s"Application minClock tracking: $minTimeStamp")

    val now = System.currentTimeMillis()
    if (latestMinClock > selfCheckClock.minClock) {
      selfCheckClock.minClock = latestMinClock
      selfCheckClock.checkTime = now
    } else if (now > selfCheckClock.checkTime + SELF_CHECK_INTERVAL_SECONDS) {
      LOG.warn(s"Clock has not advanced for $SELF_CHECK_INTERVAL_SECONDS seconds..")
      selfCheckClock.checkTime = now
      //do diagnosis
      var stallingLevel = Long.MaxValue
      for (i <- 0 until levelMinClock.length) {
        if (i < stallingLevel) {
          val levelClock = levelMinClock(i)
          if (levelClock == latestMinClock) {
            stallingLevel = i
          }
        }
      }

      //find processor id by stallinglevel
      processorIdToLevel.find(_._2 == stallingLevel).map(_._1)
        .flatMap { processorId =>
        //find processor task clocks
        val processorClock = Option(processorClockLookup.get(processorId))
        val taskClocks = processorClock.flatMap { processorClock => Option(processorClock.taskClocks) }
        taskClocks.map { taskClocks =>
          taskClocks.zipWithIndex.map { taskClockAndTaskIndex =>
            val (taskClock, taskIndex) = taskClockAndTaskIndex
            StallingTask(processorId, taskIndex, taskClock)
          }.filter {_.taskClock == latestMinClock}
        }
      }.map{stallingClocks =>
          LOG.warn("Stalling processor clocks: " + stallingClocks.mkString(","))
      }
    }
  }

  private def snapshotStartClock() : Unit = {
    store.put(START_CLOCK, minClock)
  }
}

object ClockService {
  val START_CLOCK = "startClock"

  case object ReportGlobalClock
  case object SnapshotStartClock

  case class StallingTask(processorId: ProcessorId, taskId: Int, taskClock: TimeStamp)

  class ProcessorClock(var taskClocks : Array[TimeStamp] = null)

  val SELF_CHECK_INTERVAL_SECONDS = 60 //seconds
  class SelfCheckClock(var checkTime: TimeStamp, var minClock: TimeStamp)
}