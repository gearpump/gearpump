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
import com.google.common.primitives.Longs
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.cluster.ClientToMaster.GetStallingTasks
import org.apache.gearpump.streaming.AppMasterToExecutor.Start
import org.apache.gearpump.streaming.AppMasterToMaster.StallingTasks
import org.apache.gearpump.streaming.appmaster.ClockService.HealthChecker.ClockValue
import org.apache.gearpump.streaming.appmaster.ClockService._
import org.apache.gearpump.streaming.storage.AppDataStore
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.streaming.{DAG, ProcessorId}
import org.apache.gearpump.util.{Graph, LogUtil}
import org.slf4j.Logger
import org.apache.gearpump.partitioner.PartitionerDescription

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * The clockService will maintain a global view of message timestamp in the application
 */
class ClockService(dag : DAG, store: AppDataStore) extends Actor with Stash {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  import context.dispatcher

  private val healthChecker = new HealthChecker(stallingThresholdSeconds = 60)
  private var healthCheckScheduler : Cancellable = null
  private var snapshotScheduler : Cancellable = null

  override def receive = null

  override def preStart() : Unit = {
    LOG.info("Initializing Clock service, get snapshotted StartClock ....")
    store.get(START_CLOCK).asInstanceOf[Future[TimeStamp]].map { clock =>
      val startClock = Option(clock).getOrElse(0L)
      self ! StoredStartClock(startClock)
      LOG.info(s"Start Clock Retrieved, starting ClockService, startClock: $startClock")
    }

    context.become(waitForStartClock)
  }

  override def postStop() : Unit = {
    Option(healthCheckScheduler).map(_.cancel)
    Option(snapshotScheduler).map(_.cancel)
  }

  var clocks = Map.empty[ProcessorId, ProcessorClock]
  private var upstreamClocks = Map.empty[ProcessorId, Array[ProcessorClock]]

  // We use Array instead of List for Performance consideration
  private var processorClocks = Array.empty[ProcessorClock]

  private def setDAG(dag: DAG, startClock: TimeStamp): Unit = {
    val newClocks = dag.processors.map { pair =>
      val (processorId, processor) = pair
      val parallelism = processor.parallelism
      val newProcessorClock = new ProcessorClock(processorId, parallelism)
      (processorId, clocks.getOrElse(processor.id, newProcessorClock))
    }

    this.clocks = newClocks

    this.upstreamClocks = dag.graph.vertices.map { vertex =>
      val upstreamClocks = for (edge <- dag.graph.incomingEdgesOf(vertex)) yield {
        clocks(edge._1)
      }
      (vertex, upstreamClocks)
    }.toMap

    // init the clock of all processors.
    import scala.collection.JavaConversions._
    dag.graph.topologicalOrderIterator.foreach { processorId =>
      val processorClock = clocks(processorId)
      val upstreamClock = getUpStreamMinClock(processorId)
      val birth = dag.processors(processorId).life.birth

      if (dag.graph.inDegreeOf(processorId) == 0) {
        processorClock.init(Longs.max(birth, startClock))
      } else {
        processorClock.init(upstreamClock)
      }
    }

    this.processorClocks = clocks.toArray.map(_._2)
  }

  def waitForStartClock: Receive = {
    case StoredStartClock(startClock) =>
      setDAG(dag, startClock)

      import context.dispatcher

      //period report current clock
      healthCheckScheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
        new FiniteDuration(60, TimeUnit.SECONDS), self, HealthCheck)

      //period snpashot latest min startclock to external storage
      snapshotScheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
        new FiniteDuration(5, TimeUnit.SECONDS), self, SnapshotStartClock)

      unstashAll()
      context.become(clockService)

    case _ =>
      stash()
  }

  private def getUpStreamMinClock(processorId: ProcessorId): TimeStamp = {
    if (!upstreamClocks.contains(processorId)) {
      LOG.info(s"We don't have upstream clocks for processor: $processorId")
    }
    val clockArray = upstreamClocks(processorId)
    if (clockArray == null || clockArray.length == 0) {
      Long.MaxValue
    } else {
      ProcessorClocks.minClock(clockArray)
    }
  }

  def clockService: Receive = {
    case GetUpstreamMinClock(task) =>
      sender ! UpstreamMinClock(getUpStreamMinClock(task.processorId))

    case update@ UpdateClock(task, clock) =>
      val upstreamMinClock = getUpStreamMinClock(task.processorId)
      clocks(task.processorId).updateMinClock(task.index, clock)
      sender ! UpstreamMinClock(upstreamMinClock)

    case GetLatestMinClock =>
      sender ! LatestMinClock(minClock)

    case HealthCheck =>
      selfCheck()

    case SnapshotStartClock =>
      snapshotStartClock()

    case getStalling: GetStallingTasks =>
      sender ! StallingTasks(healthChecker.getReport.stallingTasks)
    case ChangeToNewDAG(dag) =>
      setDAG(dag, minClock)
      LOG.info(s"Change to new DAG(dag = ${dag.version}), send back ChangeToNewDAGSuccess")
      sender ! ChangeToNewDAGSuccess(clocks.map{ pair =>
        val (id, clock) = pair
        (id, clock.min)
      })
  }

  private def minClock: TimeStamp = {
    ProcessorClocks.minClock(processorClocks)
  }

  def selfCheck() : Unit = {
    val minTimestamp = minClock

    if (Long.MaxValue == minTimestamp) {
      processorClocks.foreach { clock =>
        LOG.info(s"Processor ${clock.processorId} Clock: min: ${clock.min}, taskClocks: "+ clock.taskClocks.mkString(","))
      }
    }

    healthChecker.check(minTimestamp, clocks, dag)
  }

  private def snapshotStartClock() : Unit = {
    store.put(START_CLOCK, minClock)
  }
}

object ClockService {
  val START_CLOCK = "startClock"

  case object HealthCheck
  class ProcessorClock(val processorId: ProcessorId, val parallism: Int) {

    var min: TimeStamp = 0L
    var taskClocks : Array[TimeStamp] = null

    def init(startClock: TimeStamp): Unit = {
      if (taskClocks == null) {
        this.min = startClock
        this.taskClocks = new Array(parallism)
        util.Arrays.fill(taskClocks, startClock)
      }
    }

    def updateMinClock(taskIndex: Int, clock: TimeStamp): Unit = {
      taskClocks(taskIndex) = clock
      min = Longs.min(taskClocks: _*)
    }
  }

  case object SnapshotStartClock

  case class Report(stallingTasks: List[TaskId])

  class HealthChecker(stallingThresholdSeconds: Int) {
    private val LOG: Logger = LogUtil.getLogger(getClass)

    private var minClock = ClockValue(0L, 0L)
    private val stallingThresholdMilliseconds = stallingThresholdSeconds * 1000 // 60 seconds
    private var stallingTasks = Array.empty[TaskId]

    def check(currentMinClock: TimeStamp, processorClocks: Map[ProcessorId, ProcessorClock], dag: DAG): Unit = {
      val now = System.currentTimeMillis()
      var isClockStalling = false
      if (currentMinClock > minClock.appClock) {
        minClock = ClockValue(systemClock = now, appClock = currentMinClock)
      } else {
        //clock not advancing
        if (now > minClock.systemClock + stallingThresholdMilliseconds) {
          LOG.warn(s"Clock has not advanced for ${(now - minClock.systemClock)/1000} seconds since ${minClock}...")
          isClockStalling = true
        }
      }

      if (isClockStalling) {
        import scala.collection.JavaConversions._
        val processorId = dag.graph.topologicalOrderIterator.toList.find { processorId =>
          processorClocks(processorId).min == minClock.appClock
        }

        processorId.foreach {processorId =>
          val processorClock = processorClocks(processorId)
          val taskClocks = processorClock.taskClocks
          stallingTasks = taskClocks.zipWithIndex.filter(_._1 == minClock.appClock).
            map(pair => TaskId(processorId, pair._2))
        }
        LOG.info(s"Stalling Tasks: ${stallingTasks.mkString(",")}")
      } else {
        stallingTasks = Array.empty[TaskId]
      }
    }

    def getReport: Report = {
      Report(stallingTasks.toList)
    }
  }

  object HealthChecker {
    case class ClockValue(systemClock: TimeStamp, appClock: TimeStamp)
  }

  object ProcessorClocks {
    def minClock(clock: Array[ProcessorClock]): TimeStamp = {
      var i = 0
      var min = if (clock.length == 0) 0L else clock(0).min
      while(i < clock.length) {
        min = Math.min(min, clock(i).min)
        i += 1
      }
      min
    }
  }

  case class ChangeToNewDAG(dag: DAG)
  case class ChangeToNewDAGSuccess(clocks: Map[ProcessorId, TimeStamp])

  case class StoredStartClock(clock: TimeStamp)
}