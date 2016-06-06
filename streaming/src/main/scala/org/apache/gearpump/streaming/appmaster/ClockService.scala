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

package org.apache.gearpump.streaming.appmaster

import java.util
import java.util.Date
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Cancellable, Stash}
import org.apache.gearpump.google.common.primitives.Longs
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.cluster.ClientToMaster.GetStallingTasks
import org.apache.gearpump.streaming.AppMasterToMaster.StallingTasks
import org.apache.gearpump.streaming._
import org.apache.gearpump.streaming.appmaster.ClockService.HealthChecker.ClockValue
import org.apache.gearpump.streaming.appmaster.ClockService._
import org.apache.gearpump.streaming.storage.AppDataStore
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

/**
 * Maintains a global view of message timestamp in the application
 */
class ClockService(private var dag: DAG, store: AppDataStore) extends Actor with Stash {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  import context.dispatcher

  private val healthChecker = new HealthChecker(stallingThresholdSeconds = 60)
  private var healthCheckScheduler: Cancellable = null
  private var snapshotScheduler: Cancellable = null

  override def receive: Receive = null

  override def preStart(): Unit = {
    LOG.info("Initializing Clock service, get snapshotted StartClock ....")
    store.get(START_CLOCK).asInstanceOf[Future[TimeStamp]].map { clock =>
      val startClock = Option(clock).getOrElse(0L)

      minCheckpointClock = Some(startClock)

      // Recover the application by restarting from last persisted startClock.
      // Only messge after startClock will be replayed.
      self ! StoredStartClock(startClock)
      LOG.info(s"Start Clock Retrieved, starting ClockService, startClock: $startClock")
    }

    context.become(waitForStartClock)
  }

  override def postStop(): Unit = {
    Option(healthCheckScheduler).map(_.cancel)
    Option(snapshotScheduler).map(_.cancel)
  }

  // Keep track of clock value of all processors.
  private var clocks = Map.empty[ProcessorId, ProcessorClock]

  // Each process can have multiple upstream processors. This keep track of the upstream clocks.
  private var upstreamClocks = Map.empty[ProcessorId, Array[ProcessorClock]]

  // We use Array instead of List for Performance consideration
  private var processorClocks = Array.empty[ProcessorClock]

  private var checkpointClocks: Map[TaskId, Vector[TimeStamp]] = null

  private var minCheckpointClock: Option[TimeStamp] = None

  private def checkpointEnabled(processor: ProcessorDescription): Boolean = {
    val taskConf = processor.taskConf
    taskConf != null && taskConf.getBoolean("state.checkpoint.enable") == Some(true)
  }

  private def resetCheckpointClocks(dag: DAG, startClock: TimeStamp): Unit = {
    this.checkpointClocks = dag.processors.filter(startClock < _._2.life.death)
      .filter { case (_, processor) =>
        checkpointEnabled(processor)
      }.flatMap { case (id, processor) =>
      (0 until processor.parallelism).map(TaskId(id, _) -> Vector.empty[TimeStamp])
    }
    if (this.checkpointClocks.isEmpty) {
      minCheckpointClock = None
    }
  }

  private def initDag(startClock: TimeStamp): Unit = {
    recoverDag(this.dag, startClock)
  }

  private def recoverDag(dag: DAG, startClock: TimeStamp): Unit = {
    this.clocks = dag.processors.filter(startClock < _._2.life.death).
      map { pair =>
        val (processorId, processor) = pair
        val parallelism = processor.parallelism
        val clock = new ProcessorClock(processorId, processor.life, parallelism)
        clock.init(startClock)
        (processorId, clock)
      }

    this.upstreamClocks = clocks.map { pair =>
      val (processorId, processor) = pair

      val upstreams = dag.graph.incomingEdgesOf(processorId).map(_._1)
      val upstreamClocks = upstreams.flatMap(clocks.get(_))
      (processorId, upstreamClocks.toArray)
    }

    this.processorClocks = clocks.toArray.map(_._2)

    resetCheckpointClocks(dag, startClock)
  }

  private def dynamicDAG(dag: DAG, startClock: TimeStamp): Unit = {
    val newClocks = dag.processors.filter(startClock < _._2.life.death).
      map { pair =>
        val (processorId, processor) = pair
        val parallelism = processor.parallelism

        val clock = if (clocks.contains(processor.id)) {
          clocks(processorId).copy(life = processor.life)
        } else {
          new ProcessorClock(processorId, processor.life, parallelism)
        }
        (processorId, clock)
      }

    this.clocks = newClocks

    this.upstreamClocks = newClocks.map { pair =>
      val (processorId, processor) = pair

      val upstreams = dag.graph.incomingEdgesOf(processorId).map(_._1)
      val upstreamClocks = upstreams.flatMap(newClocks.get(_))
      (processorId, upstreamClocks.toArray)
    }

    // Inits the clock of all processors.
    newClocks.map { pair =>
      val (processorId, processorClock) = pair
      val upstreamClock = getUpStreamMinClock(processorId)
      val birth = processorClock.life.birth

      if (dag.graph.inDegreeOf(processorId) == 0) {
        processorClock.init(Longs.max(birth, startClock))
      } else {
        processorClock.init(upstreamClock)
      }
    }

    this.processorClocks = clocks.toArray.map(_._2)

    resetCheckpointClocks(dag, startClock)
  }

  def waitForStartClock: Receive = {
    case StoredStartClock(startClock) =>
      initDag(startClock)

      import context.dispatcher

      // Period report current clock
      healthCheckScheduler = context.system.scheduler.schedule(
        new FiniteDuration(5, TimeUnit.SECONDS),
        new FiniteDuration(60, TimeUnit.SECONDS), self, HealthCheck)

      // Period snpashot latest min startclock to external storage
      snapshotScheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
        new FiniteDuration(5, TimeUnit.SECONDS), self, SnapshotStartClock)

      unstashAll()
      context.become(clockService)

    case _ =>
      stash()
  }

  private def getUpStreamMinClock(processorId: ProcessorId): TimeStamp = {
    val clocks = upstreamClocks.get(processorId)
    if (clocks.isDefined) {
      if (clocks.get == null || clocks.get.length == 0) {
        Long.MaxValue
      } else {
        ProcessorClocks.minClock(clocks.get)
      }
    } else {
      Long.MaxValue
    }
  }

  def clockService: Receive = {
    case GetUpstreamMinClock(task) =>
      sender ! UpstreamMinClock(getUpStreamMinClock(task.processorId))

    case update@UpdateClock(task, clock) =>
      val upstreamMinClock = getUpStreamMinClock(task.processorId)

      val processorClock = clocks.get(task.processorId)
      if (processorClock.isDefined) {
        processorClock.get.updateMinClock(task.index, clock)
      } else {
        LOG.error(s"Cannot updateClock for task $task")
      }
      sender ! UpstreamMinClock(upstreamMinClock)

    case GetLatestMinClock =>
      sender ! LatestMinClock(minClock)

    case GetStartClock =>
      sender ! StartClock(getStartClock)

    case deathCheck: CheckProcessorDeath =>
      val processorId = deathCheck.processorId
      val processorClock = clocks.get(processorId)
      if (processorClock.isDefined) {
        val life = processorClock.get.life
        if (processorClock.get.min >= life.death) {

          LOG.info(s"Removing $processorId from clock service...")
          removeProcessor(processorId)
        } else {
          LOG.info(s"Unsuccessfully in removing $processorId from clock service...," +
            s" min: ${processorClock.get.min}, life: $life")
        }
      }
    case HealthCheck =>
      selfCheck()

    case SnapshotStartClock =>
      snapshotStartClock()

    case ReportCheckpointClock(task, time) =>
      updateCheckpointClocks(task, time)

    case GetCheckpointClock =>
      sender ! CheckpointClock(minCheckpointClock)

    case getStalling: GetStallingTasks =>
      sender ! StallingTasks(healthChecker.getReport.stallingTasks)

    case ChangeToNewDAG(dag) =>
      if (dag.version > this.dag.version) {
        // Transits to a new dag version
        this.dag = dag
        dynamicDAG(dag, getStartClock)
      } else {
        // Restarts current dag.
        recoverDag(dag, getStartClock)
      }
      LOG.info(s"Change to new DAG(dag = ${dag.version}), send back ChangeToNewDAGSuccess")
      sender ! ChangeToNewDAGSuccess(clocks.map { pair =>
        val (id, clock) = pair
        (id, clock.min)
      })
  }

  private def removeProcessor(processorId: ProcessorId): Unit = {
    clocks = clocks - processorId
    processorClocks = processorClocks.filter(_.processorId != processorId)

    upstreamClocks = upstreamClocks.map { pair =>
      val (id, upstreams) = pair
      val updatedUpstream = upstreams.filter(_.processorId != processorId)
      (id, updatedUpstream)
    }

    upstreamClocks = upstreamClocks - processorId

    // Removes dead processor from checkpoints.
    checkpointClocks = checkpointClocks.filter { kv =>
      val (taskId, processor) = kv
      taskId.processorId != processorId
    }
  }

  private def minClock: TimeStamp = {
    ProcessorClocks.minClock(processorClocks)
  }

  def selfCheck(): Unit = {
    val minTimestamp = minClock

    if (Long.MaxValue == minTimestamp) {
      processorClocks.foreach { clock =>
        LOG.info(s"Processor ${clock.processorId} Clock: min: ${clock.min}, " +
          s"taskClocks: " + clock.taskClocks.mkString(","))
      }
    }

    healthChecker.check(minTimestamp, clocks, dag, System.currentTimeMillis())
  }

  private def getStartClock: TimeStamp = {
    minCheckpointClock.getOrElse(minClock)
  }

  private def snapshotStartClock(): Unit = {
    store.put(START_CLOCK, getStartClock)
  }

  private def updateCheckpointClocks(task: TaskId, time: TimeStamp): Unit = {
    val clocks = checkpointClocks(task) :+ time
    checkpointClocks += task -> clocks

    if (checkpointClocks.forall(_._2.contains(time))) {
      minCheckpointClock = Some(time)
      LOG.info(s"minCheckpointTime $minCheckpointClock")

      checkpointClocks = checkpointClocks.mapValues(_.dropWhile(_ <= time))
    }
  }
}

object ClockService {
  val START_CLOCK = "startClock"

  case object HealthCheck

  class ProcessorClock(val processorId: ProcessorId, val life: LifeTime, val parallism: Int,
      private var _min: TimeStamp = 0L, private var _taskClocks: Array[TimeStamp] = null) {

    def copy(life: LifeTime): ProcessorClock = {
      new ProcessorClock(processorId, life, parallism, _min, _taskClocks)
    }

    def min: TimeStamp = _min
    def taskClocks: Array[TimeStamp] = _taskClocks

    def init(startClock: TimeStamp): Unit = {
      if (taskClocks == null) {
        this._min = startClock
        this._taskClocks = new Array(parallism)
        util.Arrays.fill(taskClocks, startClock)
      }
    }

    def updateMinClock(taskIndex: Int, clock: TimeStamp): Unit = {
      taskClocks(taskIndex) = clock
      _min = Longs.min(taskClocks: _*)
    }
  }

  case object SnapshotStartClock

  case class Report(stallingTasks: List[TaskId])

  /**
   * Check whether the clock is advancing normally
   */
  class HealthChecker(stallingThresholdSeconds: Int) {
    private val LOG: Logger = LogUtil.getLogger(getClass)

    private var minClock: ClockValue = null
    private val stallingThresholdMilliseconds = stallingThresholdSeconds * 1000
    // 60 seconds
    private var stallingTasks = Array.empty[TaskId]

    /** Check for stalling tasks */
    def check(
        currentMinClock: TimeStamp, processorClocks: Map[ProcessorId, ProcessorClock],
        dag: DAG, now: TimeStamp): Unit = {
      var isClockStalling = false
      if (null == minClock || currentMinClock > minClock.appClock) {
        minClock = ClockValue(systemClock = now, appClock = currentMinClock)
      } else {
        // Clock not advancing
        if (now > minClock.systemClock + stallingThresholdMilliseconds) {
          LOG.warn(s"Clock has not advanced for ${(now - minClock.systemClock) / 1000} seconds " +
            s"since ${minClock.prettyPrint}...")
          isClockStalling = true
        }
      }

      if (isClockStalling) {
        val processorId = dag.graph.topologicalOrderWithCirclesIterator.toList.find { processorId =>
          val clock = processorClocks.get(processorId)
          if (clock.isDefined) {
            clock.get.min == minClock.appClock
          } else {
            false
          }
        }

        processorId.foreach { processorId =>
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
    case class ClockValue(systemClock: TimeStamp, appClock: TimeStamp) {
      def prettyPrint: String = {
        "(system clock: " + new Date(systemClock).toString + ", app clock: " + appClock + ")"
      }
    }
  }

  object ProcessorClocks {

    // Get the Min clock of all processors
    def minClock(clock: Array[ProcessorClock]): TimeStamp = {
      var i = 0
      var min = if (clock.length == 0) 0L else clock(0).min
      while (i < clock.length) {
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