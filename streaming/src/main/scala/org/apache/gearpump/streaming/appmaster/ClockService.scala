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

import java.time.Instant
import java.util
import java.util.Date
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Cancellable, Stash}
import com.google.common.primitives.Longs
import org.apache.gearpump.Time
import org.apache.gearpump.Time.MilliSeconds
import org.apache.gearpump.cluster.ClientToMaster.GetStallingTasks
import org.apache.gearpump.streaming.AppMasterToMaster.StallingTasks
import org.apache.gearpump.streaming._
import org.apache.gearpump.streaming.appmaster.ClockService.HealthChecker.ClockValue
import org.apache.gearpump.streaming.appmaster.ClockService._
import org.apache.gearpump.streaming.source.Watermark
import org.apache.gearpump.streaming.storage.AppDataStore
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.concurrent.duration.FiniteDuration

/**
 * Maintains a global view of message timestamp in the application
 */
class ClockService(
    private var dag: DAG,
    appMaster: ActorRef,
    store: AppDataStore) extends Actor with Stash {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  import context.dispatcher

  private val healthChecker = new HealthChecker(stallingThresholdSeconds = 60)
  private var healthCheckScheduler: Cancellable = _
  private var snapshotScheduler: Cancellable = _

  override def receive: Receive = null

  override def preStart(): Unit = {
    LOG.info("Initializing Clock service, get snapshotted StartClock ....")
    store.get(START_CLOCK).map { clock =>
      // check for null first since
      // (null).asInstanceOf[MilliSeconds] is zero
      val startClock = if (clock != null) clock.asInstanceOf[MilliSeconds] else Time.MIN_TIME_MILLIS

      minCheckpointClock = Some(startClock)

      // Recover the application by restarting from last persisted startClock.
      // Only message after startClock will be replayed.
      self ! StoredStartClock(startClock)
      LOG.info(s"Start Clock Retrieved, starting ClockService, startClock: $startClock")
    }

    context.become(waitForStartClock)
  }

  override def postStop(): Unit = {
    Option(healthCheckScheduler).foreach(_.cancel())
    Option(snapshotScheduler).foreach(_.cancel())
  }

  // Keep track of clock value of all processors.
  private var clocks = Map.empty[ProcessorId, ProcessorClock]

  // Each process can have multiple upstream processors. This keep track of the upstream clocks.
  private var upstreamClocks = Map.empty[ProcessorId, Array[ProcessorClock]]

  // We use Array instead of List for Performance consideration
  private var processorClocks = Array.empty[ProcessorClock]

  private var checkpointClocks: Map[TaskId, Vector[MilliSeconds]] = _

  private var minCheckpointClock: Option[MilliSeconds] = None

  private def checkpointEnabled(processor: ProcessorDescription): Boolean = {
    val taskConf = processor.taskConf
    taskConf != null && taskConf.getBoolean("state.checkpoint.enable").contains(true)
  }

  private def resetCheckpointClocks(dag: DAG, startClock: MilliSeconds): Unit = {
    this.checkpointClocks = dag.processors.filter(startClock < _._2.life.death)
      .filter { case (_, processor) =>
        checkpointEnabled(processor)
      }.flatMap { case (id, processor) =>
      (0 until processor.parallelism).map(TaskId(id, _) -> Vector.empty[MilliSeconds])
    }
    if (this.checkpointClocks.isEmpty) {
      minCheckpointClock = None
    }
  }

  private def initDag(startClock: MilliSeconds): Unit = {
    recoverDag(this.dag, startClock)
  }

  private def recoverDag(dag: DAG, startClock: MilliSeconds): Unit = {
    this.clocks = dag.processors.filter(startClock < _._2.life.death).
      map { pair =>
        val (processorId, processor) = pair
        val parallelism = processor.parallelism
        val clock = new ProcessorClock(processorId, processor.life, parallelism)
        clock.init(startClock)
        (processorId, clock)
      }

    this.upstreamClocks = getUpstreamClocks(clocks)

    this.processorClocks = clocks.toArray.map(_._2)

    resetCheckpointClocks(dag, startClock)
  }

  private def dynamicDAG(dag: DAG, startClock: MilliSeconds): Unit = {
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

    this.upstreamClocks = getUpstreamClocks(clocks)

    // Inits the clock of all processors.
    clocks.foreach { pair =>
      val (processorId, processorClock) = pair
      val upstreamClock = getUpStreamMinClock(processorId)
      val birth = processorClock.life.birth

      upstreamClock match {
        case Some(clock) =>
          processorClock.init(clock)
        case None =>
          processorClock.init(Longs.max(birth, startClock))
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

  private def getUpstreamClocks(
      clocks: Map[ProcessorId, ProcessorClock]): Map[ProcessorId, Array[ProcessorClock]] = {
    clocks.foldLeft(Map.empty[ProcessorId, Array[ProcessorClock]]) {
      case (accum, (processorId, _)) =>
        val upstreams = dag.graph.incomingEdgesOf(processorId).map(_._1)
        if (upstreams.nonEmpty) {
          val upstreamClocks = upstreams.collect(clocks)
          if (upstreamClocks.nonEmpty) {
            accum + (processorId -> upstreamClocks.toArray)
          } else {
            accum
          }
        } else {
          accum
        }
    }
  }

  private def getUpStreamMinClock(processorId: ProcessorId): Option[MilliSeconds] = {
    upstreamClocks.get(processorId).map(ProcessorClocks.minClock)
  }

  def clockService: Receive = {
    case GetUpstreamMinClock(task) =>
      sendBackUpstreamMinClock(sender, task)

    case UpdateClock(task, clock) =>
      val processorClock = clocks.get(task.processorId)
      if (processorClock.isDefined) {
        processorClock.get.updateMinClock(task.index, clock)
      } else {
        LOG.error(s"Cannot updateClock for task $task")
      }
      if (Instant.ofEpochMilli(minClock).equals(Watermark.MAX)) {
        appMaster ! EndingClock
      } else {
        sendBackUpstreamMinClock(sender, task)
      }

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

    case UpdateCheckpointClock(task, time) =>
      updateCheckpointClocks(task, time)

    case GetCheckpointClock =>
      sender ! CheckpointClock(minCheckpointClock)

    case GetStallingTasks =>
      sender ! StallingTasks(healthChecker.getReport.stallingTasks)

    case ChangeToNewDAG(newDag) =>
      if (newDag.version > this.dag.version) {
        // Transits to a new dag version
        this.dag = newDag
        dynamicDAG(dag, getStartClock)
      } else {
        // Restarts current dag.
        recoverDag(newDag, getStartClock)
      }
      LOG.info(s"Change to new DAG(dag = ${newDag.version}), send back ChangeToNewDAGSuccess")
      sender ! ChangeToNewDAGSuccess(clocks.map { pair =>
        val (id, clock) = pair
        (id, clock.min)
      })
  }

  private def sendBackUpstreamMinClock(sender: ActorRef, task: TaskId): Unit = {
    sender ! UpstreamMinClock(getUpStreamMinClock(task.processorId))
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
    checkpointClocks = checkpointClocks.filter(_._1.processorId != processorId)
    checkpointClocks = checkpointClocks.filter { kv =>
      val (taskId, _) = kv
      taskId.processorId != processorId
    }
  }

  private def minClock: MilliSeconds = {
    ProcessorClocks.minClock(processorClocks)
  }

  def selfCheck(): Unit = {
    val minTimestamp = minClock

    healthChecker.check(minTimestamp, clocks, dag, System.currentTimeMillis())
  }

  private def getStartClock: MilliSeconds = {
    minCheckpointClock.getOrElse(minClock)
  }

  private def snapshotStartClock(): Unit = {
    store.put(START_CLOCK, getStartClock)
  }

  private def updateCheckpointClocks(task: TaskId, time: MilliSeconds): Unit = {
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

  class ProcessorClock(val processorId: ProcessorId, val life: LifeTime, val parallelism: Int,
      private var _min: MilliSeconds = Time.MIN_TIME_MILLIS,
      private var _taskClocks: Array[MilliSeconds] = null) {

    def copy(life: LifeTime): ProcessorClock = {
      new ProcessorClock(processorId, life, parallelism, _min, _taskClocks)
    }

    def min: MilliSeconds = _min
    def taskClocks: Array[MilliSeconds] = _taskClocks

    def init(startClock: MilliSeconds): Unit = {
      if (taskClocks == null) {
        this._min = startClock
        this._taskClocks = new Array(parallelism)
        util.Arrays.fill(taskClocks, startClock)
      }
    }

    def updateMinClock(taskIndex: Int, clock: MilliSeconds): Unit = {
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

    private var minClock: ClockValue = _
    private val stallingThresholdMilliseconds = stallingThresholdSeconds * 1000
    // 60 seconds
    private var stallingTasks = Array.empty[TaskId]

    /** Check for stalling tasks */
    def check(
        currentMinClock: MilliSeconds, processorClocks: Map[ProcessorId, ProcessorClock],
        dag: DAG, now: MilliSeconds): Unit = {
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
        val processorId = dag.graph.topologicalOrderIterator.toList.find { processorId =>
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
    case class ClockValue(systemClock: MilliSeconds, appClock: MilliSeconds) {
      def prettyPrint: String = {
        "(system clock: " + new Date(systemClock).toString + ", app clock: " + appClock + ")"
      }
    }
  }

  object ProcessorClocks {

    // Get the Min clock of all processors
    def minClock(clock: Array[ProcessorClock]): MilliSeconds = {
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
  case class ChangeToNewDAGSuccess(clocks: Map[ProcessorId, MilliSeconds])

  case class StoredStartClock(clock: MilliSeconds)
}