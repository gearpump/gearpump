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

package io.gearpump.streaming.state.api

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.state.impl.{CheckpointManager, PersistentStateConfig}
import io.gearpump.streaming.task.{ReportCheckpointClock, StartTime, Task, TaskContext}
import io.gearpump.streaming.transaction.api.CheckpointStoreFactory
import io.gearpump.util.LogUtil
import io.gearpump.{Message, TimeStamp}

object PersistentTask {
  val CHECKPOINT = Message("checkpoint")
  val LOG = LogUtil.getLogger(getClass)
}

/**
 * PersistentTask is part of the transaction API
 *
 * Users should extend this task if they want to get transaction support
 * from the framework
 */
abstract class PersistentTask[T](taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {
  import taskContext._

  import io.gearpump.streaming.state.api.PersistentTask._

  val checkpointStoreFactory = conf.getValue[CheckpointStoreFactory](
    PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY).get
  val checkpointStore = checkpointStoreFactory.getCheckpointStore(conf, taskContext)
  val checkpointInterval = conf.getLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS).get
  val checkpointManager = new CheckpointManager(checkpointInterval, checkpointStore)
  // system time interval to attempt checkpoint
  private val checkpointAttemptInterval = 1000L

  /**
   * subclass should override this method to pass in
   * a PersistentState
   *
   * the framework has already offered two states
   *
   * - NonWindowState
   * state with no time or other boundary
   * - WindowState
   * each state is bounded by a time window
   */
  def persistentState: PersistentState[T]

  /**
   * subclass should override this method to specify how a
   * new message should update state
   */
  def processMessage(state: PersistentState[T], message: Message): Unit

  val state = persistentState

  final override def onStart(startTime: StartTime): Unit = {
    val timestamp = startTime.startTime
    checkpointManager
      .recover(timestamp)
      .foreach(state.recover(timestamp, _))

    reportCheckpointClock(timestamp)
    scheduleCheckpoint(checkpointAttemptInterval)
  }

  final override def onNext(message: Message): Unit = {
    message match {
      case CHECKPOINT =>
        val upstreamMinClock = taskContext.upstreamMinClock
        if (checkpointManager.shouldCheckpoint(upstreamMinClock)) {
          checkpointManager.getCheckpointTime.foreach { checkpointTime =>
            val serialized = state.checkpoint()
            checkpointManager.checkpoint(checkpointTime, serialized)
              .foreach(state.setNextCheckpointTime)
            taskContext.output(Message(serialized, checkpointTime))
            reportCheckpointClock(checkpointTime)
          }
        }
        scheduleCheckpoint(checkpointAttemptInterval)
      case _ =>
        checkpointManager.update(message.timestamp)
          .foreach(state.setNextCheckpointTime)
        processMessage(state, message)
    }
  }

  final override def onStop(): Unit = {
    checkpointManager.close()
  }

  private def scheduleCheckpoint(interval: Long): Unit = {
    scheduleOnce(new FiniteDuration(interval, TimeUnit.MILLISECONDS))(self ! CHECKPOINT)
  }

  private def reportCheckpointClock(timestamp: TimeStamp): Unit = {
    appMaster ! ReportCheckpointClock(taskContext.taskId, timestamp)
  }
}
