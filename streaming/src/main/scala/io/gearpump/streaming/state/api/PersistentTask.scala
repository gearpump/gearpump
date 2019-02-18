/*
 * Licensed under the Apache License, Version 2.0 (the
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

import io.gearpump.Message
import io.gearpump.Time.MilliSeconds
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.state.impl.{CheckpointManager, PersistentStateConfig}
import io.gearpump.streaming.task.{Task, TaskContext, UpdateCheckpointClock}
import io.gearpump.streaming.transaction.api.CheckpointStoreFactory
import io.gearpump.util.LogUtil
import java.time.Instant

object PersistentTask {
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

  val checkpointStoreFactory = conf.getValue[CheckpointStoreFactory](
    PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY).get
  val checkpointStore = checkpointStoreFactory.getCheckpointStore(
    s"app$appId-task${taskId.processorId}_${taskId.index}")
  val checkpointInterval = conf.getLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS).get
  val checkpointManager = new CheckpointManager(checkpointInterval, checkpointStore)

  /**
   * Subclass should override this method to pass in a PersistentState. the framework has already
   * offered two states:
   *  - NonWindowState: state with no time or other boundary
   *  - WindowState:  each state is bounded by a time window
   */
  def persistentState: PersistentState[T]

  /**
   * Subclass should override this method to specify how a new message should update state
   */
  def processMessage(state: PersistentState[T], message: Message): Unit

  /** Persistent state that will be stored (by checkpointing) automatically to storage like HDFS */
  private var state: PersistentState[T] = _

  def getState: PersistentState[T] = state

  final override def onStart(startTime: Instant): Unit = {
    state = persistentState
    val timestamp = startTime.toEpochMilli
    checkpointManager
      .recover(timestamp)
      .foreach(state.recover(timestamp, _))

    reportCheckpointClock(timestamp)
  }

  final override def onNext(message: Message): Unit = {
    checkpointManager.update(message.timestamp.toEpochMilli)
      .foreach(state.setNextCheckpointTime)
    processMessage(state, message)
  }

  final override def onWatermarkProgress(watermark: Instant): Unit = {
    if (checkpointManager.shouldCheckpoint(watermark.toEpochMilli)) {
      checkpointManager.getCheckpointTime.foreach { checkpointTime =>
        val serialized = state.checkpoint()
        checkpointManager.checkpoint(checkpointTime, serialized)
          .foreach(state.setNextCheckpointTime)
        reportCheckpointClock(checkpointTime)
      }
    }
  }

  final override def onStop(): Unit = {
    checkpointManager.close()
  }

  private def reportCheckpointClock(timestamp: MilliSeconds): Unit = {
    appMaster ! UpdateCheckpointClock(taskContext.taskId, timestamp)
  }
}
