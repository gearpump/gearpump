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

package io.gearpump.streaming.sink

import io.gearpump.Message
import io.gearpump.Time.MilliSeconds
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.state.impl.{CheckpointManager, PersistentStateConfig}
import io.gearpump.streaming.task.{Task, TaskContext, UpdateCheckpointClock}
import io.gearpump.streaming.transaction.api.CheckpointStoreFactory
import java.time.Instant

object DataSinkTask {
  val DATA_SINK = "data_sink"
}

/**
 * General task that runs any [[DataSink]]
 */
class DataSinkTask private[sink](context: TaskContext, conf: UserConfig, sink: DataSink)
  extends Task(context, conf) {

  private val committableSink = sink match {
    case committable: CommittableDataSink => Some(committable)
    case _ => None
  }
  private var checkpointManager: CheckpointManager = _

  def this(context: TaskContext, conf: UserConfig) = {
    this(context, conf, conf.getValue[DataSink](DataSinkTask.DATA_SINK)(context.system).get)
  }

  override def onStart(startTime: Instant): Unit = {
    LOG.info("opening data sink...")
    committableSink.foreach { _ =>
      require(
        conf.getBoolean(PersistentStateConfig.STATE_CHECKPOINT_ENABLE).contains(true),
        s"${PersistentStateConfig.STATE_CHECKPOINT_ENABLE} must be true for a " +
          "CommittableDataSink")
      val checkpointStoreFactory = conf.getValue[CheckpointStoreFactory](
        PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY).getOrElse {
        throw new IllegalArgumentException(
          s"${PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY} must be configured for a " +
            "CommittableDataSink")
      }
      val checkpointInterval = conf.getLong(
        PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS).filter(_ > 0L).getOrElse {
        throw new IllegalArgumentException(
          s"${PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS} must be greater than zero for " +
            "a CommittableDataSink")
      }
      val checkpointStore = checkpointStoreFactory.getCheckpointStore(
        s"app${context.appId}-task${context.taskId.processorId}_${context.taskId.index}")
      checkpointManager = new CheckpointManager(checkpointInterval, checkpointStore)
    }
    sink.open(context)
    committableSink.foreach { committable =>
      val timestamp = startTime.toEpochMilli
      checkpointManager.recover(timestamp).foreach { checkpoint =>
        committable.restoreCommit(timestamp, checkpoint)
        committable.commit(timestamp)
      }
      reportCheckpointClock(timestamp)
    }
  }

  override def onNext(message: Message): Unit = {
    committableSink.foreach { committable =>
      checkpointManager.update(message.timestamp.toEpochMilli)
        .foreach(committable.setNextCheckpointTime)
    }
    sink.write(message)
  }

  override def onStop(): Unit = {
    LOG.info("closing data sink...")
    try {
      sink.close()
    } finally {
      if (checkpointManager != null) {
        checkpointManager.close()
        checkpointManager = null
      }
    }
  }

  override def onWatermarkProgress(watermark: Instant): Unit = {
    committableSink match {
      case Some(committable) =>
        while (checkpointManager.shouldCheckpoint(watermark.toEpochMilli)) {
          val checkpointTime = checkpointManager.getCheckpointTime.get
          val checkpoint = committable.prepareCommit(checkpointTime)
          val nextCheckpointTime = checkpointManager.checkpoint(checkpointTime, checkpoint)
          committable.commit(checkpointTime)
          nextCheckpointTime.foreach(committable.setNextCheckpointTime)
          reportCheckpointClock(checkpointTime)
        }
      case None => sink.onWatermarkProgress(watermark)
    }
    context.updateWatermark(watermark)
  }

  private def reportCheckpointClock(timestamp: MilliSeconds): Unit = {
    context.appMaster ! UpdateCheckpointClock(context.taskId, timestamp)
  }
}
