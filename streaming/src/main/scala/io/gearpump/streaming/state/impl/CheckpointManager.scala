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

package io.gearpump.streaming.state.impl

import io.gearpump.Time.MilliSeconds
import io.gearpump.streaming.transaction.api.CheckpointStore

/** Manage physical checkpoints to persitent storage like HDFS */
class CheckpointManager(checkpointInterval: Long,
    checkpointStore: CheckpointStore) {

  private var maxMessageTime: Long = 0L
  private var checkpointTime: Option[Long] = None

  def recover(timestamp: MilliSeconds): Option[Array[Byte]] = {
    checkpointStore.recover(timestamp)
  }

  def checkpoint(timestamp: MilliSeconds, checkpoint: Array[Byte]): Option[MilliSeconds] = {
    checkpointStore.persist(timestamp, checkpoint)
    checkpointTime = checkpointTime.collect { case time if maxMessageTime > time =>
      time + (1 + (maxMessageTime - time) / checkpointInterval) * checkpointInterval
    }

    checkpointTime
  }

  def update(messageTime: MilliSeconds): Option[MilliSeconds] = {
    maxMessageTime = Math.max(maxMessageTime, messageTime)
    if (checkpointTime.isEmpty) {
      checkpointTime = Some((1 + messageTime / checkpointInterval) * checkpointInterval)
    }

    checkpointTime
  }

  def shouldCheckpoint(upstreamMinClock: MilliSeconds): Boolean = {
    checkpointTime.exists(time => upstreamMinClock >= time)
  }

  def getCheckpointTime: Option[MilliSeconds] = checkpointTime

  def close(): Unit = {
    checkpointStore.close()
  }

  private[impl] def getMaxMessageTime: MilliSeconds = maxMessageTime
}
