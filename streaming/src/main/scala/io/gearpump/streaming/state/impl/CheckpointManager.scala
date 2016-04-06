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

package io.gearpump.streaming.state.impl

import io.gearpump.TimeStamp
import io.gearpump.streaming.transaction.api.CheckpointStore

class CheckpointManager(checkpointInterval: Long,
    checkpointStore: CheckpointStore) {

  private var maxMessageTime: Long = 0L
  private var checkpointTime: Option[Long] = None

  def recover(timestamp: TimeStamp): Option[Array[Byte]] = {
    checkpointStore.recover(timestamp)
  }

  def checkpoint(timestamp: TimeStamp, checkpoint: Array[Byte]): Option[TimeStamp] = {
    checkpointStore.persist(timestamp, checkpoint)
    checkpointTime = checkpointTime.collect { case time if maxMessageTime > time =>
      time + (1 + (maxMessageTime - time) / checkpointInterval) * checkpointInterval
    }

    checkpointTime
  }

  def update(messageTime: TimeStamp): Option[TimeStamp] = {
    maxMessageTime = Math.max(maxMessageTime, messageTime)
    if (checkpointTime.isEmpty) {
      checkpointTime = Some((1 + messageTime / checkpointInterval) * checkpointInterval)
    }

    checkpointTime
  }

  def shouldCheckpoint(upstreamMinClock: TimeStamp): Boolean = {
    checkpointTime.exists(time => upstreamMinClock >= time)
  }

  def getCheckpointTime: Option[TimeStamp] = checkpointTime

  def close(): Unit = {
    checkpointStore.close()
  }

  private[impl] def getMaxMessageTime: TimeStamp = maxMessageTime
}
