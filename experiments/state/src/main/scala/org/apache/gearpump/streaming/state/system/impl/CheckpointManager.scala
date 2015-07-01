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

package org.apache.gearpump.streaming.state.system.impl

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.state.system.api.CheckpointStore


class CheckpointManager(checkpointInterval: Long,
                        checkpointStore: CheckpointStore) {

  private var maxMessageTime = 0L
  private var checkpointTime = checkpointInterval

  def recover(timestamp: TimeStamp): Option[Array[Byte]] = {
    checkpointTime = timestamp + checkpointInterval
    checkpointStore.read(timestamp)
  }

  def checkpoint(timestamp: TimeStamp, checkpoint: Array[Byte]): Unit = {
    checkpointStore.write(timestamp, checkpoint)
  }

  def update(messageTime: TimeStamp): Unit = {
    maxMessageTime = Math.max(maxMessageTime, messageTime)
  }

  def getCheckpointTime: TimeStamp = checkpointTime

  def updateCheckpointTime(): Unit = {
    if (maxMessageTime >= checkpointTime) {
      checkpointTime += (1 + (maxMessageTime - checkpointTime) / checkpointInterval) * checkpointInterval
    }
  }

  def close(): Unit = {
    checkpointStore.close()
  }

  private[impl] def getMaxMessageTime: TimeStamp = maxMessageTime
}
