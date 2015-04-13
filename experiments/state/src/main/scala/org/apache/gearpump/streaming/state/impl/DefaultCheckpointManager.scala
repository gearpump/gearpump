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

package org.apache.gearpump.streaming.state.impl

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.state.api.{CheckpointStore, CheckpointManager}

object DefaultCheckpointManager {
  val CHECKPOINT_INTERVAL = "checkpoint_interval"
}

class DefaultCheckpointManager(val checkpointStore: CheckpointStore = new InMemoryCheckpointStore,
                               private var checkpointInterval: Long = 0L) extends CheckpointManager {

  private var checkpointTime: Option[Long] = None
  private var clock = 0L

  /**
   * checkpointInterval change will affect checkpointTime
   * after next checkpoint
   * @param interval
   */
  override def setCheckpointInterval(interval: Long): Unit = {
    this.checkpointInterval = interval
  }

  override def syncTime(timestamp: TimeStamp): Unit = {
    this.clock = timestamp
    if (checkpointTime.isEmpty) {
      checkpointTime = Some(clock + checkpointInterval)
    }
  }

  override def shouldCheckpoint: Boolean = {
    checkpointTime.nonEmpty && clock >= checkpointTime.get
  }

  override def recover(timestamp: TimeStamp): Option[Array[Byte]] = {
    checkpointStore.read(timestamp)
  }

  override def checkpoint(timestamp: TimeStamp, data: Array[Byte]): Unit = {
    checkpointStore.write(timestamp, data)
    checkpointTime = checkpointTime.map(_ + checkpointInterval)
  }

  override def getCheckpointTime: Option[TimeStamp] = checkpointTime

  override def close(): Unit = {
    checkpointStore.close()
  }
}
