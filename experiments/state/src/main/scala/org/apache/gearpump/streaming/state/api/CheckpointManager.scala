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

package org.apache.gearpump.streaming.state.api

import org.apache.gearpump.TimeStamp

/**
 * CheckpointManager provides write/read user states to/from
 * a persistent store (e.g. HDFS).
 * It maintains
 *   1. an internal clock that is synced with an reliable external source
 *   2. a checkpoint time which is advanced continuously by the configured
 *   checkpoint interval on each checkpoint commit
 */
trait CheckpointManager {

  /**
   * store timestamp to data mapping
   */
  val checkpointStore: CheckpointStore

  /**
   * interval to write checkpoint to CheckpointStore
   * @param interval in milliseconds
   */
  def setCheckpointInterval(interval: Long): Unit

  /**
   * read checkpoint at given timestamp from CheckpointStore
   * @param timestamp
   * @return
   */
  def recover(timestamp: TimeStamp): Option[Array[Byte]]

  /**
   * sync time with a reliable external clock
   * @param timestamp
   */
  def syncTime(timestamp: TimeStamp): Unit

  /**
   * whether the timestamp has exceeded the checkpoint time
   * @return
   */
  def shouldCheckpoint: Boolean

  /**
   * write out data at the timestamp to CheckpointStore
   * @param timestamp
   * @param data
   */
  def checkpoint(timestamp: TimeStamp, data: Array[Byte]): Unit

  /**
   * time to commit next checkpoint
   * @return None if the internal clock is not synced yet
   */
  def getCheckpointTime: Option[TimeStamp]

  /**
   * close external resources like CheckpointStore
   */
  def close(): Unit
}
