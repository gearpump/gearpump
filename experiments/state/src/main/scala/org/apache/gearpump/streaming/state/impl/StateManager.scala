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

import org.apache.gearpump._
import org.apache.gearpump.streaming.state.api._
import org.apache.gearpump.streaming.task.TaskContext



/**
 * this class helps to manage the state clock and checkpoint time
 * but leaves the specific state update and checkpoint to subclass
 */
private[impl] trait StateManager[T] extends State[T] {

  val stateSerializer: StateSerializer[T]
  val taskContext: TaskContext
  val checkpointInterval: Long
  private var checkpointStore: CheckpointStore = null
  private var maxMessageTime: TimeStamp = 0L
  private var stateClock: TimeStamp = 0L
  private var checkpointTime: TimeStamp = checkpointInterval

  override def setCheckpointStore(checkpointStore: CheckpointStore): Unit = {
    this.checkpointStore = checkpointStore
  }

  /**
   * recover state at given time
   * @param timestamp
   */
  override def recover(timestamp: TimeStamp): Unit = {
    stateClock = timestamp
    checkpointTime = timestamp + checkpointInterval
  }

  /**
   *  subclass are required to extend this and
   *    1. firstly call super.update() at the beginning
   *    2. define how state is updated
   */
  override def update(timestamp: TimeStamp, t: T, aggregate: (T, T) => T): Unit = {
    maxMessageTime = Math.max(timestamp, maxMessageTime)

    if (upstreamMinClock >= checkpointTime) {
      checkpoint(checkpointTime)
      stateClock = checkpointTime
      checkpointTime = getNextCheckpointTime(checkpointTime, maxMessageTime, checkpointInterval)
    }
  }

  /**
   * report state clock to task
   * @return state clock
   */
  override def minClock: TimeStamp = stateClock

  private[impl] def getCheckpointTime: TimeStamp = checkpointTime

  private[impl] def getMaxMessageTime:  TimeStamp = maxMessageTime

  protected def getNextCheckpointTime(lastTime: TimeStamp, maxClock: TimeStamp, interval: Long): Long = {
    lastTime + interval * (1 + (maxMessageTime - lastTime) / interval)
  }

  protected def upstreamMinClock: TimeStamp = taskContext.upstreamMinClock

}
