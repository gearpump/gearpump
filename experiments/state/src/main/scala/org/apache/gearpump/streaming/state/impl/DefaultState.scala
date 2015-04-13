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
import org.apache.gearpump.streaming.state.api.{CheckpointManager, StateOp, State}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

object DefaultState {
  val LOG: Logger = LogUtil.getLogger(classOf[DefaultState[_]])
}

class DefaultState[T](op: StateOp[T],
                      checkpointManager: CheckpointManager,
                      taskContext: TaskContext) extends State[T] {
  import org.apache.gearpump.streaming.state.impl.DefaultState._

  private var state: T = op.init
  private var cachedMessages = Vector.empty[Message]
  private var stateClock = 0L

  /**
   * set state clock to the timestamp and
   * recover state at timestamp from CheckpointStore
   * @param timestamp
   */
  override def init(timestamp: TimeStamp): Unit = {
    stateClock = timestamp
    checkpointManager.recover(timestamp).foreach { bytes =>
      state = op.deserialize(bytes)
    }
  }

  /**
   * messages with timestamp < checkpointTime will affect state immediately
   * otherwise, they are cached for later processing
   * @param msg
   */
  override def update(msg: Message): Unit = {
    checkpointManager.syncTime(taskContext.upstreamMinClock)
    val checkpointTime = checkpointManager.getCheckpointTime
    if (checkpointTime.nonEmpty && msg.timestamp >= checkpointTime.get) {
      cachedMessages :+= msg
    } else {
      state = op.update(msg, state)
    }
    if (checkpointManager.shouldCheckpoint) {
      checkpointManager.checkpoint(checkpointTime.get, op.serialize(state))
      LOG.info(s"checkpoint (${checkpointTime.get}, $state) at ${checkpointTime.get}")
      val nextCheckpointTime = checkpointManager.getCheckpointTime
      // it's possible that some cachedMessages' timestamps exceed next (or next next ...) checkpoint time as well
      // they will stay in cache till their time comes
      val (messages: Vector[Message], newCachedMessages: Vector[Message]) = cachedMessages.span(_.timestamp < nextCheckpointTime.get)
      messages.foreach(msg => state = op.update(msg, state))
      cachedMessages = newCachedMessages
      stateClock = checkpointTime.get
    }
  }

  override def minClock: TimeStamp = stateClock

  override def unwrap: Option[T] = Option(state)
}
