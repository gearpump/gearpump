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

package org.apache.gearpump.streaming.state.internal.impl

import org.apache.gearpump._
import org.apache.gearpump.streaming.state.api.MonoidState
import org.apache.gearpump.streaming.state.internal.api.{CheckpointWindow, ClockListener, CheckpointManager, StateManager}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.immutable.TreeMap

object DefaultStateManager {
  private val LOG: Logger = LogUtil.getLogger(classOf[DefaultStateManager[_]])
}

/**
 * manage states for non-window applications
 */
class DefaultStateManager[T](val state: MonoidState[T],
                             val checkpointWindowSize: Long,
                             val checkpointManager: CheckpointManager,
                             taskContext: TaskContext) extends StateManager[T] {
  import org.apache.gearpump.streaming.state.internal.impl.DefaultStateManager._

  private var value: T = state.init

  override val clockListeners: Array[ClockListener] = Array(checkpointManager)

  override def init(timestamp: TimeStamp): Unit = {
    super.init(timestamp)
    checkpointManager.recover(timestamp) match {
      case Some(bytes) => 
        value = state.deserialize(bytes)
        LOG.info(s"recovered $value at $timestamp")
      case None => LOG.warn(s"checkpoint at $timestamp not found")
    }
  }

  /**
   * 1. checkpoint states
   * 2. aggregate checkpointed states to value and send it down streams
   * 3. drop all the expired states
   * @param states
   */
  override def checkpoint(states: TreeMap[CheckpointWindow, T]): Unit = {
    states.foreach { case (win, st) =>
      value = state.update(value, st)
      checkpointManager.checkpoint(win.endTime, state.serialize(value))
      LOG.info(s"checkpoint (${win.endTime}, $value) at ${win.endTime}")
      taskContext.output(Message(state.serialize(value), win.endTime))
    }
    states.lastOption.foreach { case (win, _) =>
      dropStatesBefore(win.endTime)
    }
  }

}
