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
import org.apache.gearpump.streaming.state.api.{State, CheckpointStore, StateSerializer}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

object DefaultState {
  private val LOG: Logger = LogUtil.getLogger(classOf[DefaultState[_]])
}


/**
 * manage states for non-window applications
 */
class DefaultState[T](val stateSerializer: StateSerializer[T],
                      val checkpointInterval: Long,
                      val checkpointStore: CheckpointStore,
                      val taskContext: TaskContext) extends StateManager[T] with State[T] {
  import org.apache.gearpump.streaming.state.impl.DefaultState._

  private var checkpointState: Option[T] = None
  private var state: Option[T] = None

  override def recover(timestamp: TimeStamp): Unit = {
    super.recover(timestamp)
    state = checkpointStore.read(timestamp).map(stateSerializer.deserialize)
    state match {
      case Some(s) =>
        LOG.info(s"recovered state $s at $timestamp")
      case None =>
        LOG.warn(s"checkpoint at $timestamp not found")
    }
    checkpointState = state
  }

  override def update(timestamp: TimeStamp, t: T, aggregate: (T, T) => T): Unit = {
    super.update(timestamp, t, aggregate)

    val checkpointTime = getCheckpointTime
    if (timestamp < checkpointTime) {
      checkpointState = Option(checkpointState.fold(t)(aggregate(_, t)))
    }

    state = Option(state.fold(t)(aggregate(_, t)))
  }

  override def checkpoint(timestamp: TimeStamp): Unit = {
    checkpointState.foreach{state =>
      LOG.debug(s"checkpoint ($timestamp, $state) at $timestamp")
      checkpointStore.write(timestamp, stateSerializer.serialize(state))
    }
    checkpointState = state
  }

  override def get: Option[T] = state

  private[impl] def getCheckpointState: Option[T] = checkpointState

}
