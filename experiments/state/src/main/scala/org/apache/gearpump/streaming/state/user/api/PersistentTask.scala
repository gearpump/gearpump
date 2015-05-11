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

package org.apache.gearpump.streaming.state.user.api

import org.apache.gearpump.{TimeStamp, Message}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.state.system.api.PersistentState
import org.apache.gearpump.streaming.state.system.impl._
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

/**
 * PersistentTask is part of the transaction API
 *
 * Users should extend this task if they want to get transaction support
 * from the framework
 */
abstract class PersistentTask[T](taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  val stateConfig = conf.getValue[PersistentStateConfig](PersistentStateConfig.NAME).get
  val checkpointInterval = stateConfig.getCheckpointInterval
  val checkpointStore = stateConfig.getCheckpointStoreFactory.getCheckpointStore(conf, taskContext)
  val checkpointManager = new CheckpointManager(checkpointInterval, checkpointStore)

  /**
   * subclass should override this method to pass in
   * a PersistentState
   *
   * the framework has already offered two states
   *
   *   - UnboundedState
   *     state with no time or other boundary
   *   - WindowState
   *     each state is bounded by a time window
   */
  def persistentState: PersistentState[T]

  /**
   * subclass should override this method to specify how a
   * new message should update state
   */
  def processMessage(state: PersistentState[T], message: Message, checkpointTime: TimeStamp): Unit

  val state = persistentState

  final override def onStart(startTime: StartTime): Unit = {
    val timestamp = startTime.startTime
    checkpointManager
      .recover(timestamp)
      .foreach(state.recover(timestamp, _))
  }

  final override def onNext(message: Message): Unit = {
    val checkpointTime = checkpointManager.getCheckpointTime
    checkpointManager.update(message.timestamp)
    if (taskContext.upstreamMinClock >= checkpointTime) {
      val serialized = state.checkpoint(checkpointTime)
      checkpointManager.checkpoint(checkpointTime, serialized)
      checkpointManager.updateCheckpointTime()
    }

    processMessage(state, message, checkpointTime)
  }
}



