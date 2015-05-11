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

import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.state.impl.{WindowState, DefaultState}
import org.apache.gearpump.streaming.state.util.StateConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

/**
 * StatefulTask provides transaction and state support for users
 * users should extend this class if their application requires
 * state management and exact-once processing. Users usually only
 * need to pass in a StateOp to manipulate state on each new message
 */
abstract class StatefulTask[T](stateOp: StateOp[T], taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  private val stateConfig = new StateConfig(conf)
  private val windowDescription = stateConfig.getWindowDescription
  private val checkpointInterval = stateConfig.getCheckpointInterval
  private val checkpointStore = stateConfig.getCheckpointStoreFactory
    .getCheckpointStore(conf, taskContext)
  protected val state = windowDescription match {
    case Some(description) =>
      new WindowState[T](stateOp.serializer, checkpointInterval, checkpointStore,
        taskContext, description)
    case None =>
      new DefaultState[T](stateOp.serializer, checkpointInterval, checkpointStore,
        taskContext)
  }
  state.setCheckpointStore(checkpointStore)

  override def onStart(startTime: StartTime): Unit = {
    state.recover(startTime.startTime)
  }

  override def onNext(message: Message): Unit = {
    stateOp.update(state, message)
  }

  override def stateClock: Option[TimeStamp] = {
    Option(state.minClock)
  }
}
