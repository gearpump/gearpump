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
import org.apache.gearpump.streaming.state.internal.api.{CheckpointStoreFactory, WindowManager}
import org.apache.gearpump.streaming.state.internal.impl.{WindowStateManager, DefaultStateManager, DefaultCheckpointManager}
import org.apache.gearpump.streaming.state.util.StateConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

/**
 * StatefulTask provides transaction and state support for users
 * users should extend this class if their application requires state management and exact-once processing
 * they only need to override the onUpdate method which defines how to get raw data of type T from message
 */
abstract class StatefulTask[T](state: MonoidState[T], taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  private val stateConfig = new StateConfig(conf)
  private val window = stateConfig.getWindow
  private val checkpointInterval = stateConfig.getCheckpointInterval
  private val checkpointStore = stateConfig.getCheckpointStoreFactory
    .getCheckpointStore(conf, taskContext)
  private val checkpointManager = new DefaultCheckpointManager(checkpointStore,  checkpointInterval)
  private val stateManager = window match {
    case Some(Window(duration, period)) =>
      val windowSize = checkpointInterval
      val windowNum = duration.toMillis / windowSize
      val windowStride = period.toMillis / windowSize
      val windowManager = new WindowManager(windowSize, windowNum, windowStride)
      new WindowStateManager[T](state, checkpointInterval, checkpointManager, windowManager, taskContext)
    case None =>
      new DefaultStateManager[T](state, checkpointInterval, checkpointManager, taskContext)
  }


  def onUpdate(msg: Message): T

  override def onStart(startTime: StartTime): Unit = {
    stateManager.init(startTime.startTime)
  }

  override def onNext(msg: Message): Unit = {
    stateManager.update(msg, onUpdate, taskContext.upstreamMinClock)
  }

  override def stateClock: Option[TimeStamp] = {
    Option(stateManager.minClock)
  }
}
