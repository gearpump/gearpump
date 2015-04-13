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

package org.apache.gearpump.streaming.state.example.processor

import org.apache.gearpump.{TimeStamp, Message}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.state.example.op.Count
import org.apache.gearpump.streaming.state.impl._
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

class CountProcessor(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  val stateOp = new Count
  val checkpointInterval = conf.getLong(DefaultCheckpointManager.CHECKPOINT_INTERVAL)
    .getOrElse(throw new RuntimeException("checkpoint interval not configured"))
  val checkpointManager = new DefaultCheckpointManager(checkpointInterval = checkpointInterval)
  val state = new DefaultState(stateOp, checkpointManager, taskContext)

  override def onStart(startTime: StartTime): Unit = {
    state.init(startTime.startTime)
  }

  override def onNext(msg: Message): Unit = {
    state.update(msg)
  }

  override def stateClock: Option[TimeStamp] = {
    Option(state.minClock)
  }
}
