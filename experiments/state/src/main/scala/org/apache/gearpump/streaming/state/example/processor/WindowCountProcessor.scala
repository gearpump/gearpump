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

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.state.example.op.WindowCount
import org.apache.gearpump.streaming.state.impl.{DefaultCheckpointManager, FixedWindowState}
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.{Message, TimeStamp}
import org.slf4j.Logger

import scala.concurrent.duration._

object WindowCountProcessor {
  val WINDOW_SIZE = "window_size"
  val LOG: Logger = LogUtil.getLogger(classOf[WindowCountProcessor])
}

class WindowCountProcessor(taskContext : TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  import org.apache.gearpump.streaming.state.example.processor.WindowCountProcessor._

  private val windowSize: Duration = conf.getValue(WINDOW_SIZE)
    .getOrElse(throw new RuntimeException("window size not configured"))
  private val checkpointInterval = conf.getLong(DefaultCheckpointManager.CHECKPOINT_INTERVAL)
    .getOrElse(throw new RuntimeException("checkpoint interval not configured"))

  private val stateOp = new WindowCount
  private val checkpointManager = new DefaultCheckpointManager(checkpointInterval = checkpointInterval)
  private val state = new FixedWindowState[(Long, Long)](windowSize.toMillis, stateOp, checkpointManager, taskContext)

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
