/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.sink

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

object DataSinkTask {
  val DATA_SINK = "data_sink"
}

/**
 * General task that runs any [[DataSink]]
 */
class DataSinkTask(context: TaskContext, conf: UserConfig) extends Task(context, conf) {
  import org.apache.gearpump.streaming.sink.DataSinkTask._

  private val sink = conf.getValue[DataSink](DATA_SINK).get

  override def onStart(startTime: StartTime): Unit = {
    LOG.info("opening data sink...")
    sink.open(context)
  }

  override def onNext(message: Message): Unit = {
    sink.write(message)
  }

  override def onStop(): Unit = {
    LOG.info("closing data sink...")
    sink.close()
  }
}
