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

package org.apache.gearpump.streaming.source

import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

object DataSourceTask {
  val DATA_SOURCE = "data_source"
}

/**
 * general task that runs any [[DataSource]]
 * see [[DataSourceProcessor]] for its usage
 */
class DataSourceTask(context: TaskContext, conf: UserConfig) extends Task(context, conf) {
  import org.apache.gearpump.streaming.source.DataSourceTask._

  private val source = conf.getValue[DataSource](DATA_SOURCE).get

  override def onStart(newStartTime: StartTime): Unit = {
    val time = newStartTime.startTime
    LOG.info(s"opening data source at $time")
    source.open(context, time)
    self ! Message("start", System.currentTimeMillis())
  }

  override def onNext(message: Message): Unit = {
    source.read().foreach(context.output)
    self ! Message("continue", System.currentTimeMillis())
  }

  override def onStop(): Unit = {
    LOG.info("closing data source...")
    source.close()
  }
}
