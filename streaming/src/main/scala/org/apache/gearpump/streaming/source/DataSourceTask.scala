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

package org.apache.gearpump.streaming.source

import java.time.Instant

import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.Constants._
import org.apache.gearpump.streaming.dsl.plan.functions.FunctionRunner
import org.apache.gearpump.streaming.dsl.task.TransformTask.Transform
import org.apache.gearpump.streaming.task.{Task, TaskContext}

/**
 * Default Task container for [[org.apache.gearpump.streaming.source.DataSource]] that
 * reads from DataSource in batch
 * See [[org.apache.gearpump.streaming.source.DataSourceProcessor]] for its usage
 *
 * DataSourceTask calls:
 *  - `DataSource.open()` in `onStart` and pass in
 *  [[org.apache.gearpump.streaming.task.TaskContext]]
 * and application start time
 *  - `DataSource.read()` in each `onNext`, which reads a batch of messages
 *  - `DataSource.close()` in `onStop`
 */
class DataSourceTask[IN, OUT] private[source](
    context: TaskContext,
    conf: UserConfig,
    source: DataSource,
    transform: Transform[IN, OUT])
  extends Task(context, conf) {

  def this(context: TaskContext, conf: UserConfig) = {
    this(context, conf,
      conf.getValue[DataSource](GEARPUMP_STREAMING_SOURCE)(context.system).get,
      new Transform[IN, OUT](context,
        conf.getValue[FunctionRunner[IN, OUT]](GEARPUMP_STREAMING_OPERATOR)(context.system))
    )
  }

  private val batchSize = conf.getInt(DataSourceConfig.SOURCE_READ_BATCH_SIZE).getOrElse(1000)

  override def onStart(startTime: Instant): Unit = {
    LOG.info(s"opening data source at $startTime")
    source.open(context, startTime)

    self ! Watermark(source.getWatermark)
  }

  override def onNext(m: Message): Unit = {
    0.until(batchSize).foreach { _ =>
      Option(source.read()).foreach(transform.onNext)
    }

    self ! Watermark(source.getWatermark)
  }

  override def onWatermarkProgress(watermark: Instant): Unit = {
    transform.onWatermarkProgress(watermark)
  }

  override def onStop(): Unit = {
    LOG.info("closing data source...")
    source.close()
  }

}
