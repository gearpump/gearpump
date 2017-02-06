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
    operator: Option[FunctionRunner[IN, OUT]])
  extends Task(context, conf) {

  def this(context: TaskContext, conf: UserConfig) = {
    this(context, conf,
      conf.getValue[DataSource](GEARPUMP_STREAMING_SOURCE)(context.system).get,
      conf.getValue[FunctionRunner[IN, OUT]](GEARPUMP_STREAMING_OPERATOR)(context.system)
    )
  }

  private val batchSize = conf.getInt(DataSourceConfig.SOURCE_READ_BATCH_SIZE).getOrElse(1000)

  private val processMessage: Message => Unit =
    operator match {
      case Some(op) =>
        (message: Message) => {
          op.process(message.msg.asInstanceOf[IN]).foreach { m: OUT =>
            context.output(Message(m, message.timestamp))
          }
        }
      case None =>
        (message: Message) => context.output(message)
    }

  override def onStart(startTime: Instant): Unit = {
    LOG.info(s"opening data source at $startTime")
    source.open(context, startTime)
    operator.foreach(_.setup())

    self ! Watermark(source.getWatermark)
  }

  override def onNext(m: Message): Unit = {
    0.until(batchSize).foreach { _ =>
      Option(source.read()).foreach(processMessage)
    }

    self ! Watermark(source.getWatermark)
  }

  override def onStop(): Unit = {
    operator.foreach(_.teardown())
    LOG.info("closing data source...")
    source.close()
  }

}
