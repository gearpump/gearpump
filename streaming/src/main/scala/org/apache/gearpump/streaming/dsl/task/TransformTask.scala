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
package org.apache.gearpump.streaming.dsl.task

import java.time.Instant

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.Constants._
import org.apache.gearpump.streaming.dsl.window.impl.{TimestampedValue, WindowRunner}
import org.apache.gearpump.streaming.task.{Task, TaskContext, TaskUtil}

class TransformTask[IN, OUT](
    runner: WindowRunner[IN, OUT],
    taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {

  def this(context: TaskContext, conf: UserConfig) = {
    this(
      conf.getValue[WindowRunner[IN, OUT]](GEARPUMP_STREAMING_OPERATOR)(context.system).get,
      context, conf
    )
  }

  override def onNext(msg: Message): Unit = {
    runner.process(TimestampedValue(msg.value.asInstanceOf[IN], msg.timestamp))
  }

  override def onWatermarkProgress(watermark: Instant): Unit = {
    TaskUtil.trigger(watermark, runner, taskContext)
  }
}
