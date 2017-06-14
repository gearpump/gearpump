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
import java.util.function.Consumer

import com.gs.collections.impl.map.mutable.UnifiedMap
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.Constants.{GEARPUMP_STREAMING_GROUPBY_FUNCTION, GEARPUMP_STREAMING_OPERATOR}
import org.apache.gearpump.streaming.dsl.window.impl.{TimestampedValue, WindowRunner}
import org.apache.gearpump.streaming.task.{Task, TaskContext}

/**
 * Processes messages in groups as defined by groupBy function.
 */
class GroupByTask[IN, GROUP, OUT](
    groupBy: IN => GROUP,
    taskContext: TaskContext,
    userConfig: UserConfig) extends Task(taskContext, userConfig) {

  def this(context: TaskContext, conf: UserConfig) = {
    this(
      conf.getValue[IN => GROUP](GEARPUMP_STREAMING_GROUPBY_FUNCTION)(context.system).get,
      context, conf
    )
  }

  private val groups: UnifiedMap[GROUP, WindowRunner[IN, OUT]] =
    new UnifiedMap[GROUP, WindowRunner[IN, OUT]]

  override def onNext(message: Message): Unit = {
    val input = message.value.asInstanceOf[IN]
    val group = groupBy(input)

    if (!groups.containsKey(group)) {
      groups.put(group,
        userConfig.getValue[WindowRunner[IN, OUT]](
          GEARPUMP_STREAMING_OPERATOR)(taskContext.system).get)
    }

    groups.get(group).process(TimestampedValue(message.value.asInstanceOf[IN],
      message.timestamp))
  }

  override def onWatermarkProgress(watermark: Instant): Unit = {
    groups.values.forEach(new Consumer[WindowRunner[IN, OUT]] {
      override def accept(runner: WindowRunner[IN, OUT]): Unit = {
        runner.trigger(watermark).foreach {
          result =>
            taskContext.output(Message(result.value, result.timestamp))
        }
      }
    })
  }
}
