/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming.dsl.task

import com.gs.collections.impl.map.mutable.UnifiedMap
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.Constants.{GEARPUMP_STREAMING_GROUPBY_FUNCTION, GEARPUMP_STREAMING_OPERATOR}
import io.gearpump.streaming.dsl.window.impl.{StreamingOperator, TimestampedValue}
import io.gearpump.streaming.source.Watermark
import io.gearpump.streaming.task.{Task, TaskContext, TaskUtil}
import java.time.Instant
import java.util.function.Consumer

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

  private val groups: UnifiedMap[GROUP, StreamingOperator[IN, OUT]] =
    new UnifiedMap[GROUP, StreamingOperator[IN, OUT]]

  override def onNext(message: Message): Unit = {
    val input = message.value.asInstanceOf[IN]
    val group = groupBy(input)

    if (!groups.containsKey(group)) {
      val operator = userConfig.getValue[StreamingOperator[IN, OUT]](
        GEARPUMP_STREAMING_OPERATOR)(taskContext.system).get
      operator.setup()
      groups.put(group, operator)
    }

    groups.get(group).foreach(TimestampedValue(message.value.asInstanceOf[IN],
      message.timestamp))
  }

  override def onWatermarkProgress(watermark: Instant): Unit = {
    if (groups.isEmpty && watermark == Watermark.MAX) {
      taskContext.updateWatermark(Watermark.MAX)
    } else {
      groups.values.forEach(new Consumer[StreamingOperator[IN, OUT]] {
        override def accept(operator: StreamingOperator[IN, OUT]): Unit = {
          TaskUtil.trigger(watermark, operator, taskContext)
        }
      })
    }
  }

  override def onStop(): Unit = {
    groups.values.forEach(new Consumer[StreamingOperator[IN, OUT]] {
      override def accept(operator: StreamingOperator[IN, OUT]): Unit = {
        operator.teardown()
      }
    })
  }
}
