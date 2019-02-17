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

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.Constants._
import io.gearpump.streaming.dsl.window.impl.{StreamingOperator, TimestampedValue}
import io.gearpump.streaming.task.{Task, TaskContext, TaskUtil}
import java.time.Instant

class TransformTask[IN, OUT](
    operator: StreamingOperator[IN, OUT],
    taskContext: TaskContext,
    conf: UserConfig) extends Task(taskContext, conf) {

  def this(context: TaskContext, conf: UserConfig) = {
    this(
      conf.getValue[StreamingOperator[IN, OUT]](GEARPUMP_STREAMING_OPERATOR)(context.system).get,
      context, conf
    )
  }

  override def onStart(startTime: Instant): Unit = {
    operator.setup()
  }

  override def onNext(msg: Message): Unit = {
    operator.foreach(TimestampedValue(msg.value.asInstanceOf[IN], msg.timestamp))
  }

  override def onWatermarkProgress(watermark: Instant): Unit = {
    TaskUtil.trigger(watermark, operator, taskContext)
  }

  override def onStop(): Unit = {
    operator.teardown()
  }
}
