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

package io.gearpump.streaming.source

import io.gearpump._
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.Constants._
import io.gearpump.streaming.dsl.window.impl.{StreamingOperator, TimestampedValue}
import io.gearpump.streaming.task.{Task, TaskContext, TaskUtil}
import java.time.Instant

/**
 * Default Task container for [[DataSource]] that
 * reads from DataSource in batch
 * See [[DataSourceProcessor]] for its usage
 *
 * DataSourceTask calls:
 *  - `DataSource.open()` in `onStart` and pass in
 *  [[io.gearpump.streaming.task.TaskContext]]
 * and application start time
 *  - `DataSource.read()` in each `onNext`, which reads a batch of messages
 *  - `DataSource.close()` in `onStop`
 */
class DataSourceTask[IN, OUT] private[source](
    source: DataSource,
    operator: StreamingOperator[IN, OUT],
    context: TaskContext,
    conf: UserConfig)
  extends Task(context, conf) {

  def this(context: TaskContext, conf: UserConfig) = {
    this(
      conf.getValue[DataSource](GEARPUMP_STREAMING_SOURCE)(context.system).get,
      conf.getValue[StreamingOperator[IN, OUT]](GEARPUMP_STREAMING_OPERATOR)(context.system).get,
      context, conf
    )
  }

  private val batchSize = conf.getInt(DataSourceConfig.SOURCE_READ_BATCH_SIZE).getOrElse(1000)

  override def onStart(startTime: Instant): Unit = {
    LOG.info(s"opening data source at ${startTime.toEpochMilli}")
    source.open(context, startTime)
    operator.setup()

    self ! Watermark(source.getWatermark)
  }

  override def onNext(m: Message): Unit = {
    0.until(batchSize).foreach { _ =>
      Option(source.read()).foreach(process)
    }

    self ! Watermark(source.getWatermark)
  }

  override def onWatermarkProgress(watermark: Instant): Unit = {
    TaskUtil.trigger(watermark, operator, context)
  }

  override def onStop(): Unit = {
    LOG.info("closing data source...")
    source.close()
    operator.teardown()
  }

  private def process(msg: Message): Unit = {
    operator.flatMap(new TimestampedValue(msg))
      .foreach { tv => context.output(tv.toMessage) }
  }

}
