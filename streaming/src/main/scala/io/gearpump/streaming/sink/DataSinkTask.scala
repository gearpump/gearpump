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

package io.gearpump.streaming.sink

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.task.{Task, TaskContext}
import java.time.Instant

object DataSinkTask {
  val DATA_SINK = "data_sink"
}

/**
 * General task that runs any [[DataSink]]
 */
class DataSinkTask private[sink](context: TaskContext, conf: UserConfig, sink: DataSink)
  extends Task(context, conf) {


  def this(context: TaskContext, conf: UserConfig) = {
    this(context, conf, conf.getValue[DataSink](DataSinkTask.DATA_SINK)(context.system).get)
  }

  override def onStart(startTime: Instant): Unit = {
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

  override def onWatermarkProgress(watermark: Instant): Unit = {
    context.updateWatermark(watermark)
  }
}
