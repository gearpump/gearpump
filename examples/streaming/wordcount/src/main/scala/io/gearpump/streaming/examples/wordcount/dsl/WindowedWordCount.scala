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
package io.gearpump.streaming.examples.wordcount.dsl

import io.gearpump.Message
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import io.gearpump.streaming.dsl.scalaapi.{LoggerSink, StreamApp}
import io.gearpump.streaming.dsl.window.api.{EventTimeTrigger, FixedWindows}
import io.gearpump.streaming.source.{DataSource, Watermark}
import io.gearpump.streaming.task.TaskContext
import io.gearpump.util.AkkaApp
import java.time.{Duration, Instant}

object WindowedWordCount extends AkkaApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array.empty

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val context = ClientContext(akkaConf)
    val app = StreamApp("dsl", context)
    app.source[String](new TimedDataSource).
      // word => (word, count)
      flatMap(line => line.split("[\\s]+")).map((_, 1)).
      // fix window
      window(FixedWindows.apply(Duration.ofMillis(5L))
        .triggering(EventTimeTrigger)).
      // (word, count1), (word, count2) => (word, count1 + count2)
      groupBy(_._1).
      sum.sink(new LoggerSink)

    context.submit(app).waitUntilFinish()
    context.close()
  }

  private class TimedDataSource extends DataSource {

    private var data = List(
      Message("foo", Instant.ofEpochMilli(1L)),
      Message("bar", Instant.ofEpochMilli(2L)),
      Message("foo", Instant.ofEpochMilli(3L)),
      Message("foo", Instant.ofEpochMilli(5L)),
      Message("bar", Instant.ofEpochMilli(7L)),
      Message("bar", Instant.ofEpochMilli(8L))
    )

    private var watermark: Instant = Instant.ofEpochMilli(0)

    override def read(): Message = {
      if (data.nonEmpty) {
        val msg = data.head
        data = data.tail
        watermark = msg.timestamp
        msg
      } else {
        null
      }
    }

    override def open(context: TaskContext, startTime: Instant): Unit = {}

    override def close(): Unit = {}

    override def getWatermark: Instant = {
      if (data.isEmpty) {
        watermark = Watermark.MAX
      }
      watermark
    }
  }
}
