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

import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import io.gearpump.streaming.dsl.scalaapi.StreamApp
import io.gearpump.streaming.dsl.scalaapi.StreamApp._
import io.gearpump.util.PekkoApp

/** Same WordCount with High level DSL syntax */
object WordCount extends PekkoApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array.empty

  override def main(pekkoConf: Config, args: Array[String]): Unit = {
    val context: ClientContext = ClientContext(pekkoConf)
    val app = StreamApp("dsl", context)
    val data = "This is a good start, bingo!! bingo!!"
    app.source(data.linesIterator.toList, 1, "source").
      // word => (word, count)
      flatMap(line => line.split("[\\s]+")).map((_, 1)).
      // (word, count1), (word, count2) => (word, count1 + count2)
      groupByKey().sum.map(println)

    context.submit(app).waitUntilFinish()
    context.close()
  }
}
