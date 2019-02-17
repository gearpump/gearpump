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

package io.gearpump.streaming.examples.wordcount

import akka.actor.ActorSystem
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import io.gearpump.streaming.{Processor, StreamApplication}
import io.gearpump.streaming.partitioner.HashPartitioner
import io.gearpump.streaming.source.DataSourceProcessor
import io.gearpump.util.{AkkaApp, Graph}
import io.gearpump.util.Graph.Node

/** Same WordCount with low level Processor Graph syntax */
object WordCount extends AkkaApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "split" -> CLIOption[Int]("<how many source tasks>", required = false,
      defaultValue = Some(1)),
    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(1))
  )

  def application(config: ParseResult, system: ActorSystem): StreamApplication = {
    implicit val actorSystem = system

    val sumNum = config.getInt("sum")
    val splitNum = config.getInt("split")
    val split = new Split
    val sourceProcessor = DataSourceProcessor(split, splitNum, "Split")
    val sum = Processor[Sum](sumNum)
    val partitioner = new HashPartitioner
    val computation = sourceProcessor ~ partitioner ~> sum
    val app = StreamApplication("wordCount", Graph(computation), UserConfig.empty)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context: ClientContext = ClientContext(akkaConf)
    val app = application(config, context.system)
    context.submit(app)
    context.close()
  }
}

