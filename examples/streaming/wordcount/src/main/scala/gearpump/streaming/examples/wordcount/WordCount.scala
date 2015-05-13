/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gearpump.streaming.examples.wordcount

import gearpump.cluster.main.ArgumentsParser
import gearpump.streaming.{StreamApplication, Processor}
import gearpump.streaming.task.Task
import gearpump.cluster.UserConfig
import gearpump.cluster.client.ClientContext
import gearpump.cluster.main.{ParseResult, CLIOption, ArgumentsParser}
import gearpump.partitioner.{Partitioner, HashPartitioner}
import gearpump.util.{Graph, LogUtil}
import gearpump.util.Graph.Node
import org.slf4j.Logger

object WordCount extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  val RUN_FOR_EVER = -1

  override val options: Array[(String, CLIOption[Any])] = Array(
    "split" -> CLIOption[Int]("<how many split tasks>", required = false, defaultValue = Some(1)),
    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(1))
  )

  def application(config: ParseResult) : StreamApplication = {
    val splitNum = config.getInt("split")
    val sumNum = config.getInt("sum")
    val split = Processor[Split](splitNum)
    val sum = Processor[Sum](sumNum)

    // We use default HashPartitioner to shuffle data between split and sum
    val app = StreamApplication("wordCount", Graph(split ~> sum), UserConfig.empty)
    app
  }

  val config = parse(args)
  val context = ClientContext()
  val appId = context.submit(application(config))
  context.close()
}

