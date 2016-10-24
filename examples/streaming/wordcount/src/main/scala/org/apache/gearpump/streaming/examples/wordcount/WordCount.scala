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

package org.apache.gearpump.streaming.examples.wordcount

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.embedded.EmbeddedCluster
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.streaming.partitioner.HashPartitioner
import org.apache.gearpump.streaming.source.DataSourceProcessor
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph.Node
import org.apache.gearpump.util.{AkkaApp, Graph, LogUtil}
import org.slf4j.Logger

/** Same WordCount with low level Processor Graph syntax */
object WordCount extends AkkaApp with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  val RUN_FOR_EVER = -1

  override val options: Array[(String, CLIOption[Any])] = Array(
    "split" -> CLIOption[Int]("<how many source tasks>", required = false,
      defaultValue = Some(1)),
    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(1)),
    "debug" -> CLIOption[Boolean]("<true|false>", required = false, defaultValue = Some(false)),
    "sleep" -> CLIOption[Int]("how many seconds to sleep for debug mode", required = false,
      defaultValue = Some(30))
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

    val debugMode = config.getBoolean("debug")
    val sleepSeconds = config.getInt("sleep")

    val localCluster = if (debugMode) {
      val cluster = new EmbeddedCluster(akkaConf: Config)
      cluster.start()
      Some(cluster)
    } else {
      None
    }

    val context: ClientContext = localCluster match {
      case Some(local) => local.newClientContext
      case None => ClientContext(akkaConf)
    }

    val app = application(config, context.system)
    context.submit(app)

    if (debugMode) {
      Thread.sleep(sleepSeconds * 1000) // Sleeps for 30 seconds for debugging.
    }

    context.close()
    localCluster.map(_.stop())
  }
}

