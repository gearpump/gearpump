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

package io.gearpump.streaming.examples.wordcount

import io.gearpump.cluster.local.LocalCluster
import io.gearpump.streaming.{StreamApplication, Processor}
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import io.gearpump.partitioner.HashPartitioner
import io.gearpump.util.Graph.Node
import io.gearpump.util.{AkkaApp, Graph, LogUtil}
import org.slf4j.Logger

object WordCount extends AkkaApp with ArgumentsParser {
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
    val partitioner = new HashPartitioner

    val app = StreamApplication("wordCount", Graph(split ~ partitioner ~> sum), UserConfig.empty)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)

    val localCluster = if (System.getProperty("DEBUG") != null) {
      val cluster = new LocalCluster(akkaConf: Config)
      cluster.start
      Some(cluster)
    } else {
      None
    }

    val context: ClientContext = localCluster match {
      case Some(local) => local.newClientContext
      case None => ClientContext(akkaConf)
    }

    val appId = context.submit(application(config))
    context.close()
    localCluster.map(_.stop)
  }
}

