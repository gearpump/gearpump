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

package org.apache.gearpump.streaming.examples.hbase

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.external.hbase.HBaseSink
import org.apache.gearpump.streaming.StreamApplication
import org.apache.gearpump.streaming.partitioner.HashPartitioner
import org.apache.gearpump.streaming.sink.DataSinkProcessor
import org.apache.gearpump.streaming.source.DataSourceProcessor
import org.apache.gearpump.util.Graph.Node
import org.apache.gearpump.util.{AkkaApp, Graph, LogUtil}
import org.slf4j.Logger

object HBaseConn extends AkkaApp with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  val RUN_FOR_EVER = -1

  override val options: Array[(String, CLIOption[Any])] = Array(
    "splitNum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(1)),
    "sinkNum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(1))
  )

  def application(config: ParseResult, system: ActorSystem): StreamApplication = {
    implicit val actorSystem = system

    val splitNum = config.getInt("splitNum")
    val sinkNum = config.getInt("sinkNum")

    val split = new Split
    val sourceProcessor = DataSourceProcessor(split, splitNum, "Split")
    val sink = HBaseSink(UserConfig.empty, "hbase")
    val sinkProcessor = DataSinkProcessor(sink, sinkNum)
    val partitioner = new HashPartitioner
    val computation = sourceProcessor ~ partitioner ~> sinkProcessor
    val application = StreamApplication("HBase", Graph(computation), UserConfig.empty)

    application

  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)
    val appId = context.submit(application(config, context.system))
    context.close()
  }
}
