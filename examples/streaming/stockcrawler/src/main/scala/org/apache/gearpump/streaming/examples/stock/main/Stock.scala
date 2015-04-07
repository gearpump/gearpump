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


package org.apache.gearpump.streaming.examples.stock.main

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.partitioner.{Partitioner, HashPartitioner}
import org.apache.gearpump.streaming.examples.stock.StockMarket.ServiceHour
import org.apache.gearpump.streaming.examples.stock._
import org.apache.gearpump.streaming.task.Task
import org.apache.gearpump.streaming.{Processor, StreamApplication, ProcessorDescription}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Graph.{Path, Node}
import org.apache.gearpump.util.{Graph, LogUtil}
import org.slf4j.Logger

object Stock extends App with ArgumentsParser {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "crawler"-> CLIOption[Int]("<how many fetcher to get data from remote>", required = false, defaultValue = Some(10)),
    "analyzer"-> CLIOption[Int]("<parallism of analyzer>", required = false, defaultValue = Some(1)),
    "proxy" -> CLIOption[String]("proxy setting host:port, for example: 127.0.0.1:8443", required = false, defaultValue = Some("")))

  def crawler(config: ParseResult)(implicit system: ActorSystem) : StreamApplication = {
    val crawler = Processor[Crawler](config.getInt("crawler"))
    val analyzer = Processor[Analyzer](config.getInt("analyzer"))
    val queryServer = Processor[QueryServer](1)
    val partitioner = new HashPartitioner

    val proxySetting = config.getString("proxy")
    val proxy = if (proxySetting.isEmpty) {null } else HostPort(proxySetting)
    val stockMarket = new StockMarket(new ServiceHour(true), proxy)
    val stocks = stockMarket.getStockIdList

    Console.println(s"Successfully fetched stock id for ${stocks.length} stocks")

    val userConfig = UserConfig.empty.withValue("StockId", stocks).withValue[StockMarket](classOf[StockMarket].getName, stockMarket)

    val app = StreamApplication("stock_direct_analyzer",
      Graph(crawler ~ partitioner ~> analyzer, queryServer), userConfig
      )
    app
  }

  val config = parse(args)
  val context = ClientContext()

  implicit val system = context.system

  val app = crawler(config)
  val appId = context.submit(app)
  context.close()
}
